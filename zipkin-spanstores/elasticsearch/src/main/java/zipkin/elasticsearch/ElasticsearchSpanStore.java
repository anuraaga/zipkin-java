/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.elasticsearch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.google.common.primitives.UnsignedLongs;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequest;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import zipkin.Codec;
import zipkin.DependencyLink;
import zipkin.QueryRequest;
import zipkin.Span;
import zipkin.SpanStore;
import zipkin.internal.CorrectForClockSkew;
import zipkin.internal.JsonCodec;
import zipkin.internal.MergeById;
import zipkin.internal.Nullable;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class ElasticsearchSpanStore implements SpanStore {

  private final Client client;
  private final IndexNameFormatter indexNameFormatter;
  private final ElasticsearchSpanConsumer spanConsumer;
  private final String index;

  public ElasticsearchSpanStore(ElasticsearchProperties config) {
    this.client = createClient(config.getHosts(), config.getClusterName());
    this.indexNameFormatter = new IndexNameFormatter(config.getDailyIndexFormat());
    this.spanConsumer = new ElasticsearchSpanConsumer(
        client, config.getIndex(), indexNameFormatter, Clock.systemUTC());
    this.index = config.getIndex();

    checkForIndexTemplate();
  }

  @Override public void accept(List<Span> spans) {
    spanConsumer.accept(spans);
  }

  @Override public List<List<Span>> getTraces(QueryRequest request) {
    Instant end = Instant.ofEpochMilli(request.endTs);
    Instant begin = end.minusMillis(request.lookback);

    String serviceName = request.serviceName.toLowerCase();

    BoolQueryBuilder filter = boolQuery()
        .must(boolQuery()
            .should(termQuery("annotations.endpoint.serviceName", serviceName))
            .should(nestedQuery(
                "binaryAnnotations",
                termQuery("binaryAnnotations.endpoint.serviceName", serviceName))))
        .must(rangeQuery("timestamp")
            .gte(begin.toEpochMilli() * 1000)
            .lte(end.toEpochMilli() * 1000));
    if (request.spanName != null) {
      filter.must(termQuery("name", request.spanName));
    }
    for (String annotation : request.annotations) {
      filter.must(termQuery("annotations.value", annotation));
    }
    for (Map.Entry<String, String> annotation : request.binaryAnnotations.entrySet()) {
      filter.must(nestedQuery("binaryAnnotations",
          boolQuery()
              .must(termQuery("binaryAnnotations.key", annotation.getKey()))
              .must(termQuery("binaryAnnotations.value",
                  annotation.getValue()))));
    }

    if (request.minDuration != null) {
      RangeQueryBuilder durationQuery = rangeQuery("duration").gte(request.minDuration);
      if (request.maxDuration != null) {
        durationQuery.lte(request.maxDuration);
      }
      filter.must(durationQuery);
    }

    String[] indices = computeIndices(begin, end).stream().toArray(String[]::new);
    SearchRequestBuilder elasticRequest =
        client.prepareSearch(indices)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setTypes(ElasticsearchConstants.SPAN)
            .setQuery(boolQuery().must(matchAllQuery()).filter(filter))
            .setSize(0)
            .addAggregation(AggregationBuilders.terms("traceId_agg")
                .field("traceId")
                .subAggregation(AggregationBuilders.max("timestamps_Agg")
                    .field("timestamp"))
                .order(Order.aggregation("timestamps_Agg", false))
                .size(request.limit));
    SearchResponse response = elasticRequest.execute().actionGet();

    if (response.getAggregations() == null) {
      return Collections.emptyList();
    }
    Terms traceIdsAgg = response.getAggregations().get("traceId_agg");
    if (traceIdsAgg == null) {
      return Collections.emptyList();
    }
    List<Long> traceIds = traceIdsAgg.getBuckets().stream()
        .map(Terms.Bucket::getKeyAsString)
        .map(id -> UnsignedLongs.parseUnsignedLong(id, 16))
        .collect(Collectors.toList());
    return getTracesByIds(traceIds, indices);
  }

  @Override public List<Span> getTrace(long id) {
    return Iterables.getFirst(getTracesByIds(ImmutableList.of(id)), null);
  }

  @Override public List<Span> getRawTrace(long traceId) {
    SearchRequestBuilder elasticRequest = client.prepareSearch(index + "-*")
        .setTypes(ElasticsearchConstants.SPAN)
      .setQuery(termQuery("traceId",String.format("%016x", traceId)));
    SearchResponse response = elasticRequest.execute().actionGet();
    return ImmutableList.copyOf(
        Stream.of(response.getHits().getHits())
        .map(hit -> Codec.JSON.readSpan(hit.getSourceAsString().getBytes(StandardCharsets.UTF_8)))
        .iterator());
  }

  private List<List<Span>> getTracesByIds(Collection<Long> traceIds, String... indices) {
    SearchRequestBuilder elasticRequest = client.prepareSearch(indices)
        .setIndicesOptions(IndicesOptions.lenientExpandOpen())
        .setTypes(ElasticsearchConstants.SPAN)
        .setQuery(termsQuery("traceId", traceIds
            .stream()
            .map(id -> String.format("%016x", id))
            .collect(Collectors.toList())))
        // We sort descending here so traces are sorted in order of descending timestamp while we
        // group. Later we reverse the spans within a trace.
        .addSort(SortBuilders.fieldSort("timestamp")
            .order(SortOrder.DESC)
            .unmappedType("long"));
    SearchResponse response = elasticRequest.execute().actionGet();
    Map<Long, List<Span>> groupedSpans = Stream.of(response.getHits().getHits())
        .map(hit -> Codec.JSON.readSpan(hit.getSourceAsString().getBytes(StandardCharsets.UTF_8)))
        .collect(Collectors.groupingBy(s -> s.traceId, LinkedHashMap::new, Collectors.toList()));
    return ImmutableList.copyOf(groupedSpans.values().stream()
        .map(Lists::reverse)
        .map(MergeById::apply)
        .map(CorrectForClockSkew::apply)
        .iterator());
  }

  @Override
  public List<String> getServiceNames() {
    SearchRequestBuilder elasticRequest =
        client.prepareSearch(index + "-*")
            .setTypes(ElasticsearchConstants.SPAN)
            .setQuery(matchAllQuery())
            .setSize(0)
            .addAggregation(AggregationBuilders.terms("annotationServiceName_agg")
                .field("annotations.endpoint.serviceName"))
            .addAggregation(AggregationBuilders.nested("binaryAnnotations_agg")
                .path("binaryAnnotations")
                .subAggregation(AggregationBuilders.terms("binaryAnnotationsServiceName_agg")
                    .field("binaryAnnotations.endpoint.serviceName")));
    SearchResponse response = elasticRequest.execute().actionGet();
    if (response.getAggregations() == null) {
      return Collections.emptyList();
    }
    SortedSet<String> serviceNames = new TreeSet<>();
    Terms annotationServiceNamesAgg = response.getAggregations().get("annotationServiceName_agg");
    if (annotationServiceNamesAgg != null) {
      serviceNames.addAll(annotationServiceNamesAgg.getBuckets()
          .stream()
          .map(Bucket::getKeyAsString)
          .filter(s -> !s.isEmpty())
          .collect(Collectors.toList()));
    }
    Nested binaryAnnotationsAgg = response.getAggregations().get("binaryAnnotations_agg");
    if (binaryAnnotationsAgg != null && binaryAnnotationsAgg.getAggregations() != null) {
      Terms binaryAnnotationServiceNamesAgg = binaryAnnotationsAgg.getAggregations()
          .get("binaryAnnotationsServiceName_agg");
      if (binaryAnnotationServiceNamesAgg != null) {
        serviceNames.addAll(binaryAnnotationServiceNamesAgg.getBuckets()
            .stream()
            .map(Bucket::getKeyAsString)
            .filter(s -> !s.isEmpty())
            .collect(Collectors.toList()));
      }
    }
    return ImmutableList.copyOf(serviceNames);
  }

  @Override
  public List<String> getSpanNames(String serviceName) {
    if (Strings.isNullOrEmpty(serviceName)) {
      return Collections.emptyList();
    }
    serviceName = serviceName.toLowerCase();
    QueryBuilder filter = boolQuery()
        .should(termQuery("annotations.endpoint.serviceName", serviceName))
        .should(termQuery("binaryAnnotations.endpoint.serviceName", serviceName));
    SearchRequestBuilder elasticRequest = client.prepareSearch(index + "-*")
        .setTypes(ElasticsearchConstants.SPAN)
        .setQuery(boolQuery().must(matchAllQuery()).filter(filter))
        .setSize(0)
        .addAggregation(AggregationBuilders.terms("name_agg")
            .order(Order.term(true))
            .field("name"));
    SearchResponse response = elasticRequest.execute().actionGet();
    Terms namesAgg = response.getAggregations().get("name_agg");
    if (namesAgg == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(
        namesAgg.getBuckets().stream()
            .map(Bucket::getKeyAsString)
            .collect(Collectors.toList()));
  }

  @Override
  public List<DependencyLink> getDependencies(long endTs, @Nullable Long lookback) {
    Instant end = Instant.ofEpochMilli(endTs);
    Instant begin = lookback != null ? end.minusMillis(lookback) : end;
    // We just return all dependencies in the days that fall within endTs and lookback as
    // dependency links themselves don't have timestamps.
    SearchRequestBuilder elasticRequest = client.prepareSearch(
        computeIndices(begin, end).stream().toArray(String[]::new))
        .setTypes("dependencylink")
        .setQuery(matchAllQuery());
    SearchResponse response = elasticRequest.execute().actionGet();
    return ImmutableList.copyOf(
        Stream.of(response.getHits().getHits())
            .map(hit -> {
              try {
                return JsonCodec.DEPENDENCY_LINK_ADAPTER.fromJson(hit.getSourceAsString());
              } catch (IOException e) {
                throw JsonCodec.exceptionReading(
                    JsonCodec.DEPENDENCY_LINK_ADAPTER.toString(),
                    hit.getSourceAsString().getBytes(StandardCharsets.UTF_8),
                    e);
              }
            })
            .iterator());
  }

  @VisibleForTesting void clear() {
    client.admin().indices().delete(new DeleteIndexRequest(index + "-*")).actionGet();
    client.admin().indices().flush(new FlushRequest()).actionGet();
  }

  @VisibleForTesting void writeDependencyLinks(List<DependencyLink> links, Instant timestamp) {
    timestamp = timestamp.truncatedTo(ChronoUnit.DAYS);
    BulkRequestBuilder request = client.prepareBulk();
    for (DependencyLink link : links) {
      request.add(client.prepareIndex(
          indexNameFormatter.formatIndexName(index, timestamp),
          ElasticsearchConstants.DEPENDENCY_LINK)
          .setSource(JsonCodec.DEPENDENCY_LINK_ADAPTER.toJson(link)));
    }
    request.execute().actionGet();
    client.admin().indices().flush(new FlushRequest()).actionGet();
  }

  private List<String> computeIndices(Instant begin, Instant end) {
    Instant startIndex = begin.truncatedTo(ChronoUnit.DAYS);
    Instant endIndex = end.truncatedTo(ChronoUnit.DAYS);

    List<String> indices = new ArrayList<>();
    for (Instant current = startIndex; !current.isAfter(endIndex);
        current = current.plus(1, ChronoUnit.DAYS)) {
      indices.add(indexNameFormatter.formatIndexName(index, current));
    }
    return indices;
  }

  private void checkForIndexTemplate() {
    GetIndexTemplatesResponse existingTemplates =
        client.admin().indices().getTemplates(new GetIndexTemplatesRequest("zipkin_template"))
            .actionGet();
    if (!existingTemplates.getIndexTemplates().isEmpty()) {
      return;
    }
    String template;
    try {
      template =
          Resources.toString(Resources.getResource("zipkin/elasticsearch/zipkin_template.json"),
              StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("Error reading jar resource, shouldn't happen.", e);
    }
    template = template.replace("${__INDEX__}", index);
    client.admin().indices().putTemplate(
        new PutIndexTemplateRequest("zipkin_template").source(template))
        .actionGet();
  }

  private static Client createClient(String hosts, String clusterName) {
    Settings settings = Settings.builder()
        .put("cluster.name", clusterName)
        .put("discovery.zen.ping.multicast.enabled", "false")
        .put("discovery.zen.ping.unicast.hosts", hosts)
        .put("network.host", "_non_loopback_")
        .put("transport.tcp.port", 0)
        .put("http.enabled", false)
        // Workaround elasticsearch 2.0 requiring home, even for client-only nodes.
        // There isn't a real reason for a client-only node to require an elasticsearch home
        // and it just complicates deployment.
        .put("path.home", "/tmp/elasticsearch")
        .build();

    // HACK: Force classloading of LicensePlugin so elasticsearch recognizes it.
    // This is enough for this client to connect to a license-plugin host it seems.
    // We need this since we don't use a proper elasticsearch home and prefer to be
    // able to deploy with this single jar.
    try {
      Class.forName(LicensePlugin.class.getName());
    } catch (ClassNotFoundException ignored) {
      throw new IllegalStateException("Can't happen");
    }

    return nodeBuilder().settings(settings).client(true).node().client();
  }
}
