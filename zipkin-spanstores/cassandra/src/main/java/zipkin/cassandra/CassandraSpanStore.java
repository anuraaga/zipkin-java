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
package zipkin.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.twitter.zipkin.storage.cassandra.Repository;
import zipkin.Codec;
import zipkin.DependencyLink;
import zipkin.QueryRequest;
import zipkin.Span;
import zipkin.internal.CorrectForClockSkew;
import zipkin.internal.Dependencies;
import zipkin.internal.MergeById;
import zipkin.internal.Nullable;
import zipkin.internal.Pair;
import zipkin.spanstore.guava.GuavaSpanStore;
import zipkin.spanstore.guava.GuavaToAsyncSpanStoreAdapter;

import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.lang.String.format;
import static zipkin.cassandra.CassandraUtil.annotationKeys;
import static zipkin.cassandra.CassandraUtil.intersectKeySets;
import static zipkin.cassandra.CassandraUtil.keyset;
import static zipkin.cassandra.CassandraUtil.toSortedList;
import static zipkin.internal.Util.midnightUTC;

/**
 * CQL3 implementation of a span store.
 *
 * <p>This uses zipkin-cassandra-core which packages "/cassandra-schema-cql3.txt"
 */
public final class CassandraSpanStore extends GuavaToAsyncSpanStoreAdapter
    implements GuavaSpanStore, AutoCloseable {

  private final String keyspace;
  private final int indexTtl;
  private final int maxTraceCols;
  private final Cluster cluster;
  private final CassandraSpanConsumer spanConsumer;
  @VisibleForTesting final Repository repository;

  public CassandraSpanStore(CassandraConfig config) {
    this.keyspace = config.keyspace;
    this.indexTtl = config.indexTtl;
    this.maxTraceCols = config.maxTraceCols;
    this.cluster = config.toCluster();
    this.repository = new Repository(config.keyspace, cluster, config.ensureSchema);
    this.spanConsumer = new CassandraSpanConsumer(repository, config.spanTtl, config.indexTtl);
  }

  @Override protected GuavaSpanStore delegate() {
    return this;
  }

  @Override
  public ListenableFuture<Void> accept(List<Span> spans) {
    return spanConsumer.accept(spans);
  }

  @Override
  public ListenableFuture<List<List<Span>>> getTraces(QueryRequest request) {
    String spanName = request.spanName != null ? request.spanName : "";
    final ListenableFuture<Map<Long, Long>> traceIdToTimestamp;
    if (request.minDuration != null || request.maxDuration != null) {
      traceIdToTimestamp = repository.getTraceIdsByDuration(request.serviceName, spanName,
          request.minDuration, request.maxDuration != null ? request.maxDuration : Long.MAX_VALUE,
          request.endTs * 1000, (request.endTs - request.lookback) * 1000, request.limit, indexTtl);
    } else if (!spanName.isEmpty()) {
      traceIdToTimestamp = repository.getTraceIdsBySpanName(request.serviceName, spanName,
          request.endTs * 1000, request.lookback * 1000, request.limit);
    } else {
      traceIdToTimestamp = repository.getTraceIdsByServiceName(request.serviceName,
          request.endTs * 1000, request.lookback * 1000, request.limit);
    }

    List<ByteBuffer> annotationKeys = annotationKeys(request);

    ListenableFuture<Set<Long>> traceIds;
    if (annotationKeys.isEmpty()) {
      // Simplest case is when there is no annotation query. Limit is valid since there's no AND
      // query that could reduce the results returned to less than the limit.
      traceIds = transform(traceIdToTimestamp, keyset());
    } else {
      // While a valid port of the scala cassandra span store (from zipkin 1.35), there is a fault.
      // each annotation key is an intersection, which means we are likely to return < limit.
      List<ListenableFuture<Map<Long, Long>>> futureKeySetsToIntersect = new ArrayList<>();
      futureKeySetsToIntersect.add(traceIdToTimestamp);
      for (ByteBuffer annotationKey : annotationKeys) {
        futureKeySetsToIntersect.add(repository.getTraceIdsByAnnotation(annotationKey,
            request.endTs * 1000, request.lookback * 1000, request.limit));
      }
      // We achieve the AND goal, by intersecting each of the key sets.
      traceIds = transform(allAsList(futureKeySetsToIntersect), intersectKeySets());
    }
    return transformAsync(traceIds, new AsyncFunction<Set<Long>, List<List<Span>>>() {
      @Override
      public ListenableFuture<List<List<Span>>> apply(Set<Long> traceIds) {
        return transform(repository.getSpansByTraceIds(traceIds.toArray(new Long[traceIds.size()]),
            maxTraceCols), ConvertTracesResponse.INSTANCE);
      }

      @Override public String toString() {
        return "getSpansByTraceIds";
      }
    });
  }

  enum ConvertTracesResponse implements Function<Map<Long, List<ByteBuffer>>, List<List<Span>>> {
    INSTANCE;

    @Override public List<List<Span>> apply(Map<Long, List<ByteBuffer>> input) {
      Collection<List<ByteBuffer>> encodedTraces = input.values();

      List<List<Span>> result = new ArrayList<>(encodedTraces.size());
      for (List<ByteBuffer> encodedTrace : encodedTraces) {
        List<Span> spans = new ArrayList<>(encodedTrace.size());
        for (ByteBuffer encodedSpan : encodedTrace) {
          spans.add(Codec.THRIFT.readSpan(encodedSpan));
        }
        result.add(CorrectForClockSkew.apply(MergeById.apply(spans)));
      }
      return TRACE_DESCENDING.immutableSortedCopy(result);
    }
  }

  @Override
  public ListenableFuture<List<Span>> getRawTrace(long traceId) {
    return transform(repository.getSpansByTraceIds(new Long[] {traceId}, maxTraceCols),
        new Function<Map<Long, List<ByteBuffer>>, List<Span>>() {
          @Override public List<Span> apply(Map<Long, List<ByteBuffer>> encodedTraces) {
            if (encodedTraces.isEmpty()) return null;
            List<ByteBuffer> encodedTrace = encodedTraces.values().iterator().next();
            ImmutableList.Builder<Span> result = ImmutableList.builder();
            for (ByteBuffer encodedSpan : encodedTrace) {
              result.add(Codec.THRIFT.readSpan(encodedSpan));
            }
            return result.build();
          }
        });
  }

  @Override
  public ListenableFuture<List<Span>> getTrace(long traceId) {
    return transform(getRawTrace(traceId), new Function<List<Span>, List<Span>>() {
      @Override public List<Span> apply(List<Span> input) {
        if (input == null || input.isEmpty()) return null;
        return ImmutableList.copyOf(CorrectForClockSkew.apply(MergeById.apply(input)));
      }
    });
  }

  @Override
  public ListenableFuture<List<String>> getServiceNames() {
    return transform(repository.getServiceNames(), toSortedList());
  }

  @Override
  public ListenableFuture<List<String>> getSpanNames(String service) {
    if (service == null) return EMPTY_LIST;
    // service names are always lowercase!
    return transform(repository.getSpanNames(service.toLowerCase()), toSortedList());
  }

  @Override
  public ListenableFuture<List<DependencyLink>> getDependencies(long endTs,
      @Nullable Long lookback) {
    long endEpochDayMillis = midnightUTC(endTs);
    long startEpochDayMillis = midnightUTC(endTs - (lookback != null ? lookback : endTs));

    return transform(repository.getDependencies(startEpochDayMillis, endEpochDayMillis),
        ConvertDependenciesResponse.INSTANCE);
  }

  enum ConvertDependenciesResponse implements Function<List<ByteBuffer>, List<DependencyLink>> {
    INSTANCE;

    @Override public List<DependencyLink> apply(List<ByteBuffer> encodedDailyDependencies) {

      // Combine the dependency links from startEpochDayMillis until endEpochDayMillis
      Map<Pair<String>, Long> links = new LinkedHashMap<>(encodedDailyDependencies.size());

      for (ByteBuffer encodedDayOfDependencies : encodedDailyDependencies) {
        for (DependencyLink link : Dependencies.fromThrift(encodedDayOfDependencies).links) {
          Pair<String> parentChild = Pair.create(link.parent, link.child);
          long callCount = links.containsKey(parentChild) ? links.get(parentChild) : 0L;
          callCount += link.callCount;
          links.put(parentChild, callCount);
        }
      }

      List<DependencyLink> result = new ArrayList<>(links.size());
      for (Map.Entry<Pair<String>, Long> link : links.entrySet()) {
        result.add(DependencyLink.create(link.getKey()._1, link.getKey()._2, link.getValue()));
      }
      return result;
    }
  }

  /** Used for testing */
  void clear() {
    try (Session session = cluster.connect()) {
      List<ListenableFuture<?>> futures = new LinkedList<>();
      for (String cf : ImmutableList.of(
          "traces",
          "dependencies",
          "service_names",
          "span_names",
          "service_name_index",
          "service_span_name_index",
          "annotations_index",
          "span_duration_index"
      )) {
        futures.add(session.executeAsync(format("TRUNCATE %s.%s", keyspace, cf)));
      }
      Futures.getUnchecked(Futures.allAsList(futures));
    }
  }

  @Override
  public void close() {
    repository.close();
    cluster.close();
  }
}
