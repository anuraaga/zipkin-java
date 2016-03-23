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
package zipkin.server.armeria;

import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.http.HttpService;
import com.linecorp.armeria.server.http.file.HttpFileService;
import com.linecorp.armeria.server.http.healthcheck.HttpHealthCheckService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import zipkin.Codec;
import zipkin.Sampler;
import zipkin.elasticsearch.ElasticsearchConfig;
import zipkin.elasticsearch.ElasticsearchSpanStore;
import zipkin.server.armeria.handlers.ServiceHandler;
import zipkin.server.armeria.handlers.SpansHandler;
import zipkin.server.armeria.handlers.TraceHandler;
import zipkin.server.armeria.handlers.TracesHandler;
import zipkin.server.armeria.handlers.UiConfigHandler;
import zipkin.spanstore.guava.GuavaSpanStore;

@Configuration
@ComponentScan
public class ZipkinArmeriaServerConfiguration {

  @Bean
  @ConditionalOnMissingBean(Codec.Factory.class)
  Codec.Factory codecFactory() {
    return Codec.FACTORY;
  }

  @Bean
  @ConditionalOnMissingBean(Sampler.class)
  Sampler traceIdSampler(@Value("${zipkin.collector.sample-rate:1.0}") float rate) {
    return Sampler.create(rate);
  }

  @Bean GuavaSpanStore elasticsearchSpanStore(ZipkinElasticsearchProperties elasticsearch) {
    ElasticsearchConfig config = new ElasticsearchConfig.Builder()
        .cluster(elasticsearch.getCluster())
        .hosts(elasticsearch.getHosts())
        .index(elasticsearch.getIndex())
        .build();
    return new ElasticsearchSpanStore(config);
  }

  @Bean Server armeriaServer(
      ServiceHandler serviceHandler,
      SpansHandler spansHandler,
      TraceHandler traceHandler,
      TracesHandler tracesHandler,
      UiConfigHandler uiConfigHandler) {
    HttpFileService uiService = HttpFileService.forClassPath("/zipkin-ui/");
    IndexFallbackService indexService = new IndexFallbackService(uiService);
    ServerBuilder sb = new ServerBuilder()
        .port(9411, SessionProtocol.HTTP)
        .serviceAt("/api/v1/services", new HttpService(serviceHandler))
        .serviceAt("/api/v1/spans", new HttpService(spansHandler))
        .serviceAt("/api/v1/traces", new HttpService(tracesHandler))
        .serviceUnder("/api/v1/trace/", new HttpService(traceHandler))
        .serviceAt("/health", new HttpHealthCheckService())
        .serviceAt("/config.json", new HttpService(uiConfigHandler))
        // TODO This approach requires maintenance when new UI routes are added. Change to the following:
        // If the path is a a file w/an extension, treat normally.
        // Otherwise instead of returning 404, forward to the index.
        // See https://github.com/twitter/finatra/blob/458c6b639c3afb4e29873d123125eeeb2b02e2cd/http/src/main/scala/com/twitter/finatra/http/response/ResponseBuilder.scala#L321
        .serviceUnder("/traces/", indexService)
        .serviceAt("/dependency", indexService)
        .serviceAt("/", indexService)
        .serviceUnder("/", uiService.orElse(new IndexFallbackService(uiService)));
    Server server = sb.build();
    server.start();
    return server;
  }
}
