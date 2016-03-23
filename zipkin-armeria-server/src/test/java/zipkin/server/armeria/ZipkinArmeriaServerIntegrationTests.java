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

import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.http.SimpleHttpClient;
import com.linecorp.armeria.client.http.SimpleHttpRequest;
import com.linecorp.armeria.client.http.SimpleHttpRequestBuilder;
import com.linecorp.armeria.client.http.SimpleHttpResponse;
import com.linecorp.armeria.server.Server;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.charset.StandardCharsets;
import javax.inject.Inject;
import org.junit.Test;
import zipkin.Annotation;
import zipkin.Codec;
import zipkin.Endpoint;
import zipkin.Span;
import zipkin.internal.Util;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ZipkinArmeriaServerIntegrationTests {

  @Inject
  Server server;

  SimpleHttpClient client = Clients.newClient("none+http://localhost:9411", SimpleHttpClient.class);

  @Test
  public void writeSpans_noContentTypeIsJson() throws Exception {
    byte[] body = Codec.JSON.writeSpans(asList(newSpan(1L, 1L, "foo", "an", "bar")));
    SimpleHttpRequest request = SimpleHttpRequestBuilder.forPost("/api/v1/spans")
        .content(body)
        .build();
    SimpleHttpResponse response = client.execute(request).get();
    assertThat(response.status()).isEqualTo(HttpResponseStatus.ACCEPTED);
  }

  @Test
  public void writeSpans_malformedJsonIsBadRequest() throws Exception {
    byte[] body = {'h', 'e', 'l', 'l', 'o'};
    SimpleHttpRequest request = SimpleHttpRequestBuilder.forPost("/api/v1/spans")
        .content(body)
        .build();
    SimpleHttpResponse response = client.execute(request).get();
    assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void writeSpans_malformedGzipIsBadRequest() throws Exception {
    byte[] body = {'h', 'e', 'l', 'l', 'o'};
    SimpleHttpRequest request = SimpleHttpRequestBuilder.forPost("/api/v1/spans")
        .content(body)
        .header(HttpHeaderNames.CONTENT_ENCODING, "gzip")
        .build();
    SimpleHttpResponse response = client.execute(request).get();
    assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void writeSpans_contentTypeXThrift() throws Exception {
    byte[] body = Codec.THRIFT.writeSpans(asList(newSpan(1L, 1L, "foo", "an", "bar")));
    SimpleHttpRequest request = SimpleHttpRequestBuilder.forPost("/api/v1/spans")
        .content(body)
        .header(HttpHeaderNames.CONTENT_TYPE, "application/x-thrift")
        .build();
    SimpleHttpResponse response = client.execute(request).get();
    assertThat(response.status()).isEqualTo(HttpResponseStatus.ACCEPTED);
  }

  @Test
  public void writeSpans_malformedThriftIsBadRequest() throws Exception {
    byte[] body = {'h', 'e', 'l', 'l', 'o'};
    SimpleHttpRequest request = SimpleHttpRequestBuilder.forPost("/api/v1/spans")
        .content(body)
        .header(HttpHeaderNames.CONTENT_TYPE, "application/x-thrift")
        .build();
    SimpleHttpResponse response = client.execute(request).get();
    assertThat(response.status()).isEqualTo(HttpResponseStatus.BAD_REQUEST);
  }

  @Test
  public void healthIsOK() throws Exception {
    SimpleHttpRequest request = SimpleHttpRequestBuilder.forGet("/health")
        .build();
    SimpleHttpResponse response = client.execute(request).get();
    assertThat(response.status()).isEqualTo(HttpResponseStatus.OK);
  }

  @Test
  public void writeSpans_gzipEncoded() throws Exception {
    byte[] body = Codec.JSON.writeSpans(asList(newSpan(1L, 2L, "foo", "an", "bar")));
    byte[] gzippedBody = Util.gzip(body);
    SimpleHttpRequest request = SimpleHttpRequestBuilder.forPost("/api/v1/spans")
        .content(gzippedBody)
        .header(HttpHeaderNames.CONTENT_ENCODING, "gzip")
        .build();
    SimpleHttpResponse response = client.execute(request).get();
    assertThat(response.status()).isEqualTo(HttpResponseStatus.ACCEPTED);
  }

  static Span newSpan(long traceId, long id, String spanName, String value, String service) {
    Endpoint endpoint = Endpoint.create(service, 127 << 24 | 1, 80);
    Annotation ann = Annotation.create(System.currentTimeMillis(), value, endpoint);
    return new Span.Builder().id(id).traceId(traceId).name(spanName)
        .timestamp(ann.timestamp).duration(1L).addAnnotation(ann).build();
  }
}
