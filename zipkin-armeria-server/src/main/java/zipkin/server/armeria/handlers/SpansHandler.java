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

package zipkin.server.armeria.handlers;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.net.MediaType;
import com.linecorp.armeria.common.ServiceInvocationContext;
import com.linecorp.armeria.server.ServiceInvocationHandler;
import io.netty.buffer.ByteBufUtil;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.concurrent.Promise;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.inject.Named;
import zipkin.Codec;
import zipkin.Sampler;
import zipkin.Span;
import zipkin.internal.Util;
import zipkin.spanstore.guava.GuavaSpanStore;

import static java.util.Objects.requireNonNull;
import static zipkin.server.armeria.handlers.HandlerUtil.jsonResponse;
import static zipkin.server.armeria.handlers.HandlerUtil.setResultFuture;

@Named
public class SpansHandler implements ServiceInvocationHandler {

  private static final MediaType APPLICATION_THRIFT = MediaType.create("application", "x-thrift");

  private final GuavaSpanStore spanStore;
  private final Sampler sampler;
  private final Codec jsonCodec;
  private final Codec thriftCodec;

  @Inject
  public SpansHandler(GuavaSpanStore spanStore, Sampler sampler, Codec.Factory codecFactory) {
    this.spanStore = spanStore;
    this.sampler = sampler;
    this.jsonCodec = requireNonNull(codecFactory.get("application/json"), "application/json");
    this.thriftCodec = requireNonNull(codecFactory.get("application/x-thrift"),
        "application/x-thrift");
  }

  @Override public void invoke(ServiceInvocationContext ctx, Executor executor,
      Promise<Object> promise) throws Exception {
    FullHttpRequest httpRequest = ctx.originalRequest();
    if (httpRequest.method() == HttpMethod.GET) {
      getSpanNames(ctx, httpRequest, promise);
    } else if (httpRequest.method() == HttpMethod.POST) {
      uploadSpans(httpRequest, promise);
    } else {
      promise.setSuccess(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.METHOD_NOT_ALLOWED));
    }
  }

  private void getSpanNames(ServiceInvocationContext ctx, FullHttpRequest request,
      Promise<Object> promise) {
    QueryStringDecoder decoder = new QueryStringDecoder(request.uri());
    String serviceName =
        Iterables.getFirst(
            decoder.parameters().getOrDefault("serviceName", Collections.emptyList()),
            null);
    if (Strings.isNullOrEmpty(serviceName)) {
      promise.setSuccess(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.BAD_REQUEST));
      return;
    }
    setResultFuture(jsonResponse(ctx, spanStore.getSpanNames(serviceName)), promise);
  }

  private void uploadSpans(FullHttpRequest request, Promise<Object> promise) {
    String contentType = request.headers().get(HttpHeaderNames.CONTENT_TYPE);
    MediaType mediaType = contentType != null ? MediaType.parse(contentType) : MediaType.JSON_UTF_8;
    Codec codec = mediaType.is(APPLICATION_THRIFT) ? thriftCodec : jsonCodec;
    byte[] content = ByteBufUtil.getBytes(request.content());
    List<Span> spans;
    try {
      if ("gzip".equalsIgnoreCase(request.headers().get(HttpHeaderNames.CONTENT_ENCODING))) {
        content = Util.gunzip(content);
      }
      spans = codec.readSpans(content);
    }  catch (IOException|IllegalArgumentException e) {
      promise.setSuccess(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.BAD_REQUEST));
      return;
    }

    List<Span> sampled = new ArrayList<>(spans.size());
    for (Span s : spans) {
      if ((s.debug != null && s.debug) || sampler.isSampled(s.traceId)) {
        sampled.add(s);
      }
    }
    spanStore.accept(sampled);
    promise.setSuccess(
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.ACCEPTED));
  }
}
