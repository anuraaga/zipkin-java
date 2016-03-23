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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.linecorp.armeria.common.ServiceInvocationContext;
import com.spotify.futures.FuturesExtra;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.concurrent.Promise;
import java.util.Collections;

final class HandlerUtil {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static ListenableFuture<HttpResponse> jsonResponse(ServiceInvocationContext ctx,
      ListenableFuture<?> resultFuture) {
    return serializedJsonResponse(ctx,
        FuturesExtra.syncTransform(resultFuture, result -> {
          try {
            return OBJECT_MAPPER.writeValueAsBytes(result);
          } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error serializing to json.", e);
          }
        }));
  }

  static ListenableFuture<HttpResponse> serializedJsonResponse(ServiceInvocationContext ctx,
      ListenableFuture<byte[]> resultFuture) {
    return FuturesExtra.syncTransform(resultFuture, result -> {
      ByteBuf content = ctx.alloc().ioBuffer();
      content.writeBytes(result);
      DefaultFullHttpResponse response = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json");
      return response;
    });
  }

  static void setResultFuture(ListenableFuture<HttpResponse> response, Promise<Object> promise) {
    FuturesExtra.addCallback(response, promise::setSuccess, promise::setFailure);
  }

  static String getParam(QueryStringDecoder query, String name) {
    return Iterables.getFirst(
        query.parameters().getOrDefault(name, Collections.emptyList()),
        null);
  }

  private HandlerUtil() {}
}
