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

import com.google.common.util.concurrent.ListenableFuture;
import com.linecorp.armeria.common.ServiceInvocationContext;
import com.linecorp.armeria.server.ServiceInvocationHandler;
import com.spotify.futures.FuturesExtra;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.concurrent.Promise;
import java.util.List;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.inject.Named;
import zipkin.Codec;
import zipkin.Span;
import zipkin.spanstore.guava.GuavaSpanStore;

import static zipkin.internal.Util.lowerHexToUnsignedLong;
import static zipkin.server.armeria.handlers.HandlerUtil.getParam;
import static zipkin.server.armeria.handlers.HandlerUtil.serializedJsonResponse;
import static zipkin.server.armeria.handlers.HandlerUtil.setResultFuture;

@Named
public class TraceHandler implements ServiceInvocationHandler {

  private final GuavaSpanStore spanStore;

  @Inject
  public TraceHandler(GuavaSpanStore spanStore) {
    this.spanStore = spanStore;
  }

  @Override public void invoke(ServiceInvocationContext ctx, Executor executor,
      Promise<Object> promise) throws Exception {
    FullHttpRequest httpRequest = ctx.originalRequest();
    String traceId = ctx.mappedPath().substring(1);
    long id = lowerHexToUnsignedLong(traceId);
    QueryStringDecoder query = new QueryStringDecoder(httpRequest.uri());
    String raw = getParam(query, "raw");
    ListenableFuture<List<Span>> future = raw != null
        ? spanStore.getRawTrace(id) : spanStore.getTrace(id);
    ListenableFuture<byte[]> serialized =
        FuturesExtra.syncTransform(future, Codec.JSON::writeSpans);
    setResultFuture(serializedJsonResponse(ctx, serialized), promise);
  }
}
