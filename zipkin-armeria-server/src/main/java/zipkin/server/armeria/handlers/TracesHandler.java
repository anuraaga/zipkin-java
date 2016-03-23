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

import com.google.common.base.MoreObjects;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.linecorp.armeria.common.ServiceInvocationContext;
import com.linecorp.armeria.server.ServiceInvocationHandler;
import com.spotify.futures.FuturesExtra;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.inject.Named;
import org.springframework.beans.factory.annotation.Value;
import zipkin.Codec;
import zipkin.QueryRequest;
import zipkin.spanstore.guava.GuavaSpanStore;

import static com.google.common.base.Strings.nullToEmpty;
import static zipkin.server.armeria.handlers.HandlerUtil.getParam;
import static zipkin.server.armeria.handlers.HandlerUtil.serializedJsonResponse;
import static zipkin.server.armeria.handlers.HandlerUtil.setResultFuture;

@Named
public class TracesHandler implements ServiceInvocationHandler {

  private final GuavaSpanStore spanStore;
  private final int defaultLookback;

  @Inject
  public TracesHandler(
      GuavaSpanStore spanStore,
      @Value("${zipkin.query.lookback:86400000}") int defaultLookback) {
    this.spanStore = spanStore;
    this.defaultLookback = defaultLookback;
  }

  @Override public void invoke(ServiceInvocationContext ctx, Executor executor,
      Promise<Object> promise) throws Exception {
    FullHttpRequest request = ctx.originalRequest();
    QueryStringDecoder query = new QueryStringDecoder(request.uri());
    String serviceName = getParam(query, "serviceName");
    if (serviceName == null) {
      promise.setSuccess(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
          HttpResponseStatus.BAD_REQUEST));
      return;
    }

    String spanName = MoreObjects.firstNonNull(getParam(query, "spanName"), "all");
    String annotationQuery = getParam(query, "annotationQuery");
    Long minDuration = Longs.tryParse(nullToEmpty(getParam(query, "minDuration")));
    Long maxDuration = Longs.tryParse(nullToEmpty(getParam(query, "maxDuration")));
    Long endTs = Longs.tryParse(nullToEmpty(getParam(query, "endTs")));
    Long lookback = Longs.tryParse(nullToEmpty(getParam(query, "lookback")));
    Integer limit = Ints.tryParse(nullToEmpty(getParam(query, "limit")));

    QueryRequest queryRequest = new QueryRequest.Builder(serviceName)
        .spanName(spanName)
        .parseAnnotationQuery(annotationQuery)
        .minDuration(minDuration)
        .maxDuration(maxDuration)
        .endTs(endTs)
        .lookback(lookback != null ? lookback : defaultLookback)
        .limit(limit).build();
    ListenableFuture<byte[]> responseFuture =
        FuturesExtra.syncTransform(spanStore.getTraces(queryRequest), Codec.JSON::writeTraces);
    setResultFuture(serializedJsonResponse(ctx, responseFuture), promise);
  }
}
