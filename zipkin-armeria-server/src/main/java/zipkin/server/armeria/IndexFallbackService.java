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
import com.linecorp.armeria.server.DecoratingServiceCodec;
import com.linecorp.armeria.server.ServiceCodec;
import com.linecorp.armeria.server.ServiceConfig;
import com.linecorp.armeria.server.http.HttpService;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.util.concurrent.Promise;

class IndexFallbackService extends HttpService {

  public IndexFallbackService(HttpService delegate) {
    super(delegate.handler());
  }

  @Override
  public ServiceCodec codec() {
    return new AlwaysIndexServiceCodec(super.codec());
  }

  private static class AlwaysIndexServiceCodec extends DecoratingServiceCodec {
    protected AlwaysIndexServiceCodec(ServiceCodec delegate) {
      super(delegate);
    }

    @Override public DecodeResult decodeRequest(ServiceConfig cfg, Channel ch,
        SessionProtocol sessionProtocol, String hostname, String path, String mappedPath,
        ByteBuf in,
        Object originalRequest, Promise<Object> promise) throws Exception {
      return delegate().decodeRequest(cfg, ch, sessionProtocol, hostname, path, "/index.html", in,
          originalRequest, promise);
    }
  }
}
