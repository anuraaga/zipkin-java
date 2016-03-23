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

import com.linecorp.armeria.common.ServiceInvocationContext;
import com.linecorp.armeria.server.ServiceInvocationHandler;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.Executor;
import javax.inject.Inject;
import javax.inject.Named;
import zipkin.spanstore.guava.GuavaSpanStore;

import static zipkin.server.armeria.handlers.HandlerUtil.jsonResponse;
import static zipkin.server.armeria.handlers.HandlerUtil.setResultFuture;

@Named
public class ServiceHandler implements ServiceInvocationHandler {

  private final GuavaSpanStore spanStore;

  @Inject
  public ServiceHandler(GuavaSpanStore spanStore) {
    this.spanStore = spanStore;
  }

  @Override public void invoke(ServiceInvocationContext ctx, Executor executor,
      Promise<Object> promise) throws Exception {
    setResultFuture(jsonResponse(ctx, spanStore.getServiceNames()), promise);
  }
}
