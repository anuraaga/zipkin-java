/*
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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import org.junit.AssumptionViolatedException;

enum ElasticsearchTestGraph {
  INSTANCE;

  static final ElasticsearchProperties CONFIG = new ElasticsearchProperties();

  static {
    // Avoid race-conditions in travis by forcing read-your-writes consistency.
    ElasticsearchSpanConsumer.BLOCK_ON_FUTURES = true;
  }

  private AssumptionViolatedException ex;
  private ElasticsearchSpanStore spanStore;

  /** A lot of tech debt here because the repository constructor performs I/O. */
  synchronized ElasticsearchSpanStore spanStore() {
    if (ex != null) throw ex;
    if (this.spanStore == null) {
      try {
        this.spanStore = new ElasticsearchSpanStore(CONFIG);
      } catch (NoHostAvailableException e) {
        throw ex = new AssumptionViolatedException(e.getMessage());
      }
    }
    return spanStore;
  }
}
