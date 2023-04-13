/*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ms.silverking.cloud.dht.trace;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracerFactory {
  private static final Logger log = LoggerFactory.getLogger(TracerFactory.class);

  private static Tracer singletonTracerInstance = null;

  public static void setTracer(Tracer instance) {
    Preconditions.checkNotNull(instance);
    singletonTracerInstance = instance;
  }

  // This method will be called at launch time
  public static void ensureTracerInitialized() {
    if (singletonTracerInstance == null) {
      log.info("Tracer hasn't been set in TracerFactory. Initialised tracer to SkNoopTracer");
      singletonTracerInstance = new SkNoopTracer();
    }
  }

  public static void setAliasMap(IPAliasMap aliasMap) {
    singletonTracerInstance.setAliasMap(aliasMap);
  }

  public static Tracer getTracer() {
    return singletonTracerInstance;
  }

  public static boolean isInitialized() {
    return singletonTracerInstance != null;
  }

  public static void clear() {
    singletonTracerInstance = null;
  }
}