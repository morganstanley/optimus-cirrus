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
package com.ms.silverking.thread.lwt;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides: a) The capability to create LWTPools. b) Default work pools that are used by BaseWorker
 * if no other pool is provided.
 */
public class LWTPoolProvider {
  public static LWTPool defaultNonConcurrentWorkPool;
  public static LWTPool defaultConcurrentWorkPool;

  public static final double defaultMaxThreadFactor = 20.0;

  private static final Object synch = new Object();

  //////////////////////
  // Default work pools

  /**
   * Create the default LWTPools that will be used when no custom pool is provided to BaseWorker.
   *
   * @param params
   */
  public static void createDefaultWorkPools(DefaultWorkPoolParameters params) {
    boolean _created;
    synchronized (synch) {
      _created = created.getAndSet(true);
      if (!_created) {
        if (params.getNumNonConcurrentThreads() > 0) {
          if (defaultNonConcurrentWorkPool != null && !params.getIgnoreDoubleInit()) {
            throw new RuntimeException("Double initialization");
          } else {
            if (defaultNonConcurrentWorkPool == null) {
              defaultNonConcurrentWorkPool =
                  createPool(
                      LWTPoolParameters.create("defaultNonConcurrent")
                          .targetSize(params.getNumNonConcurrentThreads())
                          .maxSize(params.getMaxConcurrentThreads())
                          .workUnit(params.getWorkUnit()));
            }
          }
        }
        if (params.getNumConcurrentThreads() > 0) {
          if (defaultConcurrentWorkPool != null && !params.getIgnoreDoubleInit()) {
            throw new RuntimeException("Double initialization");
          } else {
            if (defaultConcurrentWorkPool == null) {
              defaultConcurrentWorkPool =
                  createPool(
                      LWTPoolParameters.create("defaultConcurrent")
                          .targetSize(params.getNumConcurrentThreads())
                          .maxSize(params.getMaxConcurrentThreads())
                          .workUnit(params.getWorkUnit()));
            }
          }
        }
      }
    }
  }

  private static AtomicBoolean created = new AtomicBoolean();

  /** Create the default LWTPools using the default parameters. */
  public static void createDefaultWorkPools() {
    createDefaultWorkPools(DefaultWorkPoolParameters.defaultParameters());
  }

  public static void stopDefaultWorkPools() {
    synchronized (synch) {
      if (defaultNonConcurrentWorkPool != null) {
        defaultNonConcurrentWorkPool.stop();
        defaultNonConcurrentWorkPool = null;
      }

      if (defaultConcurrentWorkPool != null) {
        defaultConcurrentWorkPool.stop();
        defaultConcurrentWorkPool = null;
      }
      created.set(false);
    }
  }
  //////////////////////
  // Custom work pools

  /**
   * Create a custom LWTPool
   *
   * @param lwtPoolParameters
   * @return
   */
  public static LWTPool createPool(LWTPoolParameters lwtPoolParameters) {
    return new LWTPoolImpl(lwtPoolParameters);
  }
}
