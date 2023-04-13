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

/**
 * Provides concrete WorkerGroup implementations
 */
public class WorkerGroupProvider {
  public static <I> WorkerGroup<I> createWorkerGroup(String name, int maxDirectCallDepth, int idleThreadThreshold) {
    return new WorkerGroupImpl<I>(name, maxDirectCallDepth, idleThreadThreshold);
  }

  public static <I> WorkerGroup<I> createWorkerGroup() {
    return createWorkerGroup(null, LWTConstants.defaultMaxDirectCallDepth, LWTConstants.defaultIdleThreadThreshold);
  }
}
