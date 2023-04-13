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
package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import com.ms.silverking.id.UUIDBase;

public class SynchronizationPoint extends Action {
  private final String name;

  private SynchronizationPoint(UUIDBase uuid, String name, Action[] upstreamDependencies) {
    super(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits(), upstreamDependencies);

    this.name = name;
  }

  public static SynchronizationPoint of(String name, Action[] upstreamDependencies) {
    return new SynchronizationPoint(UUIDBase.random(), name, upstreamDependencies);
  }

  public static SynchronizationPoint of(String name, Action upstreamDependency) {
    Action[] upstreamDependencies;

    if (upstreamDependency != null) {
      upstreamDependencies = new Action[1];
      upstreamDependencies[0] = upstreamDependency;
    } else {
      upstreamDependencies = new Action[0];
    }
    return of(name, upstreamDependencies);
  }

  public static SynchronizationPoint of(String hexString) {
    return of(hexString, new Action[0]);
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return name;
  }
}
