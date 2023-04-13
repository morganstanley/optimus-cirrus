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
package com.ms.silverking.cloud.dht.daemon;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.net.IPAndPort;

public class CompositeReplicaPrioritizer implements ReplicaPrioritizer {
  private final List<ReplicaPrioritizer> replicaPrioritizers;

  public CompositeReplicaPrioritizer(List<ReplicaPrioritizer> replicaPrioritizers) {
    this.replicaPrioritizers = ImmutableList.copyOf(replicaPrioritizers);
  }

  @Override
  public int compare(IPAndPort r0, IPAndPort r1) {
    for (ReplicaPrioritizer rp : replicaPrioritizers) {
      int result;

      result = rp.compare(r0, r1);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }
}
