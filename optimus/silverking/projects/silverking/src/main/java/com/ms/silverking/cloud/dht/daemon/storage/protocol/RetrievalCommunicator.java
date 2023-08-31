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
package com.ms.silverking.cloud.dht.daemon.storage.protocol;

import java.util.ArrayList;
import java.util.List;

import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Not thread-safe. Only to be used for a single processing pass. */
public class RetrievalCommunicator extends OpCommunicator<DHTKey, RetrievalResult>
    implements RetrievalVirtualCommunicator {
  private List<SecondaryReplicasUpdate> secondaryReplicasUpdates;

  private static final int initialSecondaryReplicaUpdateListSize = 4;

  private static final boolean debug = false;

  private static Logger log = LoggerFactory.getLogger(RetrievalCommunicator.class);

  public RetrievalCommunicator() {}

  ////////////////

  @Override
  public void sendResult(RetrievalResult result, List<IPAndPort> secondaryReplicas) {
    if (debug) {
      log.info("RetrievalCommunicator.sendResult() {}", secondaryReplicas.size());
    }
    if (secondaryReplicas.size() > 0) {
      synchronized (this) {
        if (secondaryReplicasUpdates == null) {
          secondaryReplicasUpdates = new ArrayList<>(initialSecondaryReplicaUpdateListSize);
        }
        secondaryReplicasUpdates.add(new SecondaryReplicasUpdate(secondaryReplicas, result));
      }
    }
    super.sendResult(result);
  }

  public List<SecondaryReplicasUpdate> getSecondaryReplicasUpdates() {
    return secondaryReplicasUpdates;
  }
}
