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
package optimus.dht.client.api.replication;

import java.util.Iterator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import optimus.dht.client.api.servers.ServerConnection;

public class SimpleReplicationStrategy implements ReplicationStrategy {

  private final int replicas;

  public SimpleReplicationStrategy(int replicas) {
    Preconditions.checkArgument(replicas > 0, "Replicas must be greater than 0 (was %d)", replicas);
    this.replicas = replicas;
  }

  @Override
  public ReplicatedOperation replicatedOperation(Iterator<ServerConnection> nodesForKey) {
    return new SimpleReplicatedOperation(nodesForKey);
  }

  private class SimpleReplicatedOperation implements ReplicatedOperation {

    private final Iterator<ServerConnection> nodes;
    private final List<ServerConnection> initialServers;

    public SimpleReplicatedOperation(Iterator<ServerConnection> nodes) {
      this.nodes = nodes;
      this.initialServers = ImmutableList.copyOf(Iterators.limit(nodes, replicas));
    }

    @Override
    public List<ServerConnection> initialServers() {
      return initialServers;
    }

    @Override
    public ServerConnection replacement(ServerConnection node) {
      return nodes.hasNext() ? nodes.next() : null;
    }
  }
}
