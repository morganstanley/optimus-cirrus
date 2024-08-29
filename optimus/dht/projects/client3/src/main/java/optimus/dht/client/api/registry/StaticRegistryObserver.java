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
package optimus.dht.client.api.registry;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.client.api.servers.ServerConnectionState;
import optimus.dht.client.api.servers.ServerConnection.ServerIdentity;

/**
 * RegistryObserver implementation that does not use zk registry, but just returns statically
 * configured list of servers.
 */
public class StaticRegistryObserver implements RegistryObserver {

  public static class StaticServerEntry implements Serializable {

    private final String host;
    private final int port;
    private final String uniqueId;

    public StaticServerEntry(String host, int port, String uniqueId) {
      this.host = host;
      this.port = port;
      this.uniqueId = uniqueId;
    }

    public StaticServerEntry(String hostport, String uniqueId) {
      this(hostport.split(":")[0], Integer.valueOf(hostport.split(":")[1]), uniqueId);
    }

    public String host() {
      return host;
    }

    public int port() {
      return port;
    }

    public String uniqueId() {
      return uniqueId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      StaticServerEntry that = (StaticServerEntry) o;
      return port == that.port
          && Objects.equals(host, that.host)
          && Objects.equals(uniqueId, that.uniqueId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, port, uniqueId);
    }

    @Override
    public String toString() {
      return host + ":" + port + ":" + uniqueId;
    }
  }

  private final Set<ServerConnection.ServerIdentity> servers;

  public static StaticRegistryObserver local(int localPort, String uniqueId) {
    InetSocketAddress address = new InetSocketAddress("127.0.0.1", localPort);
    ServerConnection serverNode =
        new ServerConnection(
            "127.0.0.1", localPort, address, uniqueId, null, 0, ServerConnectionState.NEW);
    return new StaticRegistryObserver(ImmutableSet.of(serverNode.serverIdentity()));
  }

  public static StaticRegistryObserver single(String hostport, String uniqueId) {
    String[] split = hostport.split(":");
    String host = split[0];
    int port = Integer.valueOf(split[1]);
    InetSocketAddress address = new InetSocketAddress(host, port);
    ServerConnection serverNode =
        new ServerConnection(host, port, address, uniqueId, null, 0, ServerConnectionState.NEW);
    return new StaticRegistryObserver(ImmutableSet.of(serverNode.serverIdentity()));
  }

  public static StaticRegistryObserver staticList(Collection<StaticServerEntry> staticServers) {
    Set<ServerIdentity> servers =
        staticServers.stream()
            .map(
                entry ->
                    new ServerConnection(
                        entry.host(),
                        entry.port(),
                        new InetSocketAddress(entry.host(), entry.port()),
                        entry.uniqueId(),
                        null,
                        0,
                        ServerConnectionState.NEW))
            .map(ServerConnection::serverIdentity)
            .collect(Collectors.toSet());
    return new StaticRegistryObserver(servers);
  }

  public StaticRegistryObserver(Set<ServerIdentity> servers) {
    this.servers = servers;
  }

  @Override
  public void registerListener(RegistryNodeChangesListener listener) {
    listener.nodesRegistryChanged(servers);
  }

  @Override
  public void waitUntilConsistentForFirstTime(Duration timeout) throws InterruptedException {
    // NOOP
  }

  @Override
  public boolean isConsistent() {
    return true;
  }

  @Override
  public String toString() {
    return "StaticRegistryObserver [servers=" + servers + "]";
  }
}
