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
package optimus.dht.client.api.servers;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a connection to a server. It doesn't need to be established yet, as
 * connections are lazily established first time we send data.
 *
 * <p>Once a connection enters terminal state (like DISCONNECTED), a new object needs to be used to
 * represent a re-established connection, with incremented reconnectCount.
 */
public class ServerConnection implements Comparable<ServerConnection>, Serializable {

  private static final Logger logger = LoggerFactory.getLogger(ServerConnection.class);

  private static final AtomicReferenceFieldUpdater<ServerConnection, ServerConnectionState>
      STATE_UPDATER =
          AtomicReferenceFieldUpdater.newUpdater(
              ServerConnection.class, ServerConnectionState.class, "state");

  private volatile ServerConnectionState state;

  private final String host;
  private final int port;
  private final InetSocketAddress serverAddress;
  private final String uniqueId;
  private final String cloudName;
  private final int reconnectCount;
  private final ServerIdentity serverIdentity;

  private final int hashCode;

  public ServerConnection(
      String host,
      int port,
      InetSocketAddress serverAddress,
      String uniqueId,
      String cloudName,
      int reconnectCount,
      ServerConnectionState state) {
    this.host = host;
    this.port = port;
    this.serverAddress = serverAddress;
    this.uniqueId = uniqueId;
    this.cloudName = cloudName;
    this.reconnectCount = reconnectCount;
    this.serverIdentity = new ServerIdentity();
    this.state = state;

    this.hashCode = Objects.hash(host, port, uniqueId, reconnectCount);
  }

  public String host() {
    return host;
  }

  public int port() {
    return port;
  }

  public InetSocketAddress serverAddress() {
    return serverAddress;
  }

  public String uniqueId() {
    return uniqueId;
  }

  public String cloudName() {
    return cloudName;
  }

  public ServerConnectionState state() {
    return state;
  }

  public int reconnectCount() {
    return reconnectCount;
  }

  public ServerIdentity serverIdentity() {
    return serverIdentity;
  }

  public boolean tryUpdateState(
      ServerConnectionState expectedState, ServerConnectionState newState) {
    boolean updated = STATE_UPDATER.compareAndSet(this, expectedState, newState);
    if (updated) {
      logger.debug("Changed state of node, old={}, new={}, node={}", expectedState, newState, this);
    }
    return updated;
  }

  public ServerConnection withResolvedServerAddress(InetSocketAddress resolvedServerAddress) {
    return new ServerConnection(
        host,
        port,
        resolvedServerAddress,
        uniqueId,
        cloudName,
        reconnectCount,
        ServerConnectionState.NEW);
  }

  public ServerConnection asUnresolved() {
    return new ServerConnection(
        host,
        port,
        serverAddress,
        uniqueId,
        cloudName,
        reconnectCount,
        ServerConnectionState.BAD_HOSTNAME);
  }

  public ServerConnection withIncrementedReconnectCount() {
    return new ServerConnection(
        host,
        port,
        serverAddress,
        uniqueId,
        cloudName,
        reconnectCount + 1,
        ServerConnectionState.DISCONNECTED);
  }

  @Override
  public int compareTo(ServerConnection o) {
    int hostCompare = host.compareTo(o.host);
    if (hostCompare != 0) return hostCompare;

    int portCompare = Integer.compare(port, o.port);
    if (portCompare != 0) return portCompare;

    int uniqueIdCompare = uniqueId.compareTo(o.uniqueId);
    if (uniqueIdCompare != 0) return uniqueIdCompare;

    // reverse order - nodes with higher reconnectCount go first
    return Integer.compare(o.reconnectCount, reconnectCount);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ServerConnection) {
      ServerConnection other = (ServerConnection) obj;
      return Objects.equals(host, other.host)
          && port == other.port
          && Objects.equals(uniqueId, other.uniqueId)
          && reconnectCount == other.reconnectCount;
    }
    return false;
  }

  @Override
  public String toString() {
    return "[host="
        + host
        + ", port="
        + port
        + ", uniqueId="
        + uniqueId
        + ", state="
        + state
        + ", reconnectCount="
        + reconnectCount
        + ", cloudName="
        + cloudName
        + "]";
  }

  /** This class represents a remote server. */
  public class ServerIdentity implements Serializable {

    private final int hashCode = Objects.hash(host, port, uniqueId);

    public ServerConnection serverConnection() {
      return ServerConnection.this;
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

    public String cloudName() {
      return cloudName;
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj instanceof ServerIdentity) {
        ServerConnection other = ((ServerIdentity) obj).serverConnection();
        return Objects.equals(host, other.host)
            && port == other.port
            && Objects.equals(uniqueId, other.uniqueId);
      }
      return false;
    }

    @Override
    public String toString() {
      return "[host="
          + host
          + ", port="
          + port
          + ", uniqueId="
          + uniqueId
          + ", cloudName="
          + cloudName
          + "]";
    }
  }
}
