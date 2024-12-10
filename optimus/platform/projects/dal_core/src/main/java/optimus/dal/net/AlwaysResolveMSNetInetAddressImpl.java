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
package optimus.dal.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

import msjava.msnet.MSNetInetAddress;
import msjava.msnet.MSNetInetAddressImpl;

public class AlwaysResolveMSNetInetAddressImpl implements MSNetInetAddressImpl {

  private final InetSocketAddress unresolvedIsa;
  // resolve hostname only once
  private final String host;

  private InetSocketAddress lastMaybeResolvedIsa;

  public AlwaysResolveMSNetInetAddressImpl(InetSocketAddress unresolvedIsa) {
    if (!unresolvedIsa.isUnresolved()) {
      throw new IllegalArgumentException(
          "Unresolved address expected, but got " + unresolvedIsa + " instead");
    }
    this.unresolvedIsa = unresolvedIsa;
    this.host = unresolvedIsa.getHostName();
  }

  
  public synchronized InetSocketAddress getInetSocketAddress() {
    ensureResolveAttempted();
    return lastMaybeResolvedIsa;
  }

  /**
   * Return true if the address and port are valid.
   *
   * @return true if the address and port are valid.
   */
  
  public synchronized boolean isValid() {
    ensureResolveAttempted();
    return !lastMaybeResolvedIsa.isUnresolved();
  }

  
  public int getPort() {
    return unresolvedIsa.getPort();
  }

  /**
   * Return the hostname. If address is for any/all local addresses, the string "any" will be
   * returned.
   *
   * @return the hostname, or "any" for all local addresses.
   */
  
  public String getHost() {
    return host;
  }

  /**
   * Gets the InetAddress.
   *
   * @return the InetAddress
   */
  
  public synchronized InetAddress getInetAddress() {
    ensureResolveAttempted();
    return lastMaybeResolvedIsa.getAddress();
  }

  
  public int hashCode() {
    return Objects.hash(host, getPort());
  }

  
  public synchronized String toString() {
    if (lastMaybeResolvedIsa == null) {
      return host + ":" + getPort();
    } else if (lastMaybeResolvedIsa.isUnresolved()) {
      return host + "/UNRESOLVABLE:" + getPort();
    } else {
      return lastMaybeResolvedIsa.getAddress() + ":" + getPort();
    }
  }

  
  public String getAddressString() {
    return host + ":" + getPort();
  }

  
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null) return false;

    if (o instanceof MSNetInetAddress) {
      o = ((MSNetInetAddress) o).getImpl();
    }

    if (o instanceof MSNetInetAddressImpl) {
      MSNetInetAddressImpl a = (MSNetInetAddressImpl) o;

      String h1 = getHost();
      String h2 = a.getHost();
      return (((h1 == h2) || ((h1 != null) && (h1.equals(h2)))) && (getPort() == a.getPort()));
    }
    return false;
  }

  private void ensureResolveAttempted() {
    if (lastMaybeResolvedIsa == null) {
      resolve();
    }
  }

  // resolve is called by MSNet before each retry, from MSNetTCPConnection.retryHandler
  
  public synchronized void resolve() {
    lastMaybeResolvedIsa =
        new InetSocketAddress(unresolvedIsa.getHostName(), unresolvedIsa.getPort());
  }
}
