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
package com.ms.silverking.cloud.dht.net;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.collection.HashedSetMap;
import com.ms.silverking.net.AddrAndPort;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Background: Under "normal" operation, each SK daemon has a fixed port bound to a single IP address. The port
 * is the same for all daemons. This implies that only a single daemon may run per server. It also implies that all
 * traffic will go over a single interface.
 * <p>
 * IP aliasing allows us to overcome these limitations. IP aliasing does this in two ways:
 * 1) The aliasing feature can be used to allow a daemon to be identified by a "fake" IPAndPort. This IPAndPort is
 * used in the topology and almost all of the rest of the code as if it were a real IP. This IPAliasMap class is
 * used by MessageGroupBase (and a few other classes) to translate this IPAndPort into an actual IPAndPort when
 * communication needs to take place. By design, most of the code is unaware of this translation.
 * 2) The aliasing feature allows traffic to be distributed across multiple interfaces. This is done by associating
 * multiple interfaces with an actual IPAndPort. In this case, MessageGroupBase will translate the usual IPAndPort
 * into one of the many remote interfaces in a round-robin fashion.
 * <p>
 * Terminology: for consistency in the above two cases, this class calls the IP address used to identify a daemon
 * the "daemon IP" or "daemon" for short. Note that the port is fixed to the SK instance port for this case.
 * Each of the one or more IPAndPorts associated with a daemon IP is called an "interface IP and port" or
 * "interface" for short. The "interface IP and port" are always a real IP and port. The daemon IP is "fake"
 * for case 1, but an actual IP for case 2.
 * <p>
 * For case (1) each daemon IP acts as an alias, and has a single interface IP and port to which it is associated.
 * For case (2) each daemon IP has several interface IPAndPorts with which it is associated.
 * <p>
 * Colloquially, for case (1) we can call the daemon IP an "alias". We don't do that in this code to avoid confusion
 * with case (2) where we could call the interface IP and ports aliases. (The daemon IP is real in this case.)
 * <p>
 * Notes on case (1) - setting up "aliases"/fake IP addresses for each daemon IP.
 * - the topology, host groups, etc, must be composed of these daemon IPs, not interface IPs
 * - the DHT config must have an ipAliasMap field in the form ipAliasMap={daemonIp=interfaceIp:port}
 * As mentioned previously, the vast majority of the code will use this daemon IP as if it were the actual IP.
 * In addition, users will only use the daemon IP.
 * <p>
 * All translation between daemon IP and interface IP and ports will take place in this class.
 * Clients will select a daemon IP address from the topology as their preferredServer. This is dealiased in
 * DHTSessionImpl to an interface IP and port, to which the client communicates.
 * Aliasing also allows nodes to deviate from the fixed port in DHT config; such nodes would require an alias
 * map entry which maps a fake IP to the port actually in use for a given address. The node itself must be passed
 * -port N on the command line at start up in order to run on a port different to that specified in DHT config.
 * This also allows many nodes to run on the same machine; several daemon IP entries in the maps could exist,
 * which resolve to the same host, but with unique ports. Note that each daemon IP must be unique in this case
 * <p>
 * It is not required that all nodes have an alias identity, so long as it is consistent between the topology
 * and host group files, and such nodes can be omitted from the alias map. The alias resolution, when it
 * does not find an entry for such a node, would leave the address unchanged and clients/servers would proceed
 * to try and connect directly
 * <p>
 * Notes on case 2:
 * When aliasing is used to support multi-homing, each daemon IP will be associated with
 * several interface and ports, and the dht config will have an entry of the
 * form: ipAliasMap={daemonIp=interfaceIp:port,...}. Note that all ports must be the same for case 2.
 * The ports may also be omitted.
 */
public class IPAliasMap {
  private final Map<IPAndPort, IPAndPort[]> daemonToInterfaces; // one-to-many
  private final Map<IPAndPort, IPAndPort> interfaceToDaemon; // many-to-one
  private final HashedSetMap<String, IPAndPort> interfaceIPToDaemons;
  private final AtomicInteger interfaceIndex; // used for round-robin between aliases
  // (e.g. for distributing traffic across multi-homed interfaces)

  private static final boolean logDaemonToInterfaceMapping = true;
  private static final boolean logInterfaceToDaemonMapping = true;

  private static Logger log = LoggerFactory.getLogger(IPAliasMap.class);

  public IPAliasMap(Map<IPAndPort, IPAndPort[]> daemonToInterfaces) {
    interfaceToDaemon = new ConcurrentHashMap<>();
    interfaceIPToDaemons = new HashedSetMap<>();
    if (daemonToInterfaces == null) {
      this.daemonToInterfaces = ImmutableMap.of();
      interfaceIndex = null;
    } else {
      quickCheckMap(daemonToInterfaces);
      this.daemonToInterfaces = new ConcurrentHashMap<>();
      interfaceIndex = new AtomicInteger();
      this.daemonToInterfaces.putAll(daemonToInterfaces);
      if (logDaemonToInterfaceMapping) {
        log.info("{}",daemonToInterfaces);
      }
      addReverseMappings(daemonToInterfaces);
    }
  }
  
  public static IPAliasMap identityMap() {
    return new IPAliasMap(null);
  }

  private void quickCheckMap(Map<IPAndPort, IPAndPort[]> daemonToInterfaces) {
    ensureDaemonPortsMatch(daemonToInterfaces);
  }

  private void ensureDaemonPortsMatch(Map<IPAndPort, IPAndPort[]> daemonToInterfaces) {
    int daemonPort;

    daemonPort = -1;
    for (IPAndPort daemonIPAndPort : daemonToInterfaces.keySet()) {
      if (daemonPort < 0) {
        daemonPort = daemonIPAndPort.getPort();
      } else {
        if (daemonIPAndPort.getPort() != daemonPort) {
          throw new RuntimeException("IPAliasMap daemon ports must all be the same");
        }
      }
    }
  }

  public Map<IPAndPort, IPAndPort[]> getDaemonToInterfacesMap() {
    return ImmutableMap.copyOf(daemonToInterfaces);
  }

  public Map<IPAndPort, IPAndPort> getInterfaceToDaemonMap() {
    return ImmutableMap.copyOf(interfaceToDaemon);
  }

  // daemon=>interface

  public AddrAndPort daemonToInterface(AddrAndPort daemon) {
    AddrAndPort[] interfaces;

    interfaces = daemonToInterfaces.get(daemon);
    if (interfaces != null) {
      // Below is random. For now we implement round robin instead
      //_dest = interfaces[ThreadLocalRandom.current().nextInt(interfaces.length)];
      // Round robin
      return interfaces[interfaceIndex.getAndIncrement() % interfaces.length];
    } else {
      return daemon;
    }
  }

  // interface=>daemon

  public IPAndPort interfaceToDaemon(InetSocketAddress interfaceIPAndPort) {
    return interfaceToDaemon(new IPAndPort(interfaceIPAndPort));
  }

  public IPAndPort interfaceToDaemon(IPAndPort interfaceIPAndPort) {
    IPAndPort daemon;

    daemon = interfaceToDaemon.get(interfaceIPAndPort);
    return daemon != null ? daemon : interfaceIPAndPort;
  }

  /**
   * For all entries in the daemonToInterfaces argument, update the
   * reverse map. That is, update the interfaceToDaemon map with
   * the corresponding mappings.
   *
   * @param daemonToInterfaces the forward map to reverse
   */
  private void addReverseMappings(Map<IPAndPort, IPAndPort[]> daemonToInterfaces) {
    for (Map.Entry<IPAndPort, IPAndPort[]> entry : daemonToInterfaces.entrySet()) {
      addInterfacesToDaemon(entry.getValue(), entry.getKey());
    }
  }

  // only used at startup
  private void addInterfacesToDaemon(IPAndPort[] interfaces, IPAndPort daemon) {
    for (IPAndPort interfaceIPAndPort : interfaces) {
      addInterfaceToDaemon(interfaceIPAndPort, daemon, false);
      addIPToDaemonMapping(interfaceIPAndPort.getIPAsString(), daemon);
    }
  }

  // used at runtime to map reverse connections
  public void addInterfaceToDaemon(IPAndPort interfaceIPAndPort, IPAndPort daemonIPAndPort) {
    addInterfaceToDaemon(interfaceIPAndPort, daemonIPAndPort, true);
  }

  private void addInterfaceToDaemon(IPAndPort interfaceIPAndPort, IPAndPort daemonIPAndPort, boolean allowRemapping) {
    IPAndPort prev;

    prev = interfaceToDaemon.put(interfaceIPAndPort, daemonIPAndPort);
    if (logInterfaceToDaemonMapping && (prev == null || !prev.equals(daemonIPAndPort))) {
      log.debug("IPAliasMap adding interfaceIPAndPort=>daemonIPAndPort: {}=>{}", interfaceIPAndPort, daemonIPAndPort);
    }
    if (!allowRemapping && prev != null && !prev.equals(daemonIPAndPort)) {
      throw new RuntimeException("Interface maps to multiple daemons: " + interfaceIPAndPort);
    }
  }

  private void addIPToDaemonMapping(String ip, IPAndPort daemonIPAndPort) {
    interfaceIPToDaemons.addValue(ip, daemonIPAndPort);
  }

  /**
   * Given an interface ip, randomly select one of the possibly many associated daemons
   *
   * @param ip
   * @return
   */
  public IPAndPort interfaceIPToRandomDaemon(String ip) {
    return interfaceIPToDaemons.getAnyValue(ip);
  }

  /**
   * If an interface ip uniquely identifies a daemon ip, return that daemon
   *
   * @param ip
   * @return the uniquely associated daemon
   */
  public IPAndPort interfaceIPToDaemon_ifUnique(String ip) {
    Set<IPAndPort> daemonIPAndPorts;

    daemonIPAndPorts = interfaceIPToDaemons.getSet(ip);
    if (daemonIPAndPorts == null || daemonIPAndPorts.size() > 1) {
      return null;
    } else {
      return daemonIPAndPorts.iterator().next();
    }
  }

  @Override
  public String toString() {
    StringBuffer sb;

    sb = new StringBuffer();
    sb.append("{");
    for (Map.Entry<IPAndPort, IPAndPort[]> entry : daemonToInterfaces.entrySet()) {
      sb.append(String.format("%s -> [", entry.getKey()));
      for (IPAndPort _interface : entry.getValue()) {
        sb.append(_interface + " ");
      }
      sb.append("]\n");
    }
    sb.append("}");
    return sb.toString() + "\n" + CollectionUtil.mapToString(interfaceToDaemon);
  }
}
