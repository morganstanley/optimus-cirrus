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
package com.ms.silverking.cloud.dht.client;

import java.io.IOException;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;
import com.ms.silverking.cloud.dht.client.impl.DHTSessionImpl;
import com.ms.silverking.cloud.dht.client.serialization.SerializationRegistry;
import com.ms.silverking.cloud.dht.common.NamespaceOptionsMode;
import com.ms.silverking.cloud.dht.common.SimpleValueCreator;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.DHTConfigurationZK;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.dht.net.IPAliasMap;
import com.ms.silverking.cloud.dht.net.IPAliasingUtil;
import com.ms.silverking.cloud.toporing.TopoRingConstants;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.net.IPAddrUtil;
import com.ms.silverking.net.IPAndPort;
import com.ms.silverking.net.async.AsyncGlobals;
import com.ms.silverking.net.async.OutgoingData;
import com.ms.silverking.net.security.AuthFailedException;
import com.ms.silverking.thread.lwt.DefaultWorkPoolParameters;
import com.ms.silverking.thread.lwt.LWTPoolProvider;
import com.ms.silverking.time.AbsMillisTimeSource;
import com.ms.silverking.time.TimerDrivenTimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base client interface to DHT functionality. Provides sessions to specific DHT instances. */
public class DHTClient {
  private static final double concurrentExtraThreadFactor = 1.25;
  private static final double nonConcurrentExtraThreadFactor = 1.0;
  private static final AbsMillisTimeSource absMillisTimeSource;
  private static final int defaultInactiveNodeTimeoutSeconds = 30;
  private static final int defaultClientWorkUnit = 16;
  private static Logger log = LoggerFactory.getLogger(DHTClient.class);
  private static ValueCreator valueCreator;

  static {
    AsyncGlobals.setVerbose(false);
    TopoRingConstants.setVerbose(false);
    LWTPoolProvider.createDefaultWorkPools(
        DefaultWorkPoolParameters.defaultParameters()
            .workUnit(defaultClientWorkUnit)
            .ignoreDoubleInit(true));
    valueCreator = SimpleValueCreator.forLocalProcess();
    absMillisTimeSource = new TimerDrivenTimeSource();
    OutgoingData.setAbsMillisTimeSource(absMillisTimeSource);
  }

  private final SerializationRegistry serializationRegistry;

  /**
   * Construct DHTClient with the specified SerializationRegistry.
   *
   * @param serializationRegistry
   * @throws IOException
   */
  @OmitGeneration
  public DHTClient(SerializationRegistry serializationRegistry) throws IOException {
    this.serializationRegistry = serializationRegistry;
  }

  /**
   * Construct DHTClient with default SerializationRegistry.
   *
   * @throws IOException
   */
  public DHTClient() throws IOException {
    this(SerializationRegistry.createDefaultRegistry());
  }

  /**
   * Return the ValueCreator in use
   *
   * @return the ValueCreator in use
   */
  public static ValueCreator getValueCreator() {
    return valueCreator;
  }

  /**
   * Open a new session to the specified SilverKing DHT instance using default SessionOptions.
   *
   * @param dhtConfigProvider specifies the SilverKing DHT instance
   * @return a new session to the given instance
   * @throws ClientException
   */
  public DHTSession openSession(ClientDHTConfigurationProvider dhtConfigProvider)
      throws ClientException {
    return openSession(new SessionOptions(dhtConfigProvider.getClientDHTConfiguration()));
  }

  /**
   * Open a new session to the specified SilverKing DHT instance using the given SessionOptions.
   *
   * @param sessionOptions options specifying the SilverKing DHT instance and the parameters of this
   *     session
   * @return a new session to the given instance
   * @throws ClientException
   */
  public DHTSession openSession(SessionOptions sessionOptions) throws ClientException {
    ClientDHTConfiguration dhtConfig;
    String preferredServer;
    NamespaceOptionsMode nsOptionsMode;
    boolean enableMsgGroupTrace;
    DHTSession session;
    int serverPort;

    dhtConfig = sessionOptions.getDHTConfig();
    preferredServer = sessionOptions.getPreferredServer();

    if (preferredServer != null) {
      // Handle cases where a reserved preferredServer is used to indicate that an embedded SK
      // instance is desired
      // Normal openSession must have nsOptionsMode
      try {
        DHTConfiguration dhtGlobalConfig;

        dhtGlobalConfig = new MetaClient(dhtConfig).getDHTConfiguration();
        nsOptionsMode = dhtGlobalConfig.getNamespaceOptionsMode();
        enableMsgGroupTrace = dhtGlobalConfig.getEnableMsgGroupTrace();
      } catch (KeeperException | IOException e) {
        throw new ClientException("Failed to read global DHTConfiguration", e);
      }
    } else {
      // This is a typical openSession - not embedded
      // Normal openSession must have nsOptionsMode
      try {
        DHTConfiguration dhtGlobalConfig;

        dhtGlobalConfig = new MetaClient(dhtConfig).getDHTConfiguration();
        nsOptionsMode = dhtGlobalConfig.getNamespaceOptionsMode();
        enableMsgGroupTrace = dhtGlobalConfig.getEnableMsgGroupTrace();
      } catch (KeeperException | IOException e) {
        throw new ClientException("Failed to read global DHTConfiguration", e);
      }
    }

    // Determine the serverPort
    try {
      if (dhtConfig.hasPort()) { // Best to avoid zk if possible
        serverPort = dhtConfig.getPort();
      } else { // not specified, must contact zk
        MetaClient mc;
        MetaPaths mp;
        long latestConfigVersion;

        log.info("dhtConfig.getZkLocs(): {}", dhtConfig.getZKConfig());
        mc = new MetaClient(dhtConfig);
        mp = mc.getMetaPaths();

        log.info("getting latest version: {}", mp.getInstanceConfigPath());
        latestConfigVersion = mc.getZooKeeper().getLatestVersion(mp.getInstanceConfigPath());
        log.info("latestConfigVersion: {}", latestConfigVersion);
        serverPort = new DHTConfigurationZK(mc).readFromZK(latestConfigVersion, null).getPort();
      }
    } catch (Exception e) {
      throw new ClientException(e);
    }

    // Now determine the daemon ip and port, and open a session
    try {
      IPAndPort preferredServerAndPort;
      IPAndPort resolvedServer;
      IPAliasMap aliasMap;

      // See IPAliasMap for discussion of the aliasing, daemon ip:port, and interface ip:port

      aliasMap = IPAliasingUtil.readAliases(dhtConfig);

      // Handle case of no preferred server explicitly specified
      if (preferredServer == null) {
        // When no preferred server is specified, we default to the local host
        preferredServer = IPAddrUtil.localIPString();
        log.debug("No preferred server specified. Using: {}", preferredServer);
        // If any aliases are defined for the local server, randomly select one
        resolvedServer = aliasMap.interfaceIPToRandomDaemon(preferredServer);
        if (resolvedServer != null) {
          log.debug("Found alias(es) for local server; randomly selected: {}", resolvedServer);
        } else {
          resolvedServer = new IPAndPort(IPAddrUtil.serverNameToAddr(preferredServer), serverPort);
        }
      } else {
        // A preferred server was explicitly specified
        preferredServerAndPort =
            new IPAndPort(IPAddrUtil.serverNameToAddr(preferredServer), serverPort);
        log.debug("preferredServerAndPort: {}", preferredServerAndPort);
        // Check if we need to resolve this daemon to an interface
        resolvedServer = (IPAndPort) aliasMap.daemonToInterface(preferredServerAndPort);
        if (resolvedServer != preferredServerAndPort) {
          log.debug("Found alias for {} -> {}", preferredServerAndPort, resolvedServer);
        }
      }

      log.debug("Opening session to resolvedServer: {}", resolvedServer);
      session =
          new DHTSessionImpl(
              dhtConfig,
              resolvedServer,
              absMillisTimeSource,
              serializationRegistry,
              sessionOptions.getTimeoutController(),
              nsOptionsMode,
              enableMsgGroupTrace,
              aliasMap,
              sessionOptions.getSessionPolicyOnDisconnect());
    } catch (IOException | AuthFailedException e) {
      throw new ClientException(e);
    }
    log.debug("session returned: {}", session);
    return session;
  }
}
