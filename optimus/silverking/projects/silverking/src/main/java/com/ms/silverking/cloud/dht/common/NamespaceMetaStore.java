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
package com.ms.silverking.cloud.dht.common;

import java.io.File;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.Namespace;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.client.impl.ActiveClientOperationTable;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceNotCreatedException;
import com.ms.silverking.cloud.dht.daemon.storage.NamespaceOptionsClientSS;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.net.IPAddrUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides NamespaceOptions for the StorageModule. Internally uses NamespaceOptionsClientBase to retrieve options.
 */
public class NamespaceMetaStore {
  private final DHTSession session;
  private final NamespaceOptionsMode nsOptionsMode;
  private final NamespaceOptionsClientSS nsOptionsClient;
  private final ConcurrentMap<Long, NamespaceProperties> nsPropertiesMap;
  private final String localHostPort;

  private static Logger log = LoggerFactory.getLogger(NamespaceMetaStore.class);

  private static final long nsOptionsFetchTimeoutMillis =
      SessionOptions.getDefaultTimeoutController().getMaxRelativeTimeoutMillis(
      null);

  public enum NamespaceOptionsRetrievalMode {FetchRemotely, LocalCheckOnly}

  ;

  private static final boolean debug = true;

  public NamespaceMetaStore(DHTSession session, ClientDHTConfigurationProvider dhtConfigProvider) {
    this.session = session;
    try {
      int port;
      ClientDHTConfiguration dhtConfig;

      ActiveClientOperationTable.disableFinalization();
      nsOptionsMode = new MetaClient(
          dhtConfigProvider.getClientDHTConfiguration()).getDHTConfiguration().getNamespaceOptionsMode();
      switch (nsOptionsMode) {
      case ZooKeeper:
        nsOptionsClient = new NamespaceOptionsClientZKImpl(dhtConfigProvider);
        break;
      case MetaNamespace:
        nsOptionsClient = new NamespaceOptionsClientNSPImpl(session, dhtConfigProvider);
        break;
      default:
        throw new RuntimeException("Unhandled nsOptionsMode: " + nsOptionsMode);
      }
      nsPropertiesMap = new ConcurrentHashMap<>();
      dhtConfig = dhtConfigProvider.getClientDHTConfiguration();
      port = dhtConfig.getPort();
      if (!dhtConfig.hasPort()) {
        port = new MetaClient(dhtConfig).getDHTConfiguration().getPort();
      }
      localHostPort = IPAddrUtil.localIPString() + ":" + port;
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }

  public static NamespaceMetaStore create(ClientDHTConfigurationProvider dhtConfigProvider) {
    try {
      return new NamespaceMetaStore(new DHTClient().openSession(dhtConfigProvider), dhtConfigProvider);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected exception", e);
    }
  }

  public Optional<NamespaceProperties> getNsPropertiesForRecovery(File nsDir) throws IOException {
    long nsContext;

    nsContext = NamespaceUtil.dirNameToContext(nsDir.getName());
    if (nsContext == NamespaceUtil.metaInfoNamespace.contextAsLong()) {
      return Optional.of(NamespaceUtil.metaInfoNamespaceProperties);
    } else {
      try {
        /*
         * - ZKImpl will directly read from ZK
         * - NSPImpl will read from "properties" file
         */
        return nsOptionsClient.getNsPropertiesForRecovery(nsDir);
      } catch (NamespacePropertiesRetrievalException re) {
        throw new IOException("Cannot get nsProperties to recover the existing ns [" + nsDir + "]", re);
      }
    }
  }

  public boolean needPropertiesFileBootstrap() {
    return nsOptionsMode != NamespaceOptionsMode.ZooKeeper;
  }

  public boolean isAutoDeleteEnabled() {
    // For now only only ZooKeeperImpl supports deletion
    return nsOptionsMode == NamespaceOptionsMode.ZooKeeper;
  }

  // FUTURE - THINK ABOUT COMPLETELY REMOVING ANY READS/WRITES TO META NS FROM THE SERVER SIDE
  // clients can probably do it all

  public NamespaceProperties getNamespaceProperties(long namespace, NamespaceOptionsRetrievalMode retrievalMode) {
    try {
      NamespaceProperties nsProperties;

      nsProperties = nsPropertiesMap.get(namespace);
      if (nsProperties != null) {
        return nsProperties;
      } else {
        switch (retrievalMode) {
        case LocalCheckOnly:
          return null;
        case FetchRemotely:
          nsProperties = nsOptionsClient.getNamespacePropertiesWithTimeout(namespace, nsOptionsFetchTimeoutMillis);
          if (nsProperties != null) {
            nsPropertiesMap.put(namespace, nsProperties);
            return nsProperties;
          } else {
            log.info("Namespace not found {}", namespace);
            log.info("{}",getNamespaceMetaDataReplicas(namespace));
            throw new NamespaceNotCreatedException(Long.toHexString(namespace));
          }
        default:
          throw new RuntimeException("Panic");
        }
      }
    } catch (TimeoutException te) {
      log.info("Failed to retrieve namespace due to timeout {}" , String.format("{}", namespace));
      log.info("{}",getNamespaceMetaDataReplicas(namespace));
      throw new NamespaceNotCreatedException(Long.toHexString(namespace), te);
    } catch (NamespacePropertiesRetrievalException re) {
      log.info("Failed to retrieve namespace {}" , String.format("{}", namespace));
      log.info("{}",getNamespaceMetaDataReplicas(namespace));
      log.info("{}",re);
      throw new NamespaceNotCreatedException(Long.toHexString(namespace), re);
    }
  }

  private String getNamespaceMetaDataReplicas(long ns) {
    SynchronousNamespacePerspective<String, String> syncNSP;
    String locations;

    syncNSP = session.getNamespace(Namespace.replicasName).openSyncPerspective(String.class, String.class);
    try {
      locations = syncNSP.get(Long.toString(ns));
      return locations;
    } catch (RetrievalException re2) {
      log.info("{}",re2.getDetailedFailureMessage());
      log.info("Unexpected failure attempting to find replica locations " + "during failed ns retrieval processing");
      return "Error";
    }
  }

  public void setNamespaceProperties(long namespace, NamespaceProperties nsProperties) {
    nsPropertiesMap.putIfAbsent(namespace, nsProperties);
  }

  public void stop() {
    session.close();
  }
}
