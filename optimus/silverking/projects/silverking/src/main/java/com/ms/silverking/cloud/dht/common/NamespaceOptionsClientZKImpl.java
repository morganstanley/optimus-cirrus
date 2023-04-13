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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.daemon.storage.NamespacePropertiesIO;
import com.ms.silverking.cloud.dht.meta.MetaPaths;
import com.ms.silverking.cloud.meta.MetaClientCore;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceOptionsClientZKImpl extends NamespaceOptionsClientBase {
  private static final long nanosPerMilli = 1000000;
  private static final long defaultTimeoutMills = 30 * 1000; // 30 seconds
  private final static String implName = "ZooKeeper";

  private static Logger log = LoggerFactory.getLogger(NamespaceOptionsClientZKImpl.class);

  private final static String softDeletePlaceholder = "deleted";
  private final static String versionNodeName = "versions";

  private static String getZKBaseVersionPath(String nsZKBasePath) {
    return nsZKBasePath + "/" + versionNodeName;
  }

  private final long relTimeoutMillis;
  private final MetaClientCore metaZK;
  private final ClientDHTConfiguration dhtConfig;

  public NamespaceOptionsClientZKImpl(ClientDHTConfigurationProvider dhtConfigurationProvider, long relTimeoutMillis)
      throws IOException, KeeperException {
    super(dhtConfigurationProvider);
    this.relTimeoutMillis = relTimeoutMillis;
    this.dhtConfig = dhtConfigurationProvider.getClientDHTConfiguration();
    this.metaZK = new MetaClientCore(dhtConfig.getZKConfig());
  }

  public NamespaceOptionsClientZKImpl(ClientDHTConfigurationProvider dhtConfigurationProvider)
      throws IOException, KeeperException {
    this(dhtConfigurationProvider, defaultTimeoutMills);
  }

  private String getNsZKBasePath(String nsDirName) {
    return MetaPaths.getNsPropertiesBasePath(dhtConfig.getName(), nsDirName);
  }

  private String getNsZKBasePath(long nsContext) {
    return getNsZKBasePath(NamespaceUtil.contextToDirName(nsContext));
  }

  private String resolveVersionPath(long nsContext) {
    return getZKBaseVersionPath(getNsZKBasePath(nsContext));
  }

  private long retrieveNsCreationTime(String versionPath) throws KeeperException {
    SilverKingZooKeeperClient zk;

    zk = metaZK.getZooKeeper();
    return zk.getStat(zk.getLeastVersionPath(versionPath)).getCtime() * nanosPerMilli;
  }

  @Override
  protected long getDefaultRelTimeoutMillis() {
    return relTimeoutMillis;
  }

  // Helper method shared by both put and delete
  private void writeNewVersion(long nsContext, String zkNodeContent) throws KeeperException {
    SilverKingZooKeeperClient zk;
    String versionPath;

    zk = metaZK.getZooKeeper();
    versionPath = resolveVersionPath(nsContext);
    if (!zk.exists(versionPath)) {
      zk.createAllNodes(versionPath);
    }
    zk.createString(versionPath + "/", zkNodeContent, CreateMode.PERSISTENT_SEQUENTIAL);
  }

  @Override
  protected void putNamespaceProperties(long nsContext, NamespaceProperties nsProperties)
      throws NamespacePropertiesPutException {
    try {
      if (nsProperties == null) {
        throw new NamespacePropertiesPutException("null NamespaceProperties is given");
      }
      writeNewVersion(nsContext, nsProperties.toString());
    } catch (KeeperException ke) {
      throw new NamespacePropertiesPutException(ke);
    }
  }

  @Override
  protected void deleteNamespaceProperties(long nsContext) throws NamespacePropertiesDeleteException {
    try {
      writeNewVersion(nsContext, softDeletePlaceholder);
    } catch (KeeperException ke) {
      throw new NamespacePropertiesDeleteException(ke);
    }
  }

  private NamespaceProperties retrieveFullNamespaceProperties(String versionPath)
      throws NamespacePropertiesRetrievalException {
    try {
      SilverKingZooKeeperClient zk;
      String skDef;
      NamespaceProperties nsProperties;

      zk = metaZK.getZooKeeper();
      skDef = zk.getString(zk.getLatestVersionPath(versionPath));

      if (skDef.equals(softDeletePlaceholder)) {
        // Current version is soft-deleted, return null to respect the interface behaviour
        return null;
      }

      nsProperties = NamespaceProperties.parse(skDef);
      if (nsProperties.hasCreationTime()) {
        // Migrated nsProperties will have override creationTime, which inherits from __DHT_Meta__
        log.info(
            "Retrieved a nsProperties with overrideCreationTime [ {} ] for ns: [ {} ]", nsProperties.getCreationTime() , nsProperties.getName());
        return nsProperties;
      } else {
        // Enrich with zk ctime
        return nsProperties.creationTime(retrieveNsCreationTime(versionPath));
      }
    } catch (KeeperException ke) {
      return null;
    } catch (Exception ke) {
      throw new NamespacePropertiesRetrievalException(ke);
    }
  }

  @Override
  protected NamespaceProperties retrieveFullNamespaceProperties(long nsContext)
      throws NamespacePropertiesRetrievalException {
    return retrieveFullNamespaceProperties(resolveVersionPath(nsContext));
  }

  @Override
  public Optional<NamespaceProperties> getNsPropertiesForRecovery(File nsDir)
      throws NamespacePropertiesRetrievalException {
    long nsContext;
    Optional<NamespaceProperties> nsPropInZkOpt;

    // Retrieve the namespace properties for this namespace from ZooKeeper
    nsContext = NamespaceUtil.dirNameToContext(nsDir.getName());
    nsPropInZkOpt = Optional.ofNullable(retrieveFullNamespaceProperties(nsContext));

    // If there are properties for this NS present in ZK then we want to cache them to disk in the namespace directory
    // so that they can be consumed from there by the node. If not, we will want to consider this namespace a candidate
    // for deletion later on so there is no need for us to bother messing with any on-disk data for the NS now.
    if (nsPropInZkOpt.isPresent()) {
      NamespaceProperties nsPropInFile;
      NamespaceProperties nsPropInZk;

      nsPropInZk = nsPropInZkOpt.get();

      try {
        // Now get the existing namespace properties on disk, if they exist
        nsPropInFile = NamespacePropertiesIO.read(nsDir);

        // If there are no properties on disk or if they are not the same as those in ZK, we want to rewrite whatever
        // is on disk to match with what is in ZK.
        if (nsPropInFile == null || !nsPropInFile.equals(nsPropInZk)) {
          NamespacePropertiesIO.rewrite(nsDir, nsPropInZk);
          log.info("Rewrote the old nsProperties [{}] to new one [{}]" , nsPropInFile, nsPropInZk );
        }
      } catch (IOException ioe) {
        throw new NamespacePropertiesRetrievalException(ioe);
      }
    }
    return nsPropInZkOpt;
  }

  @Override
  protected String implementationName() {
    return implName;
  }

  ////===== The APIs below are for admin only (not exposed in NamespaceOptionsClientCS / NamespaceOptionsClientSS)
  public void obliterateAllNsProperties() throws NamespacePropertiesDeleteException {
    try {
      String allNsBasePath;
      SilverKingZooKeeperClient zk;

      allNsBasePath = MetaPaths.getGlobalNsPropertiesBasePath(dhtConfig.getName());
      zk = metaZK.getZooKeeper();
      if (zk.exists(allNsBasePath)) {
        zk.deleteRecursive(allNsBasePath);
      }
    } catch (KeeperException ke) {
      throw new NamespacePropertiesDeleteException(ke);
    }
  }

  public Map<String, NamespaceProperties> getAllNamespaceProperties() throws NamespacePropertiesRetrievalException {
    try {
      Map<String, NamespaceProperties> nsNames = new HashMap<>();
      String allNsBasePath;
      SilverKingZooKeeperClient zk;

      allNsBasePath = MetaPaths.getGlobalNsPropertiesBasePath(dhtConfig.getName());
      zk = metaZK.getZooKeeper();
      if (zk.exists(allNsBasePath)) {
        for (String child : zk.getChildren(allNsBasePath)) {
          NamespaceProperties nsProperties;

          nsProperties = retrieveFullNamespaceProperties(getZKBaseVersionPath(allNsBasePath + "/" + child));
          nsNames.put(child, nsProperties);
        }
      }
      return nsNames;
    } catch (KeeperException ke) {
      throw new NamespacePropertiesRetrievalException(ke);
    }
  }
}
