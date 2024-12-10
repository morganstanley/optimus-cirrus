/* /*
 * Morgan Stanley makes this available to you under the Apache License, Version 2.0 (the "License").
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
 * See the NOTICE file distributed with this work for additional information regarding copyright ownership.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 
package optimus.dht.common.util.registry;

import java.net.UnknownHostException;

import com.ms.infra.zookeeper.utils.ConnectionInfo;
import com.ms.infra.zookeeper.utils.ZkClientFactory;
import com.ms.infra.zookeeper.utils.hosted.ZkConnectionResolver;
import com.ms.infra.zookeeper.utils.hosted.ZkEnv;
import com.ms.infra.zookeeper.utils.hosted.ZkRegion;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import com.ms.zookeeper.clientutils.ZkClientUtils;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkClientUtilsZookeeperFactory implements ZookeeperFactory {

  private static final Logger logger = LoggerFactory.getLogger(ZkClientUtilsZookeeperFactory.class);

  private final ZkConfig config;
  private volatile String connectionString;
  private final String zkProdId;

  public ZkClientUtilsZookeeperFactory(ZkConfig config, String connectionString, String zkProdId) {
    this.config = config;
    this.connectionString = connectionString;
    this.zkProdId = zkProdId;
  }

  @Override
  public ZooKeeper newZooKeeper(
      String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly)
      throws Exception {

    try {
      /*
       * ZKClientUtils tries loading a publicly available YAML file on AFS which contains the list of ZooKeeper
       * servers. Sometimes, due to AFS transient failures, that file cannot be read and a YAMLException is
       * thrown. Since it is impossible to check whether starting the curator was successful, this case needs to be
       * handled explicitly.
       

      // here the connectString is intentionally a rootnode without
      // servers prepended
      connectionString = ZKUtils.verifyAndReturnConnectionInfo(config).connectString();
    } catch (Exception e) {
      logger.warn("Error while accessing file containing root nodes on AFS. Using the old one", e);
    }

    try {
      boolean useSASL =
          Boolean.parseBoolean(System.getProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY));
      ZKClientConfig zkClientConfig = new ZKClientConfig();
      zkClientConfig.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "false");
      ZkEnv zkEnv = config.getEnv();
      ZkRegion zkRegion = config.getRegion();
      String rootNode = config.getRootNode();
      ConnectionInfo info =
          ZkConnectionResolver.fromEnvRegionChroot(zkEnv, zkRegion, rootNode).resolve();
      return config.isKerberos()
          ? (useSASL
              ? ZkClientFactory.create()
                  .watcher(watcher)
                  .sessionTimeout(sessionTimeout)
                  .newZooKeeper(info)
              : ZkClientUtils.getZooKeeper(connectionString, zkProdId, sessionTimeout, watcher))
          : new ZooKeeper(connectionString, sessionTimeout, watcher, zkClientConfig);
    } catch (UnknownHostException e) {
      /*
       * If the network or the DNS server fails during runtime this will throw an UnknownHostException which is
       * not retried, so service pubsub will not recover. Replacing it with a ConnectionLossException makes it
       * always retriable.
       *
       * Catching this here means that we won't detect legitimately bad hostnames either, but since those are
       * provided by sam/zookeeper we assume they are correct.
       
      KeeperException ex = new KeeperException.ConnectionLossException();
      ex.initCause(e);
      throw ex;
    }
  }
}
 */