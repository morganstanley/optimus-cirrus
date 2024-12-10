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
 
package optimus.dht.common.util.registry;

import com.ms.infra.zookeeper.utils.ConnectionInfo;
import com.ms.infra.zookeeper.utils.hosted.ZkConnectionResolver;
import msjava.zkapi.internal.ZkaResourceContext;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.yaml.snakeyaml.error.YAMLException;

public class ZKUtils {

  public static String stripPrefix(String path) {
    return path.substring(path.indexOf('/'));
  }

  public static ConnectionInfo verifyAndReturnConnectionInfo(ZkConfig config) {
    try {
      return ZkConnectionResolver.fromEnvRegionChroot(
              config.getEnv(), config.getRegion(), config.getRootNode())
          .resolve();
    } catch (YAMLException | IllegalArgumentException e) {
      throw new RuntimeException("Unable to resolve root zk node", e);
    }
  }

  public static CuratorFrameworkFactory.Builder getFrameworkBuilder(ZkConfig config) {
    ConnectionInfo connectionInfo = verifyAndReturnConnectionInfo(config);
    return CuratorFrameworkFactory.builder()
        .dontUseContainerParents()
        .ensembleProvider(new FixedEnsembleProvider(config.getRootNode()))
        .retryPolicy(ZkaResourceContext.RETRY_POLICY)
        .zookeeperFactory(
            new ZkClientUtilsZookeeperFactory(
                config, connectionInfo.connectString(), connectionInfo.proid()));
  }
}
*/