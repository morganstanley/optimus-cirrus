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

import java.util.List;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import org.apache.curator.utils.ZKPaths;

import com.google.common.base.Splitter;
import com.ms.infra.zookeeper.utils.hosted.ZkEnv;
import com.ms.infra.zookeeper.utils.hosted.ZkRegion;

public class ZkConfig {
  private static final Config cfg = ConfigFactory.load("dht-common3-zkconfig.conf");
  private static final String DEFAULT_REGION = cfg.getString("default-region");
  private static final List<String> KNOWN_ENVIRONMENTS = cfg.getStringList("known-environments");

  private final ZkEnv env;
  private final ZkRegion region;
  private final String rootNode;
  private final String basePath;
  private final boolean kerberos;

  public static ZkConfig parse(String string, boolean kerberos) {
    int rootStart = string.indexOf("/");
    if (rootStart == -1) {
      throwInvalidPath(string);
    }

    String envAndRegion = string.substring(0, rootStart);
    String rootAndBase = string.substring(rootStart + 1);

    if (!envAndRegion.endsWith("::")) {
      throwInvalidPath(string);
    }
    envAndRegion = envAndRegion.substring(0, envAndRegion.length() - 2);
    List<String> envAndRegionSplit = Splitter.on(".").splitToList(envAndRegion);
    if (envAndRegionSplit.size() > 2) {
      throwInvalidPath(string);
    }

    String envStr = envAndRegionSplit.get(0);
    if (!KNOWN_ENVIRONMENTS.contains(envStr)) {
      throw new IllegalArgumentException("Unknown zookeeper environment: " + envStr);
    }
    ZkEnv env = ZkEnv.valueOf(envStr.toUpperCase());

    String region = null;
    if (envAndRegionSplit.size() == 2) {
      region = envAndRegionSplit.get(1);
    } else {
      region = DEFAULT_REGION;
    }

    int baseStart = rootAndBase.indexOf("/");
    if (baseStart == -1) {
      throwInvalidPath(string);
    }

    return new ZkConfig(
        env,
        region,
        string.substring(rootStart, rootStart + baseStart + 1),
        rootAndBase.substring(baseStart),
        kerberos);
  }

  private static void throwInvalidPath(String string) {
    throw new IllegalArgumentException("Invalid zookeeper path: " + string);
  }

  public ZkConfig(ZkEnv env, String region, String rootNode, String basePath, boolean kerberos) {
    this.env = env;
    this.region = ZkRegion.valueOf(region.toUpperCase());
    this.rootNode = rootNode;
    this.basePath = basePath;
    this.kerberos = kerberos;
  }

  public ZkEnv getEnv() {
    return env;
  }

  public ZkRegion getRegion() {
    return region;
  }

  public String getRootNode() {
    return rootNode;
  }

  public String getBasePath() {
    return basePath;
  }

  public boolean isKerberos() {
    return kerberos;
  }

  public String paths(String... paths) {
    if (paths.length == 0) {
      return basePath;
    }

    // not terribly efficient
    String path = basePath;
    for (String node : paths) {
      path = ZKPaths.makePath(path, node);
    }

    return path;
  }
}
*/