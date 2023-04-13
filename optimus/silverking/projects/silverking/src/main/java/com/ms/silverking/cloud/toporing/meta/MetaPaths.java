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
package com.ms.silverking.cloud.toporing.meta;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient;

public class MetaPaths extends com.ms.silverking.cloud.meta.MetaPaths {
  // dht global base directories
  public static final String ringsGlobalBase = cloudGlobalBase + "/rings";
  public static final String ringsServerGlobalBase = serverConfigBase + "/rings";
  public static final String weightsBase = ringsGlobalBase + "/weights";
  public static final String weightsServerBase = ringsServerGlobalBase + "/weights";
  //public static final String  replicationBase = ringsGlobalBase +"/replication";
  public static final String instancesBase = ringsGlobalBase + "/instances";
  public static final String storagePolicyBase = ringsGlobalBase + "/storagePolicyGroups";
  private static final String configElement = "config";
  // configuration instance paths
  private final String weightsPath;
  private final String weightsServerPath;
  //private final String  replicationPath;
  private final String instancePath;
  private final String configPath;
  private final String storagePolicyGroupPath;

  public MetaPaths(NamedRingConfiguration namedRingConfig) {
    super(namedRingConfig.getRingConfiguration().getCloudConfiguration());

    RingConfiguration ringConfig;
    ImmutableList.Builder<String> listBuilder;

    ringConfig = namedRingConfig.getRingConfiguration();
    listBuilder = ImmutableList.builder();
    if (ringConfig.getWeightSpecsName() != null) {
      weightsPath = weightsBase + "/" + ringConfig.getWeightSpecsName();
      weightsServerPath = weightsServerBase + "/" + ringConfig.getWeightSpecsName();
      listBuilder.add(weightsPath);
      listBuilder.add(weightsServerPath);
    } else {
      weightsPath = null;
      weightsServerPath = null;
    }
        /*
        if (ringConfig.getReplicationSpecsName() != null) {
            replicationPath = replicationBase +"/"+ ringConfig.getReplicationSpecsName();
            listBuilder.add(replicationPath);
        } else {
            replicationPath = null;
        }
        */

    if (namedRingConfig.getRingConfiguration().getStoragePolicyGroupName() != null) {
      storagePolicyGroupPath = storagePolicyBase +
                               "/" +
                               namedRingConfig.getRingConfiguration().getStoragePolicyGroupName();
      listBuilder.add(storagePolicyGroupPath);
    } else {
      storagePolicyGroupPath = null;
    }

    if (namedRingConfig.getRingName() != null) {
      instancePath = instancesBase + "/" + namedRingConfig.getRingName();
      configPath = instancePath + "/" + configElement;
      //configPath = instancePath +"/config/" + ringConfig.getVersion();
      listBuilder.add(configPath);
    } else {
      instancePath = null;
      configPath = null;
    }
    pathList = listBuilder.build();
  }

  public static String getRingConfigPath(String ringName) {
    return instancesBase + "/" + ringName + "/" + configElement;
  }

  public static String getRingConfigInstancePath(String ringName, long configVersion) {
    return getConfigInstancePath(getRingConfigPath(ringName), configVersion);
  }

  private static String getConfigInstancePath(String configPath, long configVersion) {
    return SilverKingZooKeeperClient.padVersionPath(configPath, configVersion) + "/instance";
  }

  public String getWeightsPath() {
    return weightsPath;
  }

  public String getWeightsServerPath() {
    return weightsServerPath;
  }

  public String getConfigPath() {
    return configPath;
  }

  public String getConfigInstancePath(long configVersion) {
    return getConfigInstancePath(configPath, configVersion);
  }

  public String getRingInstancePath(long configVersion, long instanceVersion) {
    return getRingInstancePath(getConfigInstancePath(configVersion), instanceVersion);
  }

  public String getRingInstancePath(String configInstancePath, long instanceVersion) {
    return SilverKingZooKeeperClient.padVersionPath(configInstancePath, instanceVersion);
  }

  public String getStoragePolicyGroupPath() {
    return storagePolicyGroupPath;
  }
}
