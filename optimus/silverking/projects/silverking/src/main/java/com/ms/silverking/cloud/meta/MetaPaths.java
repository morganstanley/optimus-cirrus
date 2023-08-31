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
package com.ms.silverking.cloud.meta;

import com.google.common.collect.ImmutableList;

/** Global and configuration specific cloud meta paths. */
public class MetaPaths extends MetaPathsBase {
  // instance paths
  private final String topologyPath;
  private final String exclusionsPath;
  private final String hostGroupPath;

  // cloud global base directories
  public static final String topologiesBase = cloudGlobalBase + "/topologies";
  public static final String exclusionsBase = cloudGlobalBase + "/exclusions";
  public static final String hostGroupBase = cloudGlobalBase + "/hostGroupTables";

  public MetaPaths(CloudConfiguration cloudConfig) {
    ImmutableList.Builder<String> listBuilder;

    listBuilder = ImmutableList.builder();
    if (cloudConfig.getTopologyName() != null) {
      topologyPath = topologiesBase + "/" + cloudConfig.getTopologyName();
      listBuilder.add(topologyPath);
    } else {
      topologyPath = null;
    }
    if (cloudConfig.getExclusionSpecsName() != null) {
      exclusionsPath = exclusionsBase + "/" + cloudConfig.getExclusionSpecsName();
      listBuilder.add(exclusionsPath);
    } else {
      exclusionsPath = null;
    }
    if (cloudConfig.getHostGroupTableName() != null) {
      hostGroupPath = hostGroupBase + "/" + cloudConfig.getHostGroupTableName();
      listBuilder.add(hostGroupPath);
    } else {
      hostGroupPath = null;
    }
    pathList = listBuilder.build();
  }

  public String getTopologyPath() {
    return topologyPath;
  }

  public String getExclusionsPath() {
    return exclusionsPath;
  }

  public String getHostGroupPath() {
    return hostGroupPath;
  }
}
