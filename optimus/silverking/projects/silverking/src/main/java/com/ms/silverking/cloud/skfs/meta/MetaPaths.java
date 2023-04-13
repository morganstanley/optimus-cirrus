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
package com.ms.silverking.cloud.skfs.meta;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.meta.MetaPathsBase;

public class MetaPaths extends MetaPathsBase {
  // configuration instance paths
  private final String configPath;

  // dht global base directories
  public static final String skfsGlobalBase = cloudGlobalBase + "/skfs";
  public static final String configsBase = skfsGlobalBase + "/configs";

  public MetaPaths(String skfsConfigName) {
    ImmutableList.Builder<String> listBuilder;
    listBuilder = ImmutableList.builder();
    this.configPath = getConfigPath(skfsConfigName);
    listBuilder.add(this.configPath);
    pathList = listBuilder.build();
  }

  public static String getConfigPath(String skfsConfigName) {
    return configsBase + "/" + skfsConfigName;
  }

  public String getConfigPath() {
    return configPath;
  }
}
