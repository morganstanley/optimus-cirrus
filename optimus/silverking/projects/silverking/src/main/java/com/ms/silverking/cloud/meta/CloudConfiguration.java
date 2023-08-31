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

import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/** Groups cloud configuration settings. Neither named nor stored in zk. */
public class CloudConfiguration {
  private final String topologyName;
  private final String exclusionSpecsName;
  private final String hostGroupTableName;

  public static final CloudConfiguration emptyTemplate = new CloudConfiguration(null, null, null);

  static {
    ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.REQUIRE_ALL_FIELDS);
  }

  public CloudConfiguration(
      String topologyName, String exclusionSpecsName, String hostGroupTableName) {
    this.topologyName = topologyName;
    this.exclusionSpecsName = exclusionSpecsName;
    this.hostGroupTableName = hostGroupTableName;
  }

  public CloudConfiguration topologyName(String topologyName) {
    return new CloudConfiguration(topologyName, exclusionSpecsName, hostGroupTableName);
  }

  public CloudConfiguration exclusionSpecsName(String exclusionSpecsName) {
    return new CloudConfiguration(topologyName, exclusionSpecsName, hostGroupTableName);
  }

  public CloudConfiguration hostGroupTableName(String hostGroupTableName) {
    return new CloudConfiguration(topologyName, exclusionSpecsName, hostGroupTableName);
  }

  public String getTopologyName() {
    return topologyName;
  }

  public String getExclusionSpecsName() {
    return exclusionSpecsName;
  }

  public String getHostGroupTableName() {
    return hostGroupTableName;
  }

  public static CloudConfiguration parse(String def) {
    return ObjectDefParser2.parse(CloudConfiguration.class, def);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}
