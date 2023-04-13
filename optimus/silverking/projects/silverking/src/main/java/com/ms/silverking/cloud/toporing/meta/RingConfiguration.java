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

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.meta.CloudConfiguration;
import com.ms.silverking.cloud.meta.Utils;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Configuration settings required to build a ring.
 * Unlike CloudConfiguration, a RingConfiguration is named
 * and stored in zk.
 */
public class RingConfiguration implements VersionedDefinition {
  private final CloudConfiguration cloudConfig;
  private final String weightSpecsName;
  private final String ringParentName;
  private final String storagePolicyGroupName;
  private final String storagePolicyName;
  private final Set<String> hostGroups;
  private final long version;

  //public static final char    delimiter = CloudConfiguration.delimiter;

  public static final RingConfiguration emptyTemplate = new RingConfiguration(CloudConfiguration.emptyTemplate, null,
      null, null, null, null, VersionedDefinition.NO_VERSION);
  public static final Set<String> optionalFields;

  static {
    ImmutableSet.Builder<String> builder;

    builder = ImmutableSet.builder();
    builder.addAll(Utils.optionalVersionFieldSet);
    builder.add("hostGroups");
    optionalFields = builder.build();
    ObjectDefParser2.addParser(emptyTemplate, FieldsRequirement.REQUIRE_ALL_NONOPTIONAL_FIELDS, optionalFields);
  }

  public RingConfiguration(CloudConfiguration cloudConfig, String weightSpecsName, String ringParentName,
      String storagePolicyGroupName, String storagePolicyName, Set<String> hostGroups, long version) {
    this.cloudConfig = cloudConfig;
    this.weightSpecsName = weightSpecsName;
    this.ringParentName = ringParentName;
    this.storagePolicyGroupName = storagePolicyGroupName;
    this.storagePolicyName = storagePolicyName;
    this.hostGroups = hostGroups;
    this.version = version;
    //this.mapName = topologyName + delimiter + weightSpecsName
    //       + delimiter + exclusionListName + delimiter + replicationFactor;
  }

  public RingConfiguration(CloudConfiguration cloudConfig, String weightSpecsName, String ringParentName,
      String storagePolicyGroupName, String storagePolicyName, Set<String> hostGroups) {
    this(cloudConfig, weightSpecsName, ringParentName, storagePolicyGroupName, storagePolicyName, hostGroups,
        VersionedDefinition.NO_VERSION);
  }

  public RingConfiguration version(long version) {
    return new RingConfiguration(cloudConfig, weightSpecsName, ringParentName, storagePolicyGroupName,
        storagePolicyName, hostGroups, version);
  }

  public CloudConfiguration getCloudConfiguration() {
    return cloudConfig;
  }

  public String getWeightSpecsName() {
    return weightSpecsName;
  }

  public String getRingParentName() {
    return ringParentName;
  }

  public String getStoragePolicyGroupName() {
    return storagePolicyGroupName;
  }

  public String getStoragePolicyName() {
    return storagePolicyName;
  }

  public Set<String> getHostGroups() {
    return hostGroups;
  }

  @Override
  public long getVersion() {
    return version;
  }

  /*
  @Override
  public String toString() {
      return cloudConfig.toString() +delimiter+ weightSpecsName
              +delimiter+ ringParentName;
  }

  public static RingConfiguration parse(String def, long version) {
      String[]    tokens;

      System.out.println(def);
      tokens = def.split(""+ delimiter);
      return new RingConfiguration(new CloudConfiguration(tokens[0], tokens[1], tokens[2], tokens[3], tokens[4]),
                                   tokens[5], tokens[6], tokens[7], version);
  }
  */
  public static RingConfiguration parse(String def, long version) {
    RingConfiguration instance;

    //instance = (RingConfiguration)new ObjectDefParser(emptyTemplate, FieldsRequirement
    // .REQUIRE_ALL_NONOPTIONAL_FIELDS,
    //        Utils.optionalVersionFieldSet).parse(def);
    instance = ObjectDefParser2.parse(RingConfiguration.class, def);
    return instance.version(version);
  }

  @Override
  public String toString() {
    //return new ObjectDefParser(this).objectToString(this);
    return ObjectDefParser2.objectToString(this);
  }
}
