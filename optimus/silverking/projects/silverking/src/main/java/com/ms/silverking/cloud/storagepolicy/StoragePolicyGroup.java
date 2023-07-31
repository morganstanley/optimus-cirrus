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
package com.ms.silverking.cloud.storagepolicy;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.topology.NodeClass;

/**
 * Group of StoragePolicies as well as particular root node
 * class and name.
 */
public class StoragePolicyGroup implements VersionedDefinition {
  private final String name;
  private final NodeClassAndName rootClassAndName;
  private final long version;
  private final Map<String, StoragePolicy> policies;
  private final ListMultimap<NodeClass, StoragePolicy> classPolicies;

  // TODO (OPTIMUS-0000): deprecate the rootClassAndName

  public static final String label = "StoragePolicyGroup";

  public StoragePolicyGroup(String name, NodeClassAndName rootClassAndName, Map<String, StoragePolicy> policies,
      long version) {
    this.name = name;
    this.rootClassAndName = rootClassAndName;
    this.policies = policies;
    this.version = version;
    classPolicies = ArrayListMultimap.create();
    populateClassPolicies();
  }

  private void populateClassPolicies() {
    for (StoragePolicy policy : policies.values()) {
      classPolicies.put(policy.getNodeClass(), policy);
    }
  }

  public static StoragePolicyGroup create(String name, NodeClassAndName rootClassAndName,
      List<StoragePolicy> policyList, long version) {
    Map<String, StoragePolicy> policyMap;

    policyMap = new HashMap<>();
    for (StoragePolicy policy : policyList) {
      policyMap.put(policy.getName(), policy);
    }
    return new StoragePolicyGroup(name, rootClassAndName, policyMap, version);
  }

  public static StoragePolicyGroup parse(File policyFile, long version) throws PolicyParseException {
    return new PolicyParser().parsePolicyGroup(policyFile, version);
  }

  public static StoragePolicyGroup parse(String def, long version) throws PolicyParseException {
    System.out.println(def);
    return new PolicyParser().parsePolicyGroup(def, version);
  }

  public String getName() {
    return name;
  }

  //public NodeClassAndName getRootClassAndName() {
  //    return rootClassAndName;
  //}

  //public NodeClass getRootNodeClass() {
  //    return rootClassAndName.getNodeClass();
  //}

  //public StoragePolicy getRootPolicy() {
  //    return policies.get(rootClassAndName.getName());
  //}

  public StoragePolicy getPolicy(String policyName) {
    return policies.get(policyName);
  }

  public Collection<StoragePolicy> getPolicies() {
    return policies.values();
  }

  public StoragePolicy getClassPolicy(NodeClass nodeClass) {
    List<StoragePolicy> policyList;

    policyList = classPolicies.get(nodeClass);
    if (policyList.size() == 0) {
      return null;
    }
    if (policyList.size() == 1) {
      return policyList.get(0);
    } else {
      throw new RuntimeException("Multiple policies found for: " + nodeClass);
    }
  }

  @Override
  public long getVersion() {
    return version;
  }

  public String toFormattedString() {
    StringBuffer sb;

    sb = new StringBuffer();
    sb.append(label + ":" + name + " {\n");
    sb.append("\troot\t" + rootClassAndName + "\n");
    sb.append("}\n\n");
    for (StoragePolicy policy : policies.values()) {
      policy.toFormattedString(sb);
      sb.append("\n\n");
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return toFormattedString();
  }
}
