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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;

public class StoragePolicy {
  private final NodeClassAndName nodeClassAndName;
  private final SubPolicy primaryPolicy;
  private final SubPolicy secondaryPolicy;
  private final SubPolicy[] subPolicies;

  public StoragePolicy(
      NodeClassAndName nodeClassAndName, SubPolicy primaryPolicy, SubPolicy secondaryPolicy) {
    this.nodeClassAndName = nodeClassAndName;
    this.primaryPolicy = primaryPolicy;
    this.secondaryPolicy = secondaryPolicy;
    if (secondaryPolicy != null) {
      subPolicies = new SubPolicy[2];
      subPolicies[0] = primaryPolicy;
      subPolicies[1] = secondaryPolicy;
    } else {
      subPolicies = new SubPolicy[1];
      subPolicies[0] = primaryPolicy;
    }
  }

  public String getName() {
    return nodeClassAndName.getName();
  }

  public NodeClass getNodeClass() {
    return nodeClassAndName.getNodeClass();
  }

  public int getReplicas(OwnerQueryMode oqm) {
    return (oqm.includePrimary() ? primaryPolicy.getNumReplicas() : 0)
        + (oqm.includeSecondary() ? secondaryPolicy.getNumReplicas() : 0);
  }

  public SubPolicy getPolicy(OwnerQueryMode oqm) {
    switch (oqm) {
      case Primary:
        return primaryPolicy;
      case Secondary:
        return secondaryPolicy;
      case All:
        throw new RuntimeException("All not supported for getPolicy");
      default:
        throw new RuntimeException("panic");
    }
  }

  public SubPolicy[] getSubPolicies() {
    return subPolicies;
  }

  public List<String> getSubPolicyNamesForNodeClass(NodeClass nodeClass, Node child) {
    ImmutableList.Builder<String> builder;

    builder = new ImmutableList.Builder<>();
    for (SubPolicy subPolicy : subPolicies) {
      builder.addAll(subPolicy.getSubPolicyNamesForNodeClass(nodeClass, child));
    }
    return builder.build();
  }

  @Override
  public String toString() {
    return nodeClassAndName + ":" + primaryPolicy + ":" + secondaryPolicy;
  }

  public void toFormattedString(StringBuffer sb) {
    sb.append(nodeClassAndName + " {\n");
    for (SubPolicy subPolicy : subPolicies) {
      subPolicy.toFormattedString(sb);
    }
    sb.append("}\n");
    // StringUtil.replicate('\t', depth)
  }
}
