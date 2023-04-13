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
package com.ms.silverking.cloud.toporing;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import com.ms.silverking.cloud.common.OwnerQueryMode;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.cloud.ring.Ring;
import com.ms.silverking.cloud.ring.RingRegion;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.topology.NodeClass;
import com.ms.silverking.cloud.toporing.meta.WeightSpecifications;
import com.ms.silverking.util.Mutability;

/**
 * A ring with regions owned by RingEntries.
 */
public interface TopologyRing extends Ring<Long, RingEntry>, VersionedDefinition {
  public TopologyRing clone(Mutability mutability);

  public TopologyRing cloneEmpty(Mutability mutability);

  public int numMemberNodes(OwnerQueryMode oqm);

  public Collection<Node> getMemberNodes(OwnerQueryMode oqm);

  public BigDecimal getTotalWeight();

  public Collection<RingRegion> getNodeRegions(Node node, OwnerQueryMode oqm);

  public Collection<RingRegion> getNodeRegionsFiltered(Node node, Node filterNode, OwnerQueryMode oqm);

  public List<RingRegion> getNodeRegionsSorted(Node node, Comparator<RingRegion> comparator, Node filterNode,
      OwnerQueryMode oqm);

  public TopologyRing simplify();

  public void releaseToNode(RingRegion region, Node oldOwner, Node newOwner);

  public BigDecimal getOwnedFraction(Node owner, OwnerQueryMode oqm);

  public long getOwnedRingspace(Node owner, OwnerQueryMode oqm);

  public Node getNodeByID(String nodeID);

  public boolean containsNode(Node node, OwnerQueryMode oqm);

  public boolean pointOwnedByNode(long point, Node node, OwnerQueryMode oqm);

  public double getWeight(String nodeID);

  public void freeze(WeightSpecifications weightSpecs);

  public String getStoragePolicyName();

  //public String getPrimaryStoragePolicy(String nodeID);
  //public String getSecondaryStoragePolicy(String nodeID);
  public NodeClass getNodeClass();

  public void removeEntry(RingEntry oEntry);
}
