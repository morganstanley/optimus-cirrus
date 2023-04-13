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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.topology.Node;
import com.ms.silverking.cloud.toporing.RingTreeRecipe;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubPolicyMember {
  private final int quantity;
  private final NodeClassAndStoragePolicyName classAndPolicyName;
  private final Set<String> boundIDs;

  private static Logger log = LoggerFactory.getLogger(SubPolicyMember.class);

  private static final Set<String> immutableStringSet = ImmutableSet.of();
  private static final String ALL_TOKEN = "all";
  public static final int ALL = Integer.MAX_VALUE;

  public SubPolicyMember(int quantity, NodeClassAndStoragePolicyName classAndPolicyName, Set<String> boundIDs) {
    this.quantity = quantity;
    this.classAndPolicyName = classAndPolicyName;
    this.boundIDs = boundIDs;
    if (boundIDs != null) {
      if (quantity > boundIDs.size()) {
        throw new RuntimeException("quantity > boundIDs.size()");
      }
    }
  }

  public SubPolicyMember(int quantity, NodeClassAndStoragePolicyName classAndPolicyName) {
    this(quantity, classAndPolicyName, null);
  }

  public int getQuantity() {
    return quantity;
  }

  public Set<String> getBoundIDs() {
    return boundIDs;
  }

  public NodeClassAndStoragePolicyName getNodeClassAndStoragePolicyName() {
    return classAndPolicyName;
  }

  public boolean isBound() {
    return boundIDs != null;
  }

  public List<Node> getNodesList(Node parent, RingTreeRecipe recipe) {
    return ImmutableList.copyOf(getNodes(parent, recipe));
  }

  public Set<Node> getNodes(Node parent, RingTreeRecipe recipe) {
    if (boundIDs != null) {
      return getNodesBound(parent, recipe);
    } else {
      return getNodesUnbound(parent, recipe);
    }
  }

  private Set<Node> getNodesUnbound(Node parent, RingTreeRecipe recipe) {
    Set<Node> filteredChildren;
    Set<Node> nodes;

    //filteredChildren = recipe.exclusionList.filter(parent.getChildren());
    filteredChildren = recipe.nonExcludedChildren(parent.getIDString());
    if (filteredChildren.size() < quantity && quantity != ALL) {
      log.info("parent: {}" , parent);
      log.info("filteredChildren.size(): {}   quantity: {}",  filteredChildren.size() , quantity);
      log.info("recipe: {}", recipe);
      throw new RuntimeException("filteredChildren.size() < quantity");
    }
    nodes = new HashSet<>(filteredChildren.size());
    nodes.addAll(filteredChildren);
    return nodes;
  }

  private Set<Node> getNodesBound(Node parent, RingTreeRecipe recipe) {
    Set<Node> members;

    members = new HashSet<>(boundIDs.size());
    for (String id : boundIDs) {
      Node member;

      member = parent.getChildByID(id);
      if (member == null) {
        throw new RuntimeException("Couldn't find bound member: " + id + " in " + parent);
      }
      if (recipe.exclusionList.contains(id)) {
        throw new RuntimeException("Bound member is excluded: " + id + " in " + parent);
      }
      members.add(member);
    }
    return members;
  }

/*
    public List<String> getNodeIDs(Node parent) {
        List<String>    nodeIDs;
        List<Node>      children;
        
        parent.getChildren();
        nodeIDs = new ArrayList<>();
        for (int i = 0; i < quantity; i++) {
        }
        return nodeIDs;
    }    
  */

  public static SubPolicyMember parse(String def) throws PolicyParseException {
    String[] tokens;
    int quantity;
    String quantityTok;
    String tok1;

    tokens = def.split("\\s+of\\s+");

    quantityTok = tokens[0].trim();
    tok1 = tokens[1].trim();
    try {
      if (quantityTok.equalsIgnoreCase(ALL_TOKEN)) {
        quantity = ALL;
      } else {
        quantity = Integer.parseInt(quantityTok);
      }
    } catch (NumberFormatException nfe) {
      throw new PolicyParseException(nfe);
    }
    if (StringUtil.countOccurrences(tok1, ':') > 2) {
      throw new PolicyParseException("Too many : " + tok1);
    } else if (StringUtil.countOccurrences(tok1, ':') == 2) {
      String tok1A;
      String tok1B;
      int secondColonIndex;

      secondColonIndex = tok1.lastIndexOf(':');
      tok1A = tok1.substring(0, secondColonIndex);
      tok1B = tok1.substring(secondColonIndex + 1);
      return new SubPolicyMember(quantity, NodeClassAndStoragePolicyName.parse(tok1A), parseBoundIDs(tok1B));
    } else {
      return new SubPolicyMember(quantity, NodeClassAndStoragePolicyName.parse(tok1));
    }
  }

  private static Set<String> parseBoundIDs(String def) throws PolicyParseException {
    def = def.trim();
    if (!def.startsWith("{") || !def.endsWith("}")) {
      throw new PolicyParseException("Bound IDs must be contained within {}");
    } else {
      String[] ids;
      HashSet<String> idSet;

      def = def.substring(1, def.length() - 1);
      ids = def.split("\\s+");
      idSet = new HashSet<>();
      for (String id : ids) {
        idSet.add(id.trim());
      }
      return idSet;
    }
  }

  public static List<SubPolicyMember> parseMultiple(String multipleDef) throws PolicyParseException {
    String[] defs;
    List<SubPolicyMember> members;

    defs = multipleDef.split(",");
    members = new ArrayList<>(defs.length);
    for (String def : defs) {
      members.add(parse(def));
    }
    return members;
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    if (quantity != ALL) {
      sb.append(quantity);
    } else {
      sb.append("all");
    }
    sb.append(" of ");
    sb.append(classAndPolicyName);
    if (boundIDs != null && !boundIDs.isEmpty()) {
      sb.append(':');
      sb.append('{');
      for (String boundID : boundIDs) {
        sb.append(boundID);
        sb.append(' ');
      }
      sb.append('}');
    }
    return sb.toString();
  }
}
