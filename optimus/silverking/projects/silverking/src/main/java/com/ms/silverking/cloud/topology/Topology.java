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
package com.ms.silverking.cloud.topology;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.meta.VersionedDefinition;
import com.ms.silverking.collection.HashedSetMap;

/**
 * A hierarchical representation of a relationship between entities such as
 * servers, racks, datacenters, etc.
 */
public class Topology implements VersionedDefinition {
  private final String name;
  private final Node root;
  private final Map<String, NodeClass> idToClassMap;
  private final Set<NodeClass> nodeClasses;
  private final HashedSetMap<NodeClass, Node> nodesByClass;
  private final long version;

  public static Topology fromRoot(String id, Node root) {
    Map<String, NodeClass> nodeClasses;

    nodeClasses = new HashMap<>();
    return new Topology(id, root, nodeClasses);
  }

  public static Topology fromRoot(String id, Node root, long version) {
    Map<String, NodeClass> idToClassMap;

    idToClassMap = new HashMap<>();
    return new Topology(id, root, idToClassMap, version);
  }

  public static void computeNodeClasses(Node node, Map<String, NodeClass> nodeClasses) {
    //nodeClasses.put(node.getNodeClass(), )
    for (Node child : node.getChildren()) {
      computeNodeClasses(child, nodeClasses);
    }
  }

  public Topology(String id, Node root, Map<String, NodeClass> idToClassMap, long version) {
    this.name = id;
    this.root = root;
    this.idToClassMap = idToClassMap;
    this.version = version;
    nodeClasses = ImmutableSet.copyOf(idToClassMap.values());
    nodesByClass = new HashedSetMap<>();
    computeNodesByClass(nodesByClass, root);
  }

  private static void computeNodesByClass(HashedSetMap<NodeClass, Node> nodesByClass, Node parent) {
    nodesByClass.addValue(parent.getNodeClass(), parent);
    for (Node child : parent.getChildren()) {
      computeNodesByClass(nodesByClass, child);
    }
  }

  public Topology(String id, Node root, Map<String, NodeClass> idToClassMap) {
    this(id, root, idToClassMap, VersionedDefinition.NO_VERSION);
  }

  public String getName() {
    return name;
  }

  public Node getRoot() {
    return root;
  }

  @Override
  public long getVersion() {
    return version;
  }

  public Node getNodeByID(String id) {
    return getNodeByID(root, id);
  }

  private Node getNodeByID(Node curNode, String id) {
    if (curNode.getIDString().equals(id)) {
      return curNode;
    } else {
      for (Node child : curNode.getChildren()) {
        Node foundNode;

        foundNode = getNodeByID(child, id);
        if (foundNode != null) {
          return foundNode;
        }
      }
      return null;
    }
  }

  public Node getAncestor(String id, NodeClass nodeClass) {
    List<Node> pathToNode;

    pathToNode = pathTo(id);
    if (pathToNode != null) {
      for (int i = pathToNode.size() - 1; i >= 0; i--) {
        Node node;

        node = pathToNode.get(i);
        if (node.getNodeClass().equals(nodeClass)) {
          return node;
        }
      }
      return null;
    } else {
      return null;
    }
  }

  public Set<NodeClass> getNodeClasses() {
    return nodeClasses;
  }

  public Set<Node> getNodesByClass(NodeClass nodeClass) {
    return nodesByClass.getSet(nodeClass);
  }

  public List<Node> pathTo(String id) {
    return pathTo(root, id);
  }

  public List<Node> pathTo(Node curNode, String id) {
    if (curNode.getIDString().equals(id)) {
      return ImmutableList.of(curNode);
    } else {
      for (Node child : curNode.getChildren()) {
        List<Node> foundPath;

        foundPath = pathTo(child, id);
        if (foundPath != null) {
          return new ImmutableList.Builder<Node>().add(curNode).addAll(foundPath).build();
        }
      }
      return null;
    }
  }

  private int deepestCommonParentIndex(List<Node> p1, List<Node> p2) {
    int index;

    index = 0;
    while (index < p1.size() && index < p2.size() && p1.get(index) == p2.get(index)) {
      index++;
    }
    return index - 1;
  }

  /**
   * Compute distance as 2^(length of shortest leg to the common parent)
   *
   * @param id1
   * @param id2
   * @return
   */
  public int getDistanceByID(String id1, String id2) {
    List<Node> p1;
    List<Node> p2;
    int commonParentIndex;
    int d1;
    int d2;

    p1 = pathTo(id1);
    if (p1 == null) {
      return Integer.MAX_VALUE;
    }
    p2 = pathTo(id2);
    if (p2 == null) {
      return Integer.MAX_VALUE;
    }
    commonParentIndex = deepestCommonParentIndex(p1, p2);
    d1 = p1.size() - commonParentIndex - 1;
    d2 = p2.size() - commonParentIndex - 1;
    return 1 << Math.min(d1, d2);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    root.toString(sb, 0);
    return sb.toString();
  }

  public String toStructuredString() {
    return root.toStructuredString();
  }
}
