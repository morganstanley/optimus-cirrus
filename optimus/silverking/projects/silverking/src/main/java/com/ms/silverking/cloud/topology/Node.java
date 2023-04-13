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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.ms.silverking.text.StringUtil;

/**
 * Represents a Node in a Topology.
 */
public abstract class Node {
  private final NodeClass nodeClass;
  private final List<Node> children;
  private final int hashCode;

  public Node(NodeClass nodeClass, int hashCode, List<Node> children) {
    this.nodeClass = nodeClass;
    this.children = children;
    this.hashCode = hashCode;
  }

  public NodeClass getNodeClass() {
    return nodeClass;
  }

  public List<Node> getChildren() {
    return children;
  }

  public List<Node> getAllDescendants() {
    return getAllDescendants(null);
  }

  public List<Node> getAllDescendants(NodeClass nodeClass) {
    ImmutableList.Builder<Node> descendants;

    descendants = ImmutableList.builder();
    getAllDescendants(descendants, nodeClass);
    return descendants.build();
  }

  void getAllDescendants(ImmutableList.Builder<Node> descendants, NodeClass nodeClass) {
    if (nodeClass == null || childNodeClassMatches(nodeClass)) {
      descendants.addAll(children);
    } else {
      for (Node child : children) {
        child.getAllDescendants(descendants, nodeClass);
      }
    }
  }

  public boolean childNodeClassMatches(NodeClass nodeClass) {
    NodeClass childNodeClass;

    childNodeClass = getChildNodeClass();
    return childNodeClass != null && childNodeClass == nodeClass;
  }

  public NodeClass getChildNodeClass() {
    if (children.size() > 0) {
      return children.get(0).getNodeClass();
    } else {
      return null;
    }
  }

  public abstract String getIDString();

  public String getClassAndIDString() {
    return nodeClass.getName() + ":" + getIDString();
  }

  public Node getChildByID(String id) {
    for (Node child : children) {
      if (child.getIDString().equals(id)) {
        return child;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else {
      Node oNode;

      oNode = (Node) other;
      if (this.hashCode != oNode.hashCode) {
        return false;
      } else {
        return this.getIDString().equals(oNode.getIDString());
      }
    }
  }

  void buildString(StringBuilder sb, int level) {
    String indentation;

    indentation = StringUtil.replicate('\t', level);
    sb.append(indentation);
    sb.append(nodeClass);
    sb.append(':');
    sb.append(getIDString());
    if (children != null && children.size() > 0) {
      sb.append(' ');
      sb.append('{');
      sb.append('\n');
      for (Node child : children) {
        child.buildString(sb, level + 1);
      }
      sb.append(indentation);
      sb.append('}');
    }
    sb.append('\n');
  }

  public String toStructuredString() {
    StringBuilder sb;

    sb = new StringBuilder();
    buildString(sb, 0);
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public String toString() {
    return nodeClass.toString() + ":" + getIDString() + ":" + children.size();
  }

  public void toString(StringBuilder sb, int depth) {
    sb.append(StringUtil.replicate(' ', depth));
    sb.append(getClassAndIDString());
    sb.append('\n');
    for (Node child : children) {
      child.toString(sb, depth + 1);
    }
  }

  public boolean hasChildren() {
    return children.size() > 0;
  }

  public static Set<String> nodeSetToNodeIDSet(Set<Node> usedNodes) {
    Set<String> nodeIDSet;

    nodeIDSet = new HashSet<>();
    for (Node node : usedNodes) {
      nodeIDSet.add(node.getIDString());
    }
    return nodeIDSet;
  }
}
