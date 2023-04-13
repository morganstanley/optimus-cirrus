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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.ms.silverking.text.StringUtil;

/**
 * Used by parser to store information about a topology entry that is being parsed.
 */
class TopologyEntry {
  private final String def;
  private List<TopologyEntry> children;

  TopologyEntry(String def) {
    this.def = def;
  }

  String getDef() {
    return def;
  }

  List<TopologyEntry> getChildren() {
    return children;
  }

  TopologyEntry getLastChild() {
    if (children == null) {
      return null;
    } else {
      return children.get(children.size() - 1);
    }
  }

  void addChild(TopologyEntry entry) {
    if (children == null) {
      children = new LinkedList<>();
    }
    children.add(entry);
  }

  void buildString(StringBuilder sb, int level) {
    sb.append(StringUtil.replicate('\t', level));
    sb.append(def);
    sb.append('\n');
    if (children != null) {
      for (TopologyEntry child : children) {
        child.buildString(sb, level + 1);
      }
    }
  }

  static Node entriesToNodes(TopologyEntry entry) throws TopologyParseException {
    List<TopologyEntry> entryChildren;
    List<Node> children;

    entryChildren = entry.getChildren();
    if (entryChildren != null) {
      children = new ArrayList<>(entryChildren.size());
      for (TopologyEntry entryChild : entryChildren) {
        children.add(entriesToNodes(entryChild));
      }
    } else {
      children = new ArrayList<>(0);
    }
    return GenericNode.create(entry.getDef(), children);
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    buildString(sb, 0);
    return sb.toString();
  }
}
