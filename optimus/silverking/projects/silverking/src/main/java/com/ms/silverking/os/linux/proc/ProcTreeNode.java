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
package com.ms.silverking.os.linux.proc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.text.StringUtil;

public class ProcTreeNode {
  private final int pid;
  private final List<ProcTreeNode> children;

  public ProcTreeNode(int pid) {
    this.pid = pid;
    children = new ArrayList<>();
  }

  public void addChild(ProcTreeNode child) {
    children.add(child);
  }

  public int getPID() {
    return pid;
  }

  public List<ProcTreeNode> getChildren() {
    return children;
  }

  public int getDepth() {
    if (children.size() == 0) {
      return 0;
    } else {
      int maxChildDepth;

      maxChildDepth = 0;
      for (ProcTreeNode child : children) {
        maxChildDepth = Math.max(maxChildDepth, child.getDepth());
      }
      return maxChildDepth + 1;
    }
  }

  public void toString(StringBuilder sb, int depth) {
    sb.append(StringUtil.replicate('\t', depth));
    sb.append(pid);
    sb.append('\n');
    for (ProcTreeNode child : children) {
      child.toString(sb, depth + 1);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    toString(sb, 0);
    return sb.toString();
  }

  public static ProcTreeNode buildTree(List<ProcessStat> statList, int rootPID) {
    return buildForest(statList).get(rootPID);
  }

  public static Map<Integer, ProcTreeNode> buildForest(List<ProcessStat> statList) {
    Map<Integer, ProcTreeNode> treeNodes;

    treeNodes = new HashMap<>();
    for (ProcessStat stat : statList) {
      ProcTreeNode node;

      node = treeNodes.get(stat.pid);
      if (node != null) {
        // Theoretically possible, but highly unlikely.
        // Don't build a tree in this case.
        throw new RuntimeException("Unexpected duplicate pid");
      } else {
        node = new ProcTreeNode(stat.pid);
        treeNodes.put(stat.pid, node);
      }
    }
    for (ProcessStat stat : statList) {
      ProcTreeNode node;

      node = treeNodes.get(stat.pid);
      if (node == null) {
        throw new RuntimeException("panic");
      } else {
        ProcTreeNode parent;

        parent = treeNodes.get(stat.ppid);
        if (parent == null) {
          //System.out.println("Couldn't find parent "+ stat.ppid +" for: "+ stat.pid);
        } else {
          parent.addChild(node);
        }
      }
    }
    return treeNodes;
  }

  public static void main(String[] args) {
    try {
      if (args.length != 1) {
        System.out.println("args: <rootPID>");
      } else {
        int rootPID;
        ProcTreeNode root;

        rootPID = Integer.parseInt(args[0]);
        root = buildTree(new ProcReader().activeProcessStats(), rootPID);
        System.out.println(root);
        System.out.println("Depth: " + root.getDepth());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
