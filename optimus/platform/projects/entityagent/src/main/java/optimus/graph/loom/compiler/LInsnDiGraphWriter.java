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
package optimus.graph.loom.compiler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

/** Debug utility to write dot files representing a DAG of blocks */
public class LInsnDiGraphWriter {
  StringBuilder sb = new StringBuilder();
  HashMap<Op, String> opNames = new HashMap<>();
  HashSet<Op> visitedOp = new HashSet<>();
  // In order not to introduce nodes into clusters too early some edges
  // (those that cross clusters) are delayed
  ArrayList<String> delayedEdges = new ArrayList<>();
  ArrayList<String> dependencyEdges = new ArrayList<>();
  ArrayList<String> delayedBlockEdges = new ArrayList<>();

  // useful when debugging functions that have too many blocks to render all their instructions.
  // Change the implementation to expand only the block ids you are interested in
  // e.g.: return block.blockID <= 22;
  private boolean keepExpanded(Block block) {
    return true;
  }

  public static String toString(Block block, String methodId, boolean minimal) {
    var writer = new LInsnDiGraphWriter();
    writer.writeHeader(methodId);
    writer.visitBlock(block, minimal);
    writer.writeDelayedEdges();
    writer.writeFooter();
    return writer.sb.toString();
  }

  private void writeDelayedEdges() {
    if (!delayedBlockEdges.isEmpty()) {
      appendln("{");
      appendln("edge[color=dimgrey penwidth=0.5 arrowsize=0.5 fontsize=11]");
      for (var edge : delayedBlockEdges) {
        sb.append(edge);
      }
      appendln("}");
    }

    for (var edge : delayedEdges) {
      sb.append(edge);
    }

    if (!dependencyEdges.isEmpty()) {
      appendln("{");
      appendln("edge[color=red penwidth=0.5 arrowsize=0.5]");
      for (var edge : dependencyEdges) {
        sb.append(edge);
      }
      appendln("}");
    }
  }

  public void visitBlock(Block block, boolean minimal) {
    if (visitedOp.contains(block)) return;
    visitedOp.add(block);

    writerSubgraphHeader(block);
    var minimizeBlock = minimal || !keepExpanded(block);
    if (minimizeBlock) {
      writeNode(block.lastOp);
    } else {
      var ops = block.availOps.isEmpty() ? Collections.singleton(block.lastOp) : block.availOps;
      visitNodes(ops, block.blockID);
      visitNodes(block.readsVars, block.blockID);
      // we know that Phi is always an Op! So the cast is safe
      //noinspection unchecked
      visitNodes((ArrayList<Op>) (Object) block.phiOps, block.blockID);
    }
    writeFooter();
    var innerBlocksCount = block.targets == null ? 0 : block.targets.length;
    for (int i = 0; i < innerBlocksCount; i++) {
      var label = "";
      if (innerBlocksCount == 2) {
        label = i == 0 ? "false" : "true";
      }
      // for now not putting any label when more than 2 inner blocks, we can revisit this later!
      var dependentBlock = block.targets[i];
      writeBlockEdge(block, dependentBlock, label, minimizeBlock);
      visitBlock(dependentBlock, minimal);
    }
  }

  private void visitNodes(Collection<Op> ops, int blockID) {
    for (var op : ops) {
      if (op.blockOwner.blockID == blockID && !visitedOp.contains(op)) {
        visitedOp.add(op);
        var targetName = writeNode(op);
        writeInputEdges(op, targetName);
        writeDependencyEdges(op, targetName);
        visitNodes(op.consumers, blockID);
      }
    }
  }

  private void writeInputEdges(Op op, String targetName) {
    for (var srcOp : op.inputs) {
      var srcName = idOf(srcOp);
      var edgeString = srcName + " -> " + targetName + "\n";
      if (srcOp.blockOwner == op.blockOwner) sb.append(edgeString);
      else delayedEdges.add(edgeString);
    }
  }

  private void writeDependencyEdges(Op op, String srcName) {
    for (var depOp : op.dependencies) {
      var dopName = idOf(depOp);
      var edgeString = srcName + " -> " + dopName + "\n";
      dependencyEdges.add(edgeString);
    }
  }

  private void writeBlockEdge(Block from, Block to, String label, boolean minimal) {
    var fromName = idOf(from.lastOp);
    var toName = idOf(to.availOps.isEmpty() || minimal ? to.lastOp : to.availOps.peek());
    delayedBlockEdges.add(
        fromName
            + " -> "
            + toName
            + " [lhead=cluster"
            + to.blockOwner
            + " ltail=cluster"
            + from.blockOwner
            + " label=\""
            + label
            + "\"]\n");
  }

  private void appendln(String str) {
    sb.append(str).append("\n");
  }

  private void writeHeader(String title) {
    appendln("digraph {");

    // temp workaround for broken strato dot integration -- Ian Drewett is working on the fix
    appendln("fixit [style=invis,URL=\"\"]");

    appendln("labelloc=\"t\"");
    appendln("label=\"" + title + "\"");
    appendln("graph[ordering=out,compound=true]");
    appendln("node [fontsize=12 shape=plain style=solid fontname=Calibri color=skyblue]");
    appendln("edge [fontsize=15]");
  }

  private void writerSubgraphHeader(Block block) {
    appendln("subgraph cluster" + block.blockOwner + " {");
    appendln("label=\"" + block.blockOwner + "\n(" + block.label + ")\"");
    appendln("color=dimgrey penwidth=0.3");
  }

  private void writeFooter() {
    appendln("}");
  }

  private String idOf(Op insn) {
    var i = opNames.get(insn);
    if (i != null) return i;
    else {
      var name = "n" + opNames.size();
      opNames.put(insn, name);
      return name;
    }
  }

  private String writeNode(Op insn) {
    var name = idOf(insn);
    sb.append(name);
    sb.append("[xlabel=").append(insn.index);
    if (insn.isNode()) sb.append(" color=blue");
    sb.append(" label=\"").append(insn.toString().replace("[", "\\["));
    appendln("\"]");
    return name;
  }

  private static final boolean shortCalls = true;

  public static String asString(AbstractInsnNode insn) {
    if (shortCalls && insn.getOpcode() == Opcodes.INVOKEDYNAMIC) {
      return "INVOKEDYNAMIC " + ((InvokeDynamicInsnNode) insn).bsm.getName();
    } else if (shortCalls && insn.getOpcode() == Opcodes.INVOKEVIRTUAL) {
      return "INVOKEVIRTUAL " + ((MethodInsnNode) insn).name + ((MethodInsnNode) insn).desc;
    } else if (shortCalls && insn.getOpcode() == Opcodes.GETSTATIC) {
      return "GETSTATIC " + ((FieldInsnNode) insn).name;
    }
    var printer = new Textifier();
    var tmv = new TraceMethodVisitor(printer);
    insn.accept(tmv);
    var text = ((String) (printer.getText().get(0))).trim().replace("\"", "\\\"");

    // making sure labels match in the debugger
    if (insn instanceof LabelNode) {
      var label = ((LabelNode) insn).getLabel();
      return text.replace("L0", label.toString());
    }
    if (insn instanceof LineNumberNode) {
      var label = (((LineNumberNode) insn).start).getLabel();
      return text.replace("L0", label.toString());
    }
    if (insn instanceof JumpInsnNode) {
      var label = (((JumpInsnNode) insn).label).getLabel();
      return text.replace("L0", label.toString());
    }
    if (insn instanceof TableSwitchInsnNode) {
      var ts = (TableSwitchInsnNode) insn;
      for (int i = 0; i < ts.labels.size(); i++) {
        var label = ts.labels.get(0).getLabel();
        text = text.replace(i + ": L" + i, i + ": " + label.toString());
      }
      var dfltLabel = ts.dflt.getLabel();
      return text.replace("default: L" + ts.labels.size(), "default: " + dfltLabel.toString());
    }

    return text;
  }
}
