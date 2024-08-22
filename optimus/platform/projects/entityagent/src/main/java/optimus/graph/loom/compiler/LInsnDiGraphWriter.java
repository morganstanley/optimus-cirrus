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
import java.util.HashMap;
import java.util.HashSet;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.util.Textifier;
import org.objectweb.asm.util.TraceMethodVisitor;

public class LInsnDiGraphWriter {
  StringBuilder sb = new StringBuilder();
  HashMap<Op, String> opNames = new HashMap<>();
  HashSet<Op> visitedOp = new HashSet<>();
  // In order not to introduce nodes into clusters too early some edges
  // (those that cross clusters) are delayed
  ArrayList<String> delayedEdges = new ArrayList<>();
  ArrayList<String> delayedBlockEdges = new ArrayList<>();

  public static String toString(Block block) {
    var writer = new LInsnDiGraphWriter();
    writer.writeHeader();
    writer.visitBlock(block);
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
  }

  public void visitBlock(Block block) {
    if (visitedOp.contains(block)) return;
    visitedOp.add(block);

    writerSubgraphHeader(block);
    var ops = block.availOps;
    visitNodes(ops, block.blockID);
    writeFooter();
    var innerBlocksCount = block.targets == null ? 0 : block.targets.length;
    for (int i = 0; i < innerBlocksCount; i++) {
      var label = "";
      if (innerBlocksCount == 2) {
        label = i == 0 ? "true" : "false";
      }
      // for now not putting any label when more than 2 inner blocks, we can revisit this later!
      var dependentBlock = block.targets[i];
      writeBlockEdge(block, dependentBlock, label);
      visitBlock(dependentBlock);
    }
  }

  private void visitNodes(Collection<Op> ops, int blockID) {
    for (var op : ops) {
      if (op.blockID == blockID && !visitedOp.contains(op)) {
        visitedOp.add(op);
        var targetName = writeNode(op);
        writeInputEdges(op, targetName);
        // orderInputs(op.inputs);
        visitNodes(op.consumers, blockID);
      }
    }
  }

  private void writeInputEdges(Op op, String targetName) {
    for (var srcOp : op.inputs) {
      var srcName = idOf(srcOp);
      var edgeString = srcName + " -> " + targetName + "\n";
      if (srcOp.blockID == op.blockID) sb.append(edgeString);
      else delayedEdges.add(edgeString);
    }
  }

  private void writeBlockEdge(Block from, Block to, String label) {
    var fromName = idOf(from.lastOp);
    var toName = idOf(to.availOps.peek());
    delayedBlockEdges.add(
        fromName
            + " -> "
            + toName
            + " [lhead=cluster"
            + to.blockID
            + " ltail=cluster"
            + from.blockID
            + " label=\""
            + label
            + "\"]\n");
  }

  private void appendln(String str) {
    sb.append(str).append("\n");
  }

  private void writeHeader() {
    appendln("digraph {");
    appendln("graph[ordering=out,compound=true]");
    appendln("node [fontsize=12 shape=plain style=solid fontname=Calibri color=skyblue]");
    appendln("edge [fontsize=15]");
  }

  private void writerSubgraphHeader(Block block) {
    appendln("subgraph cluster" + block.blockID + " {");
    appendln("label=\"Block " + block.blockID + "\"");
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
    if (insn.isNode) sb.append(" color=blue");
    sb.append(" label=\"").append(insn);
    appendln("\"]");
    return name;
  }

  private static final boolean shortCalls = true;

  public static String asString(AbstractInsnNode insn) {
    if (shortCalls && insn.getOpcode() == Opcodes.INVOKEDYNAMIC) {
      return "INVOKEDYNAMIC " + ((InvokeDynamicInsnNode) insn).bsm.getName();
    } else if (shortCalls && insn.getOpcode() == Opcodes.INVOKEVIRTUAL) {
      return "INVOKEVIRTUAL " + ((MethodInsnNode) insn).name + ((MethodInsnNode) insn).desc;
    }
    var printer = new Textifier();
    var tmv = new TraceMethodVisitor(printer);
    insn.accept(tmv);
    return ((String) (printer.getText().get(0))).trim().replace("\"", "\\\"");
  }
}
