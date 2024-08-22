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

import static optimus.graph.loom.compiler.LInsnDiGraphWriter.asString;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.IFEQ;
import static org.objectweb.asm.Opcodes.IRETURN;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.ATHROW;
import java.util.ArrayList;
import org.objectweb.asm.Label;
import org.objectweb.asm.tree.AbstractInsnNode;

class Op {
  AbstractInsnNode insnNode;
  int localSlot;
  int stackSlot; // Keep track of the result of this operation
  int index;
  int lineNum;
  int blockID;
  public final boolean isNode;
  Label label;
  public int popCount;
  public boolean hasResult;
  public int resultLoadOp = ALOAD; // etc...
  ArrayList<Op> consumers = new ArrayList<>();
  ArrayList<Op> inputs = new ArrayList<>();
  int incomingEdges;

  @Override
  public String toString() {
    return asString(insnNode);
  }

  public Op(AbstractInsnNode i, int index, int lineNum, int blockID, boolean isNode) {
    insnNode = i;
    this.index = index;
    this.lineNum = lineNum;
    this.blockID = blockID;
    this.isNode = isNode;
  }

  public Op() {
    isNode = false;
  }

  public void addConsumer(Op consumerOp) {
    consumerOp.incomingEdges++;
    consumerOp.inputs.add(this);
    consumers.add(consumerOp);
  }

  public int index() {
    return index;
  }

  public boolean isEndOfFlow() {
    var opcode = insnNode.getOpcode();
    return (IRETURN <= opcode && opcode <= RETURN) || opcode == ATHROW;
  }

  public boolean isEndOfBranch() {
    var opcode = insnNode.getOpcode();
    return (IFEQ <= opcode && opcode <= RETURN) || opcode == ATHROW;
  }

  static class Phi extends Op {
    int transferSlot;

    Phi(int stackSlot) {
      transferSlot = stackSlot;
      this.stackSlot = stackSlot;
      lineNum = -1;
      hasResult = true;
    }

    @Override
    public String toString() {
      return "φ " + transferSlot;
    }
  }
}
