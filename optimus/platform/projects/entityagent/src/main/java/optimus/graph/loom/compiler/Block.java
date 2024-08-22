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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.objectweb.asm.tree.FrameNode;

class Block extends Op {

  public Op lastOp; // Last op in the block which is often branch. Handle non-branch better!

  static class StackSlot {
    public Op op;
  }

  public FrameNode frame;

  public StackSlot[] stack;
  public int stackCount; // Number of items on the stack

  Block[] targets;
  ArrayList<Block> predecessors = new ArrayList<>();

  public void setTargets(Block[] blocks) {
    targets = blocks;
    for (var b : blocks) b.predecessors.add(this);
  }

  boolean visited;
  long dom;

  public void setStack(Block block) {
    if (stackCount != 0 && block.stackCount != stackCount)
      throw new RuntimeException("LERROR: Trying to set non empty stack with different size!");

    stackCount = block.stackCount;
    for (int i = 0; i < stackCount; i++) {
      Op phiOp;
      if (stack[i] == null) {
        stack[i] = new StackSlot();
        phiOp = new Phi(i);
        phiOp.blockID = blockID;
        stack[i].op = phiOp;
        availOps.add(stack[i].op);
      } else {
        phiOp = stack[i].op;
      }
      phiOp.inputs.add(block.stack[i].op);
    }
  }

  public Block(int maxStack, int blockID) {
    stack = new StackSlot[maxStack]; // Assume the same max stack
    this.blockID = blockID;
  }

  final PriorityQueue<Op> availOps = new PriorityQueue<>(Comparator.comparingInt(Op::index));
  final ArrayDeque<Op> delayedOps = new ArrayDeque<>();

  void pushStack(Op op) {
    if (stackCount >= stack.length) stack = Arrays.copyOf(stack, stackCount * 2);

    var slot = stack[stackCount];
    if (slot == null) slot = stack[stackCount] = new StackSlot();
    slot.op = op;
    op.stackSlot = stackCount;
    stackCount++;
  }

  Op popStack() {
    var res = stack[--stackCount];
    stack[stackCount] = null;
    res.op.stackSlot = -1;
    return res.op;
  }
}
