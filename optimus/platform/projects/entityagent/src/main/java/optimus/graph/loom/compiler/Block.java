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
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;

/** Represents a group of continuous (i.e., no jumps!) set of operations */
class Block extends Op {
  int blockID; // Assigned in the original order and can be reassigned on block reduce

  public Op lastOp; // Last op in the block which is often branch. Handle non-branch better!

  static class StackSlot {
    public Op op;

    @Override
    public String toString() {
      return op.toString();
    }
  }

  public FrameNode frame;

  public StackSlot[] stack;
  public int stackCount; // Number of items on the stack

  public ArrayList<Phi> phiOps = new ArrayList<>();
  public ArrayList<Op> readsVars = new ArrayList<>();

  public HashSet<Op> dbgOpsInBlock = new HashSet<>();

  Block[] targets;
  ArrayList<Block> predecessors = new ArrayList<>();
  ArrayList<Op> entryOps = new ArrayList<>(); // Jumps that would get to this block

  public Object[] dbgNonAvailableOps() {
    return dbgOpsInBlock.stream().filter(op -> !op.isAvailable()).toArray();
  }

  public void setTargets(Block[] blocks) {
    targets = blocks;
  }

  boolean visited;
  boolean foldedIn = false;
  ArrayList<Block> foldedBlocks = new ArrayList<>();
  long dom; // it keeps track of dominators

  public boolean hasSingleTarget() {
    return targets != null && targets.length == 1;
  }

  public boolean hasSinglePredecessor() {
    return predecessors != null && predecessors.size() == 1;
  }

  public boolean hasSameSingleTargetAndPredecessor(Block block) {
    return hasSingleTarget() // same single target
        && block.hasSingleTarget()
        && targets[0] == block.targets[0]
        // same single predecessor
        && hasSinglePredecessor()
        && block.hasSinglePredecessor()
        && predecessors.get(0) == block.predecessors.get(0);
  }

  public Phi newPhi(int i) {
    var op = new Phi(i);
    op.blockOwner = blockOwner;
    phiOps.add(op);
    return op;
  }

  public void setStack(Block block, int skipCount) {
    predecessors.add(block);
    if (block.lastOp == null) LMessage.fatal("Block completed without a lastOp?");
    entryOps.add(block.lastOp);
    setStackCount(block.stackCount - skipCount);
    for (int i = 0; i < stackCount; i++) {
      Op phiOp;
      if (stack[i] == null) {
        stack[i] = new StackSlot();
        phiOp = newPhi(i);
        stack[i].op = phiOp;
      } else {
        phiOp = stack[i].op;
      }
      var inputOp = block.stack[i].op;
      inputOp.addConsumer(phiOp);
      ValueGroup.link(phiOp, inputOp);
      if (phiOp.resultType == null) phiOp.resultType = inputOp.resultType; // just for single input
    }
  }

  public void setFrame(FrameNode frame) {
    this.frame = frame;
  }

  public void updatePhiTypes(DCFlowGraph state) {
    if (frame == null) return; // Single input should be per phi

    var frameStackSize = frame.stack.size();
    if (phiOps.size() != frameStackSize) LMessage.fatal("Invalid frame count");
    for (int i = 0; i < frameStackSize; i++) {
      Op phiOp = phiOps.get(i);
      var stackSlot = frame.stack.get(i);
      if (stackSlot instanceof String) phiOp.resultType = Type.getObjectType((String) stackSlot);
      for (var input : phiOp.inputs) {
        if (input.hasResult)
          state.adapter.registerCommonType(input.resultTypeName(), phiOp.resultTypeName());
      }
    }
  }

  private void setStackCount(int newStackCount) {
    if (stackCount != 0 && newStackCount != stackCount)
      LMessage.fatal("Trying to set non empty stack with different size!");

    stackCount = newStackCount;
  }

  public Block(int maxStack, Label orgLabel) {
    stack = new StackSlot[maxStack]; // Assume the same max stack
    this.label = orgLabel; // During parse retain original label (helps with visualizing)
    this.blockID = -1; // new block that has seen no instructions
    this.blockOwner = this;
  }

  @Override
  public String toString() {
    return "Block" + blockID;
  }

  final PriorityQueue<Op> availOps = new PriorityQueue<>(Comparator.comparingInt(Op::index));
  final ArrayDeque<Op> delayedOps = new ArrayDeque<>();

  void pushStack(Op op, boolean warnOnUnexpectedStackSize) {
    if (stackCount >= stack.length) {
      // Critical during parsing only, usually means that we missed the pop!
      if (warnOnUnexpectedStackSize) LMessage.fatal("Unexpected stack size!");
      stack = Arrays.copyOf(stack, stackCount * 2);
    }
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

  public void addReadVar(Op op) {
    readsVars.add(op);
    op.addIncomingEdge(this); // [SEE_BLOCK_READVAR]
  }

  public void updateHeadLabel(LabelNode startBlockLabel) {
    var orgLabel = this.label;
    this.label = startBlockLabel.getLabel();
    for (var entryOp : entryOps) {
      if (entryOp.insnNode == null) continue; // [SEE_FT_OPTIONAL_GOTO]
      if (entryOp.insnNode instanceof JumpInsnNode) {
        var jin = (JumpInsnNode) entryOp.insnNode;
        if (jin.label.getLabel() == orgLabel) {
          jin.label = startBlockLabel;
        }
      } else if (entryOp.insnNode instanceof TableSwitchInsnNode) {
        var tsin = (TableSwitchInsnNode) entryOp.insnNode;
        if (tsin.dflt.getLabel() == orgLabel) tsin.dflt = startBlockLabel;
        else swapLabels(startBlockLabel, orgLabel, tsin.labels);
      } else if (entryOp.insnNode instanceof LookupSwitchInsnNode) {
        var lswi = (LookupSwitchInsnNode) entryOp.insnNode;
        if (lswi.dflt.getLabel() == orgLabel) lswi.dflt = startBlockLabel;
        else swapLabels(startBlockLabel, orgLabel, lswi.labels);
      } else LMessage.fatal("Unsupported instruction for updatedHeadLabel. Please add support!");
    }
  }

  private static void swapLabels(LabelNode labelNode, Label orgLabel, List<LabelNode> labels) {
    for (var i = 0; i < labels.size(); i++)
      if (labels.get(i).getLabel() == orgLabel) {
        labels.set(i, labelNode);
        // break;
      }
  }

  // blockIDs will still match even when blockOwners are different when we are merging blocks
  public boolean matches(Block other) {
    return blockID == other.blockID;
  }

  // we don't write branchOp until the all the inputs are ready! // [SEE_BLOCK_VAR_DEPENDENCY]
  void readVarInputsAsDependency(Branch branchOp) {
    for (var readVar : readsVars) {
      readVar.singleInput().addDependency(branchOp);
    }
  }
}
