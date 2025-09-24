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
import java.util.ArrayList;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;

/**
 * Represents an operation, which usually (see inner static classes for exceptions) corresponds to a
 * bytecode instruction.
 */
class Op {
  static final int IS_NODE = 1;
  static final int IS_DELAYED = 1 << 1;
  static final int IS_CTOR = 1 << 2;
  static final int MUTATES_GLOBAL = 1 << 3;
  AbstractInsnNode insnNode;
  int localSlot;
  int stackSlot; // Keep track of the result of this operation
  int index;
  int lineNum;
  Block blockOwner;
  public int flags;
  final int mutatesArgsMask; // can potentially mutate argument N mask
  Label label;

  int dbgWritten;
  public int popCount;
  public boolean hasResult;
  public Type resultType; // REVISIT: consider storing as String!!!

  ValueGroup valueGroup;
  ArrayList<Op> inputs = new ArrayList<>(); // Actual inputs to this Op

  ArrayList<Op> consumers = new ArrayList<>();
  // All those ops needs to be delayed until this Op is done (the edge drawn in red in digraph)
  ArrayList<Op> dependencies = new ArrayList<>();
  private int incomingEdges;
  private final ArrayList<Op> dbgIncomingEdges = new ArrayList<>();

  @Override
  public String toString() {
    // useful when debugging:
    // return index + "." + isAvailable() + "." + asString(insnNode);
    return asString(insnNode);
  }

  public Op(
      AbstractInsnNode i,
      int index,
      int lineNum,
      Block block,
      Type resultType,
      int flags,
      int mutatesArgsMask) {
    insnNode = i;
    this.index = index;
    this.lineNum = lineNum;
    this.blockOwner = block;
    this.flags = flags;
    this.mutatesArgsMask = mutatesArgsMask;
    this.valueGroup = new ValueGroup(this);
    this.resultType = resultType;
  }

  public Op() {
    this.flags = 0;
    this.mutatesArgsMask = 0;
    this.valueGroup = new ValueGroup(this);
  }

  void addIncomingEdge(Op source) {
    incomingEdges++;
    dbgIncomingEdges.add(source);
    if (source == this) LMessage.fatal("Cannot add incoming edge on the op itself!");
  }

  public void addDependency(Op dependency) {
    dependencies.add(dependency);
    dependency.addIncomingEdge(this);
  }

  public void removeDependency(Op dependency) {
    dependencies.remove(dependency);
    dependency.removeIncomingEdge(this);
  }

  int removeIncomingEdge(Op source) {
    if (!dbgIncomingEdges.remove(source)) {
      LMessage.error(
          "Can't remove "
              + source
              + " ["
              + source.index
              + "] from "
              + this
              + " ["
              + this.index
              + "] count: "
              + incomingEdges);
    }
    return --incomingEdges;
  }

  boolean isAvailable() {
    return incomingEdges == 0;
  }

  public boolean isNode() {
    return (flags & IS_NODE) == IS_NODE;
  }

  public boolean isCtor() {
    return (flags & IS_CTOR) != 0;
  }

  public boolean isDelayed() {
    return (flags & IS_DELAYED) == IS_DELAYED;
  }

  public boolean mutatesGlobal() {
    return (flags & MUTATES_GLOBAL) == MUTATES_GLOBAL;
  }

  /** Assume @node does NOT mutate args */
  public boolean canMutateArgs(int argIndex) {
    return (mutatesArgsMask & (1 << argIndex)) != 0;
  }

  public boolean isPhi() {
    return false;
  }

  public void addConsumer(Op consumerOp) {
    consumerOp.addIncomingEdge(this);
    consumerOp.inputs.add(this);
    consumers.add(consumerOp);
  }

  public void removeConsumer(Op consumerOp) {
    consumerOp.removeIncomingEdge(this);
    consumers.remove(consumerOp);
  }

  public void removeInput(Op inputOp) {
    inputs.remove(inputOp);
    popCount--;
  }

  public int index() {
    return index;
  }

  public String resultTypeName() {
    return resultType.getInternalName();
  }

  public void write(Regenerator reg) {
    reg.write(this, insnNode, popCount, hasResult, true);
  }

  public Op singleInput() {
    if (inputs.size() != 1) LMessage.fatal("Expected one input");
    return inputs.get(0);
  }

  public Op singleConsumer() {
    if (consumers.size() != 1) LMessage.fatal("Expected one consumer");
    return consumers.get(0);
  }

  public void changeInputFromTo(Op org, Op newOp) {
    dbgIncomingEdges.remove(org);
    dbgIncomingEdges.add(newOp);
    for (int i = 0; i < inputs.size(); i++)
      if (inputs.get(i) == org) {
        inputs.set(i, newOp);
        return;
      }
    LMessage.fatal("changeInputFromTo could not find org " + org);
  }

  /** We can just dup the operation but I don't think it's worth it */
  public void markAsDelayed() {
    flags = flags & ~IS_NODE | IS_DELAYED;
  }

  /** Represents a phi function, which allows to pick between multiple inputs. */
  static class Phi extends Op {
    private final int transferSlot;

    Phi(int stackSlot) {
      this.transferSlot = stackSlot;
      this.stackSlot = stackSlot;
      this.index = stackSlot;
      lineNum = -1;
      hasResult = true;
    }

    @Override
    public boolean isPhi() {
      return true;
    }

    @Override
    public String toString() {
      return "φ" + transferSlot + ": " + resultType;
    }
  }

  /**
   * Represents a jump together with their possible target destinations. Examples of jumps include
   * if else, lookupswitch, tableswitch, goto, and return instructions.
   */
  static class Branch extends Op {
    public Block commonTarget;
    Block[] targets;
    private final boolean hasFallThroughTarget; // target[0] is a fall through target

    public Branch(AbstractInsnNode i, int index, int lineNum, Block block, boolean ft) {
      // branches cannot mutate args!
      super(i, index, lineNum, block, null, 0, 0);
      this.hasFallThroughTarget = ft;
    }

    @Override
    public String toString() {
      return insnNode == null ? "[NXT]" : super.toString();
    }

    @Override
    public void write(Regenerator regenerator) {
      if (insnNode != null) super.write(regenerator); // write IF...
      else if (hasFallThroughTarget) {
        // super.write() updates the stack for real branches
        // but we also need to do it for "fake" ones.
        regenerator.adjustInputsStack(inputs);
      }

      if (commonTarget != null) { // if we are dealing with a reduced branch
        regenerator.writeBlocks(targets);
        var newStartingLabel = regenerator.addLabelNode(commonTarget.label);
        commonTarget.updateHeadLabel(newStartingLabel);
      } else {
        // it has a fall through branch (IF_XXXX) or was just a real FT [SEE_FT_OPTIONAL_GOTO]
        if (hasFallThroughTarget && targets[0].visited) {
          // fallThrough branch is a target of another jump and it has been laid out before.
          regenerator.writeJumpToBlock(targets[0]);
        }
        if (regenerator.state.compilerArgs.usePostOrder)
          for (Block target : targets) {
            regenerator.regen(target);
          }
      }
    }
  }

  /** Represents a parameter, either global (see globalParam) or passed into the method. */
  static class Param extends Op {
    Param(int slot, Type resultType) {
      this.localSlot = slot;
      this.resultType = resultType;
    }

    @Override
    public String toString() {
      return resultType.toString();
    }
  }
}
