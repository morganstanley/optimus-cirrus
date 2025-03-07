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

import static optimus.graph.loom.compiler.LCompiler.POP_NOTHING_BUT_READS;
import static optimus.graph.loom.compiler.LCompiler.POP_NOTHING_NA;
import static optimus.graph.loom.compiler.Op.MUTATES_GLOBAL;
import static optimus.graph.loom.compiler.Op.IS_CTOR;
import static optimus.graph.loom.compiler.Op.IS_NODE;
import java.util.Collections;
import java.util.List;
import optimus.graph.loom.LoomAdapter;
import optimus.graph.loom.TransformableMethod;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.LookupSwitchInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TableSwitchInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

/** Compiler phase. It reads a list of bytecode instructions to create a DAG of blocks. */
public class Parser implements Opcodes {
  private static final Block[] EmptyTargets = new Block[0];

  private static final boolean warnOnUnexpectedStackSize = true; // necessary for parsing!

  private int curIIndex; // Used to preserve the ordering close to the original

  private int curLineNum; // Track the line number for each instruction

  private Block curBlock; // Track the current branch in the control flow

  private Op lastOp; // Last generated Op

  // if compilerArgs.assumeGlobalMutation, preserves the order between non-node method invocations
  private final Op globalParam =
      new Op.Param(-1, null) {
        @Override
        public String toString() {
          return "GlobalParam";
        }
      };

  private final MethodNode method;
  private final AbstractInsnNode[] insns;

  private final DCFlowGraph state;

  public Parser(TransformableMethod tmethod, LoomAdapter adapter) {
    this.method = tmethod.method;
    this.insns = method.instructions.toArray();
    this.state = new DCFlowGraph(tmethod, adapter);
  }

  DCFlowGraph parse() {
    curIIndex = 0;
    curLineNum = -1;
    curBlock = null;
    FrameNode lastFrameNode = null;

    // Track the line number for each instruction
    Label curLabel = null;
    registerLocalVariables();
    for (AbstractInsnNode insn : insns) {
      curIIndex++;
      if (insn instanceof LabelNode) {
        curLabel = ((LabelNode) insn).getLabel();
        if (state.isRegisteredLabel(curLabel) && lastOp != null) lastOp.label = curLabel;

      } else if (insn instanceof LineNumberNode) {
        curLineNum = ((LineNumberNode) insn).line;
      } else if (insn instanceof FrameNode) {
        lastFrameNode = (FrameNode) insn;
      } else {
        // Start a new block if there is no current block or saw a FrameNode
        if (curBlock == null || lastFrameNode != null) {
          var newBlock = state.blockFromLabel(curLabel);
          newBlock.setFrame(lastFrameNode);
          lastFrameNode = null;
          switchBlockTo(newBlock, new Block[] {newBlock}, null, 0);
          state.tryToAssignStartBlock(curBlock);
        }
        // First time see instructions in this block
        if (curBlock.blockID < 0) {
          curBlock.lineNum = curLineNum; // Debugging help...
          state.registerBlock(curBlock);
        }
        var handler = OpcodeHandlers.opcodes[insn.getOpcode()];
        handler.handle(this, insn);
      }
    }
    return state;
  }

  void storeVar(AbstractInsnNode insn) {
    var vi = (VarInsnNode) insn;
    var op = handleOp(insn, 1, false, null);
    var inputOp = op.inputs.get(0); // store operations only have one input!
    op.valueGroup = inputOp.valueGroup; // Value moved
    op.resultType = inputOp.resultType;
    state.updateVar(vi.var, op);
  }

  void loadVar(AbstractInsnNode i) {
    var localOp = state.getVar(((VarInsnNode) i).var);
    if (localOp == null) LMessage.fatal("Reading a value before writing it!");

    var op = handleOp(i, POP_NOTHING_NA, true, localOp.resultType);
    op.valueGroup = localOp.valueGroup; // Value moved
    if (localOp instanceof Op.Param) curBlock.availOps.add(op);
    else if (localOp.blockOwner != curBlock) {
      localOp.addConsumer(op);

      curBlock.addReadVar(op);

      // Branch can't be scheduled until the localOp completes [SEE_BLOCK_VAR_DEPENDENCY]
      localOp.addDependency(localOp.blockOwner.lastOp);
    } else localOp.addConsumer(op);
  }

  /** If block.targets where not set before (like cond jump) it will be set here */
  private void switchBlockTo(Block block, Block[] targets, Op.Branch exitOp, int popCount) {
    if (curBlock == block) return;
    // Complete block ...
    if (curBlock != null) {
      if (exitOp == null) exitOp = newBranch(null, true);
      curBlock.lastOp = exitOp;
      exitOp.targets = targets;
      curBlock.setTargets(targets);
      for (var target : targets) target.setStack(curBlock, popCount);
      handleOp(exitOp, curBlock.stackCount, false); // Eat an entire stack
    }
    lastOp = null;
    curBlock = block;
  }

  private Op newOp(AbstractInsnNode i) {
    return newOp(i, null, 0, 0);
  }

  private Op newOp(AbstractInsnNode i, Type resultType, int flags, int mutatesArgMask) {
    lastOp = new Op(i, curIIndex, curLineNum, curBlock, resultType, flags, mutatesArgMask);
    curBlock.dbgOpsInBlock.add(lastOp);
    return lastOp;
  }

  /**
   * @param fallThrough true if branch's first target is a fall-through
   */
  private Op.Branch newBranch(AbstractInsnNode insn, boolean fallThrough) {
    return new Op.Branch(insn, curIIndex, curLineNum, curBlock, fallThrough);
  }

  Op handleOp(AbstractInsnNode i, int popCount, boolean hasResult, Type resultType) {
    return handleOp(i, popCount, hasResult, resultType, /*flags*/ 0, /* mutatesArgMask*/ 0);
  }

  void handleInvoke(String desc, AbstractInsnNode insn) {
    int opCode = insn.getOpcode();
    var mtype = Type.getMethodType(desc);
    var needsThis = opCode == INVOKESTATIC || opCode == INVOKEDYNAMIC ? 0 : 1;
    var removeFromStack = mtype.getArgumentCount() + needsThis;

    var returnType = mtype.getReturnType();
    // Non-void function push values
    var hasResult = returnType.getSize() > 0;
    var isNode = isNodeCall(insn);
    // we assume that node functions do not mutate their args
    // TODO (OPTIMUS-66991): Revisit the -1, ideally we want to only flag non-primitive args...
    var mutatesArgMask = isNode ? 0 : -1;
    var isCtor = opCode == INVOKESPECIAL && ((MethodInsnNode) insn).name.equals("<init>");
    var globalMutation = state.compilerArgs.assumeGlobalMutation;
    var flags = (isNode ? IS_NODE : globalMutation ? MUTATES_GLOBAL : 0) | (isCtor ? IS_CTOR : 0);
    handleOp(insn, removeFromStack, hasResult, returnType, flags, mutatesArgMask);
  }

  void handleArrayStore(AbstractInsnNode i) {
    handleOp(i, 3, false, null, 0, 0x1);
  }

  void handleDup(AbstractInsnNode i) {
    var top = curBlock.stack[curBlock.stackCount - 1];
    var newOp = newOp(i);
    newOp.popCount = POP_NOTHING_BUT_READS;
    newOp.hasResult = true;
    newOp.resultType = top.op.resultType;
    newOp.valueGroup = top.op.valueGroup;
    top.op.addConsumer(newOp);
    curBlock.pushStack(newOp, warnOnUnexpectedStackSize);
  }

  /** POP2 is used to either pop two values or pop longs/doubles (they have size 2) */
  void handlePop2(AbstractInsnNode i) {
    var top = curBlock.stack[curBlock.stackCount - 1];
    var popHandler = OpcodeHandlers.opcodes[POP];
    popHandler.handle(this, i);
    if (top.op.resultType.getSize() <= 1) {
      popHandler.handle(this, i);
    }
  }

  void handleReturnValue(AbstractInsnNode i) {
    handleEndOfFlow(i, 1, 1);
  }

  void handleReturnVoid(AbstractInsnNode i) {
    handleEndOfFlow(i, POP_NOTHING_NA, 0);
  }

  void handleThrow(AbstractInsnNode i) {
    handleEndOfFlow(i, 1, curBlock.stackCount);
  }

  private void handleEndOfFlow(AbstractInsnNode i, int popCount, int expectedStackSize) {
    if (curBlock.stackCount != expectedStackSize) LMessage.fatal("Stack count is wrong!");
    var endOfFlowOp = newBranch(i, false);
    assignDataLabel(endOfFlowOp);
    switchBlockTo(null, EmptyTargets, endOfFlowOp, popCount);
  }

  void handleUnconditionalJump(AbstractInsnNode insn) {
    var ji = (JumpInsnNode) insn;
    handleJump(0, ji, Collections.emptyList(), ji.label, 0);
  }

  void handleConditionalJump(int popCount, AbstractInsnNode insn) {
    var ji = (JumpInsnNode) insn;
    handleJump(popCount, ji, Collections.emptyList(), ji.label, 1);
  }

  void handleTableSwitch(AbstractInsnNode insn) {
    var ts = (TableSwitchInsnNode) insn;
    handleJump(1, insn, ts.labels, ts.dflt, 0);
  }

  void handleLookupSwitch(AbstractInsnNode insn) {
    var ls = (LookupSwitchInsnNode) insn;
    handleJump(1, ls, ls.labels, ls.dflt, 0);
  }

  private void handleJump(
      int popC, AbstractInsnNode insn, List<LabelNode> labels, LabelNode dflt, int fallThrough) {
    var targets = new Block[fallThrough + (dflt == null ? 0 : 1) + labels.size()];
    Block falseBlock = null;
    var targetCount = 0;
    var hasFallThroughTarget = fallThrough == 1;

    if (hasFallThroughTarget) { // fallThrough
      if (insn.getNext() instanceof LabelNode) {
        var falseLabel = ((LabelNode) insn.getNext()).getLabel();
        falseBlock = state.blockFromLabel(falseLabel);
      } else falseBlock = state.blockFromLabel(null);
      targets[targetCount++] = falseBlock;
    }

    if (dflt != null) {
      var dfltTarget = state.blockFromLabel(dflt.getLabel());
      targets[targetCount++] = dfltTarget;
    }

    for (LabelNode label : labels) {
      var target = state.blockFromLabel(label.getLabel());
      targets[targetCount++] = target;
    }

    var branchOp = newBranch(insn, hasFallThroughTarget);
    assignDataLabel(branchOp);
    switchBlockTo(falseBlock, targets, branchOp, popC);
  }

  private Op handleOp(
      AbstractInsnNode i,
      int popCount,
      boolean hasResult,
      Type resultType,
      int flags,
      int mutatesArgMask) {
    var op = newOp(i, resultType, flags, mutatesArgMask);
    handleOp(op, popCount, hasResult);
    return op;
  }

  private void handleOp(Op newOp, int popCount, boolean hasResult) {
    // Notice -1 i.e. POP_NOTHING_NA is a valid count, meaning not available and nothing to pop
    newOp.popCount = popCount;
    newOp.hasResult = hasResult;
    if (newOp.mutatesGlobal()) mutateSlots(newOp, globalParam);
    if (popCount == 0 && newOp.isAvailable()) curBlock.availOps.add(newOp);
    else
      for (int i = 0; i < popCount; i++) {
        var arg = curBlock.popStack();
        var argPos = popCount - i - 1;
        // <init> always mutates the argument even for non-mutable class!
        if ((argPos == 0 && newOp.isCtor()) || newOp.canMutateArgs(argPos) && isMutable(arg))
          mutateSlots(newOp, arg);
        else if (needDependency(newOp, arg)) arg.valueGroup.lastUpdate.addDependency(newOp);
        arg.addConsumer(newOp);
      }
    if (hasResult) curBlock.pushStack(newOp, warnOnUnexpectedStackSize);
    if (state.compilerArgs.enqueueEarlier && newOp.isNode()) markAsHigherPriority(newOp);
  }

  private boolean isNodeCall(AbstractInsnNode i) {
    if (i instanceof MethodInsnNode) {
      var invoke = (MethodInsnNode) i;
      return state.adapter.isNodeCall(invoke);
    } else return false;
  }

  private void assignDataLabel(Op.Branch op) {
    var nexti = op.insnNode.getNext();
    var nextLabel = nexti instanceof LabelNode ? ((LabelNode) nexti).getLabel() : null;
    if (nextLabel != null && state.isRegisteredLabel(nextLabel)) op.label = nextLabel;
  }

  private boolean isMutable(Op op) {
    if (op.insnNode instanceof LdcInsnNode) {
      return false;
    }

    if (op.valueGroup.mutable) return true;

    var resultTypeSort = op.resultType.getSort();
    if (op.hasResult && (resultTypeSort == Type.OBJECT || resultTypeSort == Type.ARRAY)) {
      // Detect and exclude strings, entities from the @loom(entities=...) and check the list
      return !state.adapter.isImmutable(op.resultType.getInternalName());
    }

    return false;
  }

  private void mutateSlots(Op updateOp, Op arg) {
    var lastUpdate = arg.valueGroup.lastUpdate;
    if (needDependency(updateOp, arg) && lastUpdate.blockOwner == curBlock) {
      lastUpdate.addDependency(updateOp);
      if (updateOp.insnNode.getOpcode() == INVOKEDYNAMIC) updateOp.valueGroup.mutable = true;
      ValueGroup.link(updateOp, arg);
    }
    arg.valueGroup.setLastUpdate(updateOp);
  }

  private boolean needDependency(Op updateOp, Op arg) {
    var lastUpdate = arg.valueGroup.lastUpdate;
    return lastUpdate != arg // arg has been updated
        && lastUpdate != updateOp // updateOp wasn't already what updated arg
        && !(lastUpdate instanceof Op.Param); // Op.Param are always available
  }

  private void markAsHigherPriority(Op op) {
    op.index -= insns.length;
    // forcing priority rearrangement if already in availOps
    if (curBlock.availOps.remove(op)) curBlock.availOps.add(op);
    // its inputs must also change in priority!
    op.inputs.forEach(this::markAsHigherPriority);
  }

  /** Almost the same as updateLocalVariables but doesn't mutate it in case we need to bail out */
  private void registerLocalVariables() {
    for (var lv : method.localVariables) {
      state.registerLabel(lv.start.getLabel());
      state.registerLabel(lv.end.getLabel());
    }
  }
}
