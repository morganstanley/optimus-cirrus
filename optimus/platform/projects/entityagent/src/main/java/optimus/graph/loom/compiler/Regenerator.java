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

import static optimus.graph.loom.NameMangler.mangleName;
import static optimus.graph.loom.NameMangler.mkQueuedName;
import static optimus.debug.CommonAdapter.changeReturnType;
import static optimus.debug.CommonAdapter.descToClass;
import static optimus.debug.CommonAdapter.getReturnDesc;
import static optimus.debug.InstrumentationConfig.OBJECT_DESC;
import static optimus.graph.loom.LoomConfig.NODE_FUTURE;
import static optimus.graph.loom.LoomConfig.NODE_FUTURE_DESC;
import static optimus.graph.loom.LoomConfig.NODE_FUTURE_TYPE;
import static optimus.graph.loom.LoomConfig.TO_VALUE_PREFIX;
import static optimus.graph.loom.compiler.LCompiler.POP_NOTHING_BUT_READS;
import java.util.ArrayList;
import java.util.List;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

/**
 * Compiler phase. Given a DAG of blocks, it write bytecode instructions that schedule node calls
 * and retrieve their value as necessary.
 */
public class Regenerator implements Opcodes {

  private static final boolean warnOnUnexpectedStackSize = false; // not needed for regen

  private int curLineNum; // Track the line number for each instruction

  private Block curBlock; // Track the current branch in the control flow

  private final boolean visibleQueuedNode = true;

  final DCFlowGraph state;

  Regenerator(DCFlowGraph state) {
    this.state = state;
  }

  void regen() {
    curLineNum = -1;
    addLabelNode(state.getStartBlock().label);
    // state.blocks are in the original parse order
    // TODO (OPTIMUS-66991): Revisit - can we use the revert rpo order here?
    //  see optimus.graph.loom.CompilerArgs#usePostOrder
    for (var block : state.blocks) regen(block);
    if (!(state.getLastResult() instanceof LabelNode)) addLabelNode(new Label());
    state.updateLocalVariables();
  }

  void regen(Block block) {
    if (block.foldedIn || block.visited) return;
    block.visited = true;

    // Blocks needs a label only if it had one
    if (block.label != null) block.updateHeadLabel(setupLabelIfNeeded());

    curBlock = block;
    var availOps = block.availOps;
    var delayedOps = block.delayedOps;

    block.updatePhiTypes(state);
    availOps.addAll(block.phiOps);

    for (var readVar : block.readsVars) {
      // [SEE_BLOCK_READVAR] On entry into the block all readVar
      // that had no-other dependency will become available
      if (readVar.removeIncomingEdge(block) == 0) {
        availOps.add(readVar);
      }
    }
    while (!(availOps.isEmpty())) {
      writeAvailOps(block);
      if (!delayedOps.isEmpty()) {
        var op = delayedOps.removeFirst();
        availOps.add(op);
      }
    }
    block.lastOp.write(this);

    invariantCheckAfterBlockWritten(block);
  }

  private void writeAvailOps(Block block) {
    var availOps = block.availOps;
    var delayedOps = block.delayedOps;
    while (!availOps.isEmpty()) {
      var op = availOps.poll();
      if (op != block.lastOp) {
        updateLineNumber(op);
        if (op.isNode() && !(availOps.isEmpty() && delayedOps.isEmpty())) {
          writeEnqueueNodeCall(op);
          op.markAsDelayed();
          delayedOps.offerLast(op);
        } else if (op.isDelayed()) {
          writeNodeToValue(op);
          releaseDependencies(op);
        } else {
          op.write(this);
          releaseDependencies(op);
        }
      }
    }
  }

  private void writeEnqueueNodeCall(Op op) {
    var nodeCall = (MethodInsnNode) op.insnNode;
    var opCode = nodeCall.getOpcode();
    var queuedName = mkQueuedName(mangleName(nodeCall.owner), nodeCall.name);
    var desc = changeReturnType(nodeCall.desc, NODE_FUTURE_DESC);
    var i = new MethodInsnNode(opCode, nodeCall.owner, queuedName, desc, nodeCall.itf);

    var someVariablesEndOfLife = op.label;
    op.label = null; // delay label until after store
    write(op, i, op.popCount, op.hasResult, true);
    op.localSlot = state.addTempVar(NODE_FUTURE_TYPE);
    op.label = someVariablesEndOfLife;
    write(op, new VarInsnNode(ASTORE, op.localSlot), 1, false, false);
    if (visibleQueuedNode && op.label == null) { // Create `start` label for node$queued var
      op.label = new Label();
      addLabelNode(op.label);
    }
  }

  /** Writes out toValue$ call with proper casting/unboxing provided the original call op */
  private void writeNodeToValue(Op op) {
    var nodeQueuedStartLabel = op.label;
    op.label = null; // None of the write(op....) should write it!
    write(op, new VarInsnNode(ALOAD, op.localSlot), 0, true, false);
    var methodOp = (MethodInsnNode) op.insnNode;
    var returnType = getReturnDesc(methodOp.desc);
    String toValueName;
    String toValueReturnType = returnType;
    var needsCast = false;
    if (returnType.startsWith("L") || returnType.startsWith("[")) {
      toValueName = TO_VALUE_PREFIX;
      toValueReturnType = OBJECT_DESC;
      if (!returnType.equals(OBJECT_DESC)) needsCast = true;
    } else {
      toValueName = TO_VALUE_PREFIX + returnType;
    }
    var toValueDesc = "()" + toValueReturnType;
    var i = new MethodInsnNode(INVOKEINTERFACE, NODE_FUTURE, toValueName, toValueDesc, true);
    write(op, i, 1, true, false);
    if (needsCast) {
      var castToOriginalType = new TypeInsnNode(CHECKCAST, descToClass(returnType));
      write(op, castToOriginalType, 1, true, false);
    }
    // This is not needed as JVM very happily gets rid of no-longer used references
    // but if one doesn't trust this... consider uncommenting the next 2 lines
    // state.addToResult(new InsnNode(ACONST_NULL));
    // state.addToResult(new VarInsnNode(ASTORE, op.localSlot));
    if (visibleQueuedNode && nodeQueuedStartLabel != null) {
      var start = state.registerLabel(nodeQueuedStartLabel);
      var end = addLabelNode(new Label());
      var name = methodOp.name + "_node_" + op.localSlot;
      var lvn = new LocalVariableNode(name, NODE_FUTURE_DESC, null, start, end, op.localSlot);
      state.addLocalVariable(lvn);
    }
  }

  private void releaseDependencies(Op op) {
    releaseDependencies(op, op.consumers);
    releaseDependencies(op, op.dependencies);
  }

  private void releaseDependencies(Op orgOp, List<Op> ops) {
    for (var c : ops) {
      var edgesLeft = c.removeIncomingEdge(orgOp);
      if (edgesLeft == 0 && curBlock.matches(c.blockOwner)) {
        curBlock.availOps.add(c);
      }
    }
  }

  /** Allows to specify all the parts separately */
  void write(Op op, AbstractInsnNode i, int popC, boolean hasResult, boolean orderSlots) {
    // Verify inputs are in correct slots
    if (orderSlots && (popC > 0 || popC == POP_NOTHING_BUT_READS)) {
      adjustInputsStack(op.inputs);

      if (popC > 0) {
        if (popC != op.inputs.size()) LMessage.fatal("Invalid number of arguments");
        for (var index = 0; index < popC; index++) {
          var input = op.inputs.get(popC - 1 - index);
          if (input.stackSlot != curBlock.stackCount - (popC - index)) {
            LMessage.fatal("input.stackSlot and stackCount doesn't match!");
          }
        }
      }
    }
    adjustStack(op, popC, hasResult);
    // Add result....
    if (!op.isPhi()) { // Phi is not an actual bytecode instruction!
      state.addToResult(i);
      op.dbgWritten = state.getResult().size();
      if (op.label != null) addLabelNode(op.label);
    }
  }

  /* Adjust stack on regen() */
  private void adjustStack(Op op, int popCount, boolean hasResult) {
    for (int i = 0; i < popCount; i++) curBlock.popStack();
    if (hasResult) curBlock.pushStack(op, warnOnUnexpectedStackSize);
  }

  // consider moving this to a separate class Result?
  void adjustInputsStack(ArrayList<Op> inputs) {
    int argCount = inputs.size();
    int matchedPos = 0; // Number of args from right that matched
    // Right to left!
    for (Op input : inputs) {
      if (input.stackSlot == -1) {
        spillStack(matchedPos);
        matchedPos = 0;
      } else if (input.stackSlot == curBlock.stackCount - 1 - matchedPos) {
        matchedPos += 1;
      } else {
        spillStack(curBlock.stackCount - input.stackSlot - 1);
        matchedPos = 1; // Last slot is only matching
      }
    }
    // Left to right
    for (var idx = argCount - matchedPos - 1; idx >= 0; idx--) {
      var input = inputs.get(idx); // Last argument first
      curBlock.pushStack(input, warnOnUnexpectedStackSize);
      state.addToResult(new VarInsnNode(input.resultType.getOpcode(ILOAD), input.localSlot));
    }
  }

  private void spillStack(int count) {
    for (int i = 0; i < count; i++) {
      var op = curBlock.popStack();
      // spilledOps.add(op);
      op.localSlot = state.addTempVar(op.resultType);
      // double and long need an extra slot to be stored!
      if (op.resultType.getSize() > 1) {
        state.addBusySlot();
      }
      state.addToResult(new VarInsnNode(op.resultType.getOpcode(ISTORE), op.localSlot));
    }
  }

  void writeBlocks(Block[] targets) {
    var prevBlock = curBlock;
    for (Block target : targets) {
      curBlock = prevBlock;
      regen(target);
    }
    curBlock = prevBlock;
  }

  void writeJumpToBlock(Block block) {
    state.addToResult(new JumpInsnNode(GOTO, state.registerLabel(block.label)));
  }

  LabelNode addLabelNode(Label label) {
    var labelNode = state.registerLabel(label);
    var lastLabelNode = getLastLabelNode();
    if (lastLabelNode == labelNode) return labelNode; // Already wrote the same label
    if (lastLabelNode != null) {
      LMessage.error("Label right after label?");
      return labelNode;
    }
    state.addToResult(labelNode);
    return labelNode;
  }

  private void updateLineNumber(Op op) {
    if (op.lineNum > 0 && op.lineNum != curLineNum) {
      LabelNode labelNode = getLastLabelNode();
      if (labelNode == null) labelNode = addLabelNode(new Label());
      state.addToResult(new LineNumberNode(op.lineNum, labelNode));
      curLineNum = op.lineNum;
    }
  }

  LabelNode setupLabelIfNeeded() {
    var lastLabelNode = getLastLabelNode();
    if (lastLabelNode != null) return lastLabelNode;
    if (state.getLastResult() instanceof LineNumberNode)
      LMessage.fatal(
          "Last result was lineNumber, not label! This is a case that we need to handle");
    var labelNode = state.registerLabel(new Label());
    state.addToResult(labelNode);
    return labelNode;
  }

  private LabelNode getLastLabelNode() {
    var lastResult = state.getLastResult();
    return lastResult instanceof LabelNode ? (LabelNode) lastResult : null;
  }

  private void invariantCheckAfterBlockWritten(Block block) {
    var dbgLeftOvers = block.dbgNonAvailableOps();
    if (dbgLeftOvers.length > 0) {
      LMessage.error("Leftover instructions on the block while regen! Very questionable");
    }
  }
}
