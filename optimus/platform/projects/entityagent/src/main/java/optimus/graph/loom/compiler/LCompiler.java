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

import static optimus.debug.CommonAdapter.changeReturnType;
import static optimus.debug.CommonAdapter.descToClass;
import static optimus.debug.CommonAdapter.getReturnDesc;
import static optimus.debug.CommonAdapter.isStatic;
import static optimus.debug.CommonAdapter.makePrivate;
import static optimus.debug.InstrumentationConfig.OBJECT_DESC;
import static optimus.debug.InstrumentationConfig.OGSC_TYPE;
import static optimus.graph.loom.LoomConfig.NODE;
import static optimus.graph.loom.LoomConfig.NODE_DESC;
import static optimus.graph.loom.LoomConfig.PLAIN_SUFFIX;
import static optimus.graph.loom.LoomConfig.QUEUED_SUFFIX;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import optimus.BiopsyLab;
import optimus.graph.loom.CompilerArgs;
import optimus.graph.loom.LoomAdapter;
import optimus.graph.loom.TransformableMethod;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.AbstractInsnNodeEx;
import org.objectweb.asm.tree.FrameNode;
import org.objectweb.asm.tree.InsnList;
import org.objectweb.asm.tree.InsnNode;
import org.objectweb.asm.tree.JumpInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LineNumberNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.MethodNode;
import org.objectweb.asm.tree.TypeInsnNode;
import org.objectweb.asm.tree.VarInsnNode;

/** For some ideas see org.objectweb.asm.MethodWriter */
public class LCompiler implements Opcodes {
  private static final Op PARAM = new Op(null, -1, -1, -1, false);
  private final boolean debug;
  private final CompilerArgs compilerArgs;
  private final MethodNode method;
  private final LoomAdapter loomAdapter;

  /* _______________________________ BEGIN PARSING STATE __________________________*/
  private Op[] vars;
  private int tmpVars;

  private final AbstractInsnNode[] insns;
  private int curIIndex; // Used to preserve the ordering close to the original
  private int curLineNum; // Track the line number for each instruction

  private Block startBlock;
  private Block curBlock; // Track the current branch in the control flow
  private int blockCount;
  private Op lastOp; // Last generated Op
  /* Only named/labeled block get registered here */
  private final HashMap<Label, Block> label2block = new HashMap<>();
  private final HashMap<Label, LabelNode> label2node = new HashMap<>();

  /* _______________________________ END PARSING STATE __________________________*/

  /** Result instructions list */
  private ArrayList<AbstractInsnNode> result = new ArrayList<>();

  private final boolean visibleQueuedNode = true;

  public void assertEmptyStack() {
    if (curBlock.stackCount != 0) throw new RuntimeException("LERROR: Stack is not empty!");
  }

  public LCompiler(TransformableMethod tmethod, LoomAdapter loomAdapter) {
    this.compilerArgs = tmethod.compilerArgs;
    this.debug = tmethod.compilerArgs.debug;
    this.method = tmethod.method;
    this.loomAdapter = loomAdapter;
    this.insns = method.instructions.toArray();
    vars = new Op[method.maxLocals];
    var varsIndex = 0;
    if (!isStatic(method.access)) {
      vars[varsIndex++] = PARAM;
    }

    if (method.parameters != null)
      for (var arg : method.parameters) {
        vars[varsIndex++] = PARAM;
      }
  }

  public static void transform(TransformableMethod method, LoomAdapter loomAdapter) {
    // we don't transform if there are no node calls in the method implementation!
    if (method.compilerArgs.level <= 0 || !method.hasNodeCalls) return;

    // we cannot rearrange node methods using try catches!
    if (!method.method.tryCatchBlocks.isEmpty()) return;

    if (method.compilerArgs.queueSizeSensitive) writePlainFunc(method, loomAdapter);

    dumpToFile("before.txt", method);

    try {
      var lc = new LCompiler(method, loomAdapter);
      lc.parse();
      // null would mean we aborted the parsing or otherwise decided not to transform
      // for example no-node calls were made
      if (lc.result != null) {
        lc.computeDOM();
        lc.dumpDotGraphToFile();
        lc.regen();
      }

      // regen failed?
      if (lc.result != null) {
        var instructions = new InsnList();

        LabelNode labelNeedsFrame = null;
        if (method.compilerArgs.queueSizeSensitive) {
          labelNeedsFrame = (LabelNode) lc.result.get(0);
          writeSwitchToPlain(method.method, instructions, labelNeedsFrame, loomAdapter.cls.name);
        }

        for (var i : lc.result) {
          AbstractInsnNodeEx.clear(i);
          instructions.add(i);
          if (i == labelNeedsFrame) instructions.add(new FrameNode(F_SAME, 0, null, 0, null));
        }
        method.method.instructions = instructions;
        dumpToFile("after.txt", method);
      }
    } catch (Throwable ex) {
      ex.printStackTrace();
    }
  }

  private void computeDOM() {
    var r = new ArrayList<Block>(blockCount);
    rpo(startBlock, r);
    Collections.reverse(r);
    for (var b : r) b.visited = false; // Reset visited marker
    for (var b : r) {
      b.dom = (1L << b.blockID);
      long union = b.predecessors.isEmpty() ? 0 : -1; // All bits are in
      for (var p : b.predecessors) union &= p.dom;
      b.dom |= union;
    }
  }

  private void rpo(Block visit, ArrayList<Block> r) {
    if (visit.targets != null)
      for (var block : visit.targets) {
        if (block.visited) continue;
        block.visited = true;
        rpo(block, r);
      }
    r.add(visit);
  }

  private static void writeSwitchToPlain(
      MethodNode method, InsnList instructions, LabelNode firstLabel, String owner) {
    instructions.add(new MethodInsnNode(INVOKESTATIC, OGSC_TYPE, "hasEnoughWork", "()Z"));
    instructions.add(new JumpInsnNode(IFEQ, firstLabel));
    var hasThis = !isStatic(method.access);
    if (hasThis) instructions.add(new VarInsnNode(ALOAD, 0));
    var paramTypes = Type.getArgumentTypes(method.desc);
    var offset = hasThis ? 1 : 0;
    for (Type paramType : paramTypes) {
      instructions.add(new VarInsnNode(paramType.getOpcode(ILOAD), offset));
      offset += paramType.getSize();
    }
    var name = method.name + PLAIN_SUFFIX;
    instructions.add(new MethodInsnNode(INVOKESPECIAL, owner, name, method.desc));
    var returnType = Type.getReturnType(method.desc);
    instructions.add(new InsnNode(returnType.getOpcode(IRETURN)));
  }

  private static void writePlainFunc(TransformableMethod tm, LoomAdapter loomAdapter) {
    var method = tm.method;
    var access = makePrivate(method.access);
    var name = method.name + PLAIN_SUFFIX;
    String[] exceptions = method.exceptions.toArray(new String[0]);
    var plainMethod = new MethodNode(access, name, method.desc, method.signature, exceptions);
    method.accept(plainMethod);
    for (var op : plainMethod.instructions) {
      if (op instanceof MethodInsnNode) {
        var call = (MethodInsnNode) op;
        if (call.name.equals(method.name) && call.owner.equals(loomAdapter.cls.name))
          call.name = method.name + PLAIN_SUFFIX;
      }
    }
    plainMethod.visibleAnnotations = null;
    plainMethod.invisibleAnnotations = null;
    plainMethod.parameters = null;
    loomAdapter.cls.methods.add(plainMethod);
  }

  /**
   * It dumps the bytecode in a file in the current directory, which is extremely useful when
   * debugging.
   */
  private static void dumpToFile(String file, TransformableMethod method) {
    if (method.compilerArgs.debug) dumpToFile(file, BiopsyLab.byteCodeAsString(method.method));
  }

  private static void dumpToFile(String file, String content) {
    try {
      Files.writeString(Paths.get(file), content);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * It dumps the dependencies representation in a file in the current directory, which is extremely
   * useful when debugging.
   */
  private void dumpDotGraphToFile() {
    if (debug) {
      var newLine = System.lineSeparator();
      var dotgraph = String.join(newLine, "```dot", LInsnDiGraphWriter.toString(startBlock), "```");
      dumpToFile("dotgraph.md", dotgraph);
    }
  }

  void storeVal(AbstractInsnNode i) {
    var vi = (VarInsnNode) i;
    var op = handleOp(i, 1, false, ILOAD + i.getOpcode() - ISTORE);
    vars[vi.var] = op;
  }

  void loadVar(AbstractInsnNode i) {
    var localOp = vars[((VarInsnNode) i).var];
    if (localOp == null) throw new RuntimeException("LERROR: Reading a value before writing it!");

    var op = handleOp(i, -1 /* pops nothing but not available!*/, true, i.getOpcode());
    if (localOp == PARAM) curBlock.availOps.add(op);
    else localOp.addConsumer(op);
  }

  private void parse() {
    startBlock = new Block(method.maxStack, 0);
    curIIndex = 0;
    curLineNum = -1;
    curBlock = startBlock;
    blockCount = 1;

    // Track the line number for each instruction
    Label curLabel = null;
    registerLocalVariables();
    for (AbstractInsnNode i : insns) {
      curIIndex++;
      if (i instanceof LabelNode) {
        curLabel = ((LabelNode) i).getLabel();
        if (label2node.containsKey(curLabel) && lastOp != null) lastOp.label = curLabel;
        if (curBlock == startBlock && startBlock.label == null) startBlock.label = curLabel;
      } else if (i instanceof LineNumberNode) {
        curLineNum = ((LineNumberNode) i).line;
        if (curBlock != null && curBlock.lineNum <= 0) curBlock.lineNum = curLineNum;
      } else if (i instanceof FrameNode) {
        var block = blockFromLabel(curLabel);
        block.frame = (FrameNode) i;
        block.lineNum = curLineNum;
        if (lastOp != null) switchBlockTo(block);
      } else {
        var handler = OpcodeHandlers.opcodes[i.getOpcode()];
        handler.handle(this, i);
      }
    }
  }

  /** If block.targets where not set before (like cond jump) it will be set here */
  private void switchBlockTo(Block block) {
    var prevBlock = curBlock;
    if (prevBlock != null) {
      if (lastOp.isEndOfBranch()) prevBlock.lastOp = lastOp;
      else {
        var prevLastOp = lastOp;
        prevBlock.lastOp = newOp(new JumpInsnNode(GOTO, registerLabel(block.label)), false);
        prevLastOp.addConsumer(lastOp); // newOp mutates lastOp
      }
      if (block != null) {
        if (prevBlock != block) block.setStack(prevBlock);
        if (prevBlock.targets == null && !lastOp.isEndOfFlow())
          prevBlock.setTargets(new Block[] {block});
      }
    }
    curBlock = block;
  }

  private Op newOp(AbstractInsnNode i, boolean isNode) {
    lastOp = new Op(i, curIIndex, curLineNum, curBlock.blockID, isNode);
    return lastOp;
  }

  void handleDup(AbstractInsnNode i) {
    var top = curBlock.stack[curBlock.stackCount - 1];
    var newOp = newOp(i, false);
    newOp.popCount = -2;
    newOp.hasResult = true;
    newOp.resultLoadOp = top.op.resultLoadOp;
    top.op.addConsumer(newOp);
    curBlock.pushStack(newOp);
  }

  /** POP2 is used to either pop two values or pop longs/doubles (they have size 2) */
  void handlePop2(AbstractInsnNode i) {
    var top = curBlock.stack[curBlock.stackCount - 1];
    var popHandler = OpcodeHandlers.opcodes[POP];
    popHandler.handle(this, i);
    if (top.op.resultLoadOp != LLOAD && top.op.resultLoadOp != DLOAD) {
      popHandler.handle(this, i);
    }
  }

  void handleConditionalJump(int popCount, AbstractInsnNode i) {
    handleFrameOp(i, popCount);
    var jump = (JumpInsnNode) i;
    var falseBlock = blockFromLabel(null);
    var trueBlock = blockFromLabel(jump.label.getLabel());
    // important! Keep the original order here (false then true)...
    curBlock.setTargets(new Block[] {falseBlock, trueBlock});
    trueBlock.setStack(curBlock);
    // falseBlock.setStack() will be handled in switchBlockTo
    switchBlockTo(falseBlock);
  }

  void handleJump(AbstractInsnNode i) {
    var top = lastOp; // ???
    var newOp = handleFrameOp(i, -1);
    top.addConsumer(newOp);
    var jump = (JumpInsnNode) i;
    var target = blockFromLabel(jump.label.getLabel());
    target.setStack(curBlock);
    curBlock.setTargets(new Block[] {target});
    switchBlockTo(null);
  }

  void handleInvoke(String desc, AbstractInsnNode insn) {
    int opCode = insn.getOpcode();
    var mtype = Type.getMethodType(desc);
    var needsThis = opCode == INVOKESTATIC || opCode == INVOKEDYNAMIC ? 0 : 1;
    var removeFromStack = mtype.getArgumentCount() + needsThis;

    var returnType = mtype.getReturnType();
    // Non-void function push values
    var loadOpcode = returnType != Type.VOID_TYPE ? returnType.getOpcode(ILOAD) : 0;
    var hasResult = returnType.getSize() > 0;
    var isNode = isNodeCall(insn);
    handleOp(insn, removeFromStack, hasResult, loadOpcode, isNode);
  }

  Op handleFrameOp(AbstractInsnNode i, int popCount) {
    return handleOp(i, popCount, false, 0 /* not used*/, false);
  }

  Op handleOp(AbstractInsnNode i, int popCount, boolean hasResult, int loadOp) {
    return handleOp(i, popCount, hasResult, loadOp, /*isNode*/ false);
  }

  Op handleOp(AbstractInsnNode i, int popCount, boolean hasResult, int loadOp, boolean isNode) {
    var op = newOp(i, isNode);
    op.resultLoadOp = loadOp;
    handleOp(op, popCount, hasResult);
    return op;
  }

  private void handleOp(Op newOp, int popCount, boolean hasResult) {
    // Notice -1 is a valid count, meaning not available and nothing to pop
    newOp.popCount = popCount;
    newOp.hasResult = hasResult;
    if (popCount == 0) curBlock.availOps.add(newOp);
    else
      for (int i = 0; i < popCount; i++) {
        var op = curBlock.popStack();
        op.addConsumer(newOp);
      }
    if (hasResult) curBlock.pushStack(newOp);
    if (compilerArgs.enqueueEarlier && newOp.isNode) markAsHigherPriority(newOp);
  }

  private void markAsHigherPriority(Op op) {
    op.index -= insns.length;
    // forcing priority rearrangement if already in availOps
    if (curBlock.availOps.remove(op)) curBlock.availOps.add(op);
    // its inputs must also change in priority!
    op.inputs.forEach(this::markAsHigherPriority);
  }

  /* Adjust stack on regen() */
  private void adjustStack(Op op, int popCount, boolean hasResult) {
    for (int i = 0; i < popCount; i++) curBlock.popStack();
    if (hasResult) curBlock.pushStack(op);
  }

  private boolean isNodeCall(AbstractInsnNode i) {
    if (i instanceof MethodInsnNode) {
      var invoke = (MethodInsnNode) i;
      return loomAdapter.isNodeCall(invoke);
    } else return false;
  }

  private void regen() {
    curLineNum = -1;
    regen(startBlock);
    updateLocalVariables();
  }

  private void regen(Block block) {
    if (block.visited) return;

    block.visited = true;
    curBlock = block;
    if (block.label != null) addLabelNode(block.label);
    if (block.frame != null) result.add(block.frame);
    var availOps = block.availOps;
    var delayedOps = block.delayedOps;
    while (!(availOps.isEmpty() && delayedOps.isEmpty())) {
      writeAvailOps(block);
      if (!delayedOps.isEmpty()) {
        var op = delayedOps.removeFirst();
        updateLineNumber(op);
        writeNodeToValue(op);
        releaseDependencies(op);
      }
    }
    if (block.targets != null) {
      for (var target : block.targets) regen(target);
    }
  }

  /** Writes out toValue call with proper casting/unboxing provided the original call op */
  private void writeNodeToValue(Op op) {
    write(op, new VarInsnNode(ALOAD, op.localSlot), 0, true, false);
    var methodOp = (MethodInsnNode) op.insnNode;
    var returnType = getReturnDesc(methodOp.desc);
    String toValueName;
    String toValueReturnType = returnType;
    var needsCast = false;
    if (!returnType.startsWith("L")) {
      toValueName = "toValue" + returnType;
    } else {
      toValueName = "toValue";
      toValueReturnType = OBJECT_DESC;
      if (!returnType.equals(OBJECT_DESC)) needsCast = true;
    }
    var toValueDesc = "()" + toValueReturnType;
    var i = new MethodInsnNode(INVOKEVIRTUAL, NODE, toValueName, toValueDesc, false);
    write(op, i, 1, true, false);
    if (needsCast) {
      var castToOriginalType = new TypeInsnNode(CHECKCAST, descToClass(returnType));
      write(op, castToOriginalType, 1, true, false);
    }
    if (visibleQueuedNode && op.label != null) {
      var start = registerLabel(op.label);
      var end = addLabelNode(new Label());
      var name = methodOp.name + "_node_" + op.localSlot;
      var lvn = new LocalVariableNode(name, NODE_DESC, null, start, end, op.localSlot);
      method.localVariables.add(lvn);
    }
  }

  private void writeAvailOps(Block block) {
    var availOps = block.availOps;
    var delayedOps = block.delayedOps;
    while (!availOps.isEmpty()) {
      var op = availOps.poll();
      updateLabelNodeOp(op);
      updateLineNumber(op);
      if (op.isNode && !(availOps.isEmpty() && delayedOps.isEmpty())) {
        enqueueNodeCall(op);
        delayedOps.offerLast(op);
      } else {
        write(op);
        if (op.label != null) addLabelNode(op.label);
        releaseDependencies(op);
      }
    }
  }

  /** Making sure labelNodes match, particularly for jump instructions */
  private void updateLabelNodeOp(Op op) {
    if (op.insnNode instanceof JumpInsnNode) {
      var jump = (JumpInsnNode) op.insnNode;
      jump.label = registerLabel(jump.label.getLabel());
    }
  }

  private void enqueueNodeCall(Op op) {
    var nodeCall = (MethodInsnNode) op.insnNode;
    var opCode = nodeCall.getOpcode();
    var queuedName = nodeCall.name + QUEUED_SUFFIX;
    var desc = changeReturnType(nodeCall.desc, NODE_DESC);
    var i = new MethodInsnNode(opCode, nodeCall.owner, queuedName, desc, nodeCall.itf);
    write(op, i, op.popCount, op.hasResult, true);
    op.localSlot = method.maxLocals + tmpVars++;
    write(op, new VarInsnNode(ASTORE, op.localSlot), 1, false, false);
    if (visibleQueuedNode && op.label == null) {
      op.label = new Label();
      addLabelNode(op.label);
    }
  }

  private void write(Op op) {
    write(op, op.insnNode, op.popCount, op.hasResult, true);
  }

  /** Allows to specify all the parts separately */
  private void write(Op op, AbstractInsnNode i, int popC, boolean hasResult, boolean orderSlots) {
    // Verify inputs are in correct slots
    if (orderSlots && popC > 0) {
      if (popC != op.inputs.size())
        throw new RuntimeException("LERROR: Invalid number of arguments");
      adjustInputsStack(op.inputs);

      for (var index = 0; index < popC; index++) {
        var input = op.inputs.get(popC - 1 - index);
        if (input.stackSlot != curBlock.stackCount - (popC - index)) {
          throw new RuntimeException("LERROR: input.stackSlot and stackCount doesn't match!");
        }
      }
    }
    adjustStack(op, popC, hasResult);
    // Add result....
    if (op instanceof Op.Phi) {
      // not an actual bytecode instruction!
    } else {
      result.add(i);
    }
  }

  private void adjustInputsStack(ArrayList<Op> inputs) {
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
      curBlock.pushStack(input);
      if (input.resultLoadOp < ILOAD || input.resultLoadOp > ALOAD) {
        throw new RuntimeException("LERROR: resultLoadOp is not a LOAD operation!");
      }
      result.add(new VarInsnNode(input.resultLoadOp, input.localSlot));
    }
  }

  private void spillStack(int count) {
    for (int i = 0; i < count; i++) {
      var op = curBlock.popStack();
      op.localSlot = method.maxLocals + tmpVars++;
      if (op.resultLoadOp < ILOAD || op.resultLoadOp > ALOAD) {
        throw new RuntimeException("LERROR: resultLoadOp is not a LOAD operation!");
      }
      var storeOp = ISTORE + op.resultLoadOp - ILOAD;
      // double and long need an extra slot to be stored!
      if (storeOp == DSTORE || storeOp == LSTORE) tmpVars++;
      result.add(new VarInsnNode(storeOp, op.localSlot));
    }
  }

  private void updateLineNumber(Op i) {
    if (i.lineNum > 0 && i.lineNum != curLineNum) {
      LabelNode labelNode;
      if (!result.isEmpty() && result.get(result.size() - 1) instanceof LabelNode) {
        labelNode = (LabelNode) result.get(result.size() - 1);
      } else labelNode = addLabelNode(new Label());
      result.add(new LineNumberNode(i.lineNum, labelNode));
      curLineNum = i.lineNum;
    }
  }

  private LabelNode addLabelNode(Label label) {
    var labelNode = registerLabel(label);
    result.add(labelNode);
    return labelNode;
  }

  private void updateLocalVariables() {
    for (var lv : method.localVariables) {
      lv.start = registerLabel(lv.start.getLabel());
      lv.end = registerLabel(lv.end.getLabel());
    }
  }

  /** Almost the same as updateLocalVariables but doesn't mutate it in case we need to bail out */
  private void registerLocalVariables() {
    for (var lv : method.localVariables) {
      registerLabel(lv.start.getLabel());
      registerLabel(lv.end.getLabel());
    }
  }

  /** Labels other than those used for line number should be registered (aka important) */
  private LabelNode registerLabel(Label label) {
    return label2node.computeIfAbsent(label, LabelNode::new);
  }

  private void releaseDependencies(Op op) {
    for (var c : op.consumers) {
      c.incomingEdges--;
      if (c.incomingEdges == 0) curBlock.availOps.add(c);
    }
  }

  private Block blockFromLabel(Label label) {
    return label == null
        ? new Block(method.maxStack, blockCount++)
        : label2block.computeIfAbsent(
            label,
            lbl -> {
              var block = new Block(method.maxStack, blockCount++);
              block.label = lbl;
              return block;
            });
  }
}
