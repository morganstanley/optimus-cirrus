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

import static optimus.debug.CommonAdapter.isStatic;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import optimus.graph.loom.CompilerArgs;
import optimus.graph.loom.LoomAdapter;
import optimus.graph.loom.TransformableMethod;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.LabelNode;
import org.objectweb.asm.tree.LocalVariableNode;
import org.objectweb.asm.tree.MethodNode;

/**
 * Data Flow Graph generated from the implementation of a method. It accumulates state that is
 * shared and manipulated by the compiler phases.
 */
class DCFlowGraph {
  private static final Op BusySlot = new Op.Param(-1, null);

  private final MethodNode method;
  final CompilerArgs compilerArgs;

  public final LoomAdapter adapter;

  /* Only named/labeled block get registered here */
  private final HashMap<Label, Block> label2block = new HashMap<>();

  /* Contains labels to retain the original life time of variables */
  private final HashMap<Label, LabelNode> label2node = new HashMap<>();

  ArrayList<Block> blocks = new ArrayList<>();
  private Block startBlock;

  private Op[] vars;

  private int varsCount;

  private int varsTempCount;

  /** Result instructions list */
  private ArrayList<AbstractInsnNode> result = new ArrayList<>();

  public DCFlowGraph(TransformableMethod tmethod, LoomAdapter adapter) {
    this.method = tmethod.method;
    this.compilerArgs = tmethod.compilerArgs;
    this.adapter = adapter;

    vars = new Op[method.maxLocals];
    if (!isStatic(method.access)) {
      vars[varsCount++] = new Op.Param(varsCount, Type.getObjectType(adapter.cls.name));
    }

    if (method.parameters != null) {
      for (var argType : tmethod.argTypes) {
        vars[varsCount++] = new Op.Param(varsCount, argType);
        if (argType.getSize() > 1) {
          vars[varsCount++] = BusySlot;
        }
      }
    }
  }

  /**
   * If a block with the given label has been seen before, return it. Otherwise create a new block
   * associated with the given label. Note that some block don't have a label (null) (e.g., an if
   * jump for the false target).
   */
  Block blockFromLabel(Label label) {
    return label == null
        ? new Block(method.maxStack, null) // new block without label
        : label2block.computeIfAbsent(label, lbl -> new Block(method.maxStack, lbl));
  }

  boolean isRegisteredLabel(Label label) {
    return label2node.containsKey(label);
  }

  /** Labels other than those used for line number should be registered (aka important) */
  LabelNode registerLabel(Label label) {
    return label2node.computeIfAbsent(label, LabelNode::new);
  }

  void addLocalVariable(LocalVariableNode lvn) {
    method.localVariables.add(lvn);
  }

  void addBusySlot() {
    addTempVar(BusySlot);
  }

  int addTempVar(Type type) {
    var op = new Op.Param(varsCount, type);
    return addTempVar(op);
  }

  int addTempVar(Op op) {
    int slot = varsTempCount + method.maxLocals;
    if (vars.length >= slot) vars = Arrays.copyOf(vars, Math.max(slot * 2, 4));
    vars[slot] = op;
    varsTempCount++;
    return slot;
  }

  Op getVar(int slot) {
    return vars[slot];
  }

  void updateVar(int slot, Op op) {
    vars[slot] = op;
    varsCount = Math.max(varsCount, slot);
  }

  void updateLocalVariables() {
    for (var lv : method.localVariables) {
      lv.start = label2node.get(lv.start.getLabel());
      lv.end = label2node.get(lv.end.getLabel());
      if (lv.start == null || lv.end == null)
        LMessage.fatal("Found a local variable without a label?");
    }
  }

  AbstractInsnNode getLastResult() {
    return result.isEmpty() ? null : result.get(result.size() - 1);
  }

  // mainly for easier debugging...
  void addToResult(AbstractInsnNode i) {
    result.add(i);
  }

  // null would mean we aborted the parsing or otherwise decided not to transform
  void giveUp() {
    result = null;
  }

  boolean isResultAvailable() {
    return result != null;
  }

  ArrayList<AbstractInsnNode> getResult() {
    return result;
  }

  void tryToAssignStartBlock(Block block) {
    if (startBlock == null) startBlock = block;
  }

  Block getStartBlock() {
    return startBlock;
  }

  void registerBlock(Block block) {
    block.blockID = blocks.size();
    blocks.add(block);
  }

  public Type[] outVarTypes() {
    var r = new Type[vars.length];
    for (int i = 0; i < vars.length; i++) {
      r[i] = vars[i] == null ? null : vars[i].resultType;
    }
    return r;
  }
}
