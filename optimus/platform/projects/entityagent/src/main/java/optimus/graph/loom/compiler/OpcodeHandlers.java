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
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;

class OpcodeHandlers implements Opcodes {
  static final Handler[] opcodes = new Handler[255];

  static void fill(int start, int end, Handler op) {
    for (int i = start; i <= end; i++) opcodes[i] = op;
  }

  static {
    Handler unknown =
        (c, i) -> {
          throw new RuntimeException("LERROR: Unsupported bytecode: " + asString(i));
        };
    fill(0, opcodes.length - 1, unknown);

    var oneTrue = new Trivial(1, true);
    var oneFalse = new Trivial(1, false);
    var threeFalse = new Trivial(3, false);

    fill(ICONST_M1, ICONST_5, new Trivial(0, true, -ILOAD));
    fill(LCONST_0, LCONST_1, new Trivial(0, true, -LLOAD));
    fill(FCONST_0, FCONST_2, new Trivial(0, true, -FLOAD));
    fill(DCONST_0, DCONST_1, new Trivial(0, true, -DLOAD));
    fill(BIPUSH, SIPUSH, new Trivial(0, true, -ILOAD));

    for (var i = IASTORE; i <= SASTORE; i++) opcodes[i] = threeFalse;

    fill(IADD, DADD, new Trivial(2, true, IADD));
    fill(ISUB, DSUB, new Trivial(2, true, ISUB));
    fill(IMUL, DMUL, new Trivial(2, true, IMUL));
    fill(IDIV, DDIV, new Trivial(2, true, IDIV));
    fill(IREM, DREM, new Trivial(2, true, IREM));
    fill(INEG, DNEG, new Trivial(1, true, INEG));

    fill(ISHL, LSHR, new Trivial(2, true, ISHL));
    fill(IUSHR, LUSHR, new Trivial(2, true, IUSHR));
    fill(IAND, LAND, new Trivial(2, true, IAND));
    fill(IOR, LOR, new Trivial(2, true, IOR));
    fill(IXOR, LXOR, new Trivial(2, true, IXOR));

    opcodes[POP] = oneFalse;
    opcodes[POP2] = LCompiler::handlePop2;

    var zeroTrueALOAD = new Trivial(0, true, -ALOAD);
    var oneTrueALOAD = new Trivial(1, true, -ALOAD);
    var zeroTrueDynamic = new Dynamic(0, true);
    var oneFalseDynamic = new Dynamic(1, false);

    opcodes[ACONST_NULL] = zeroTrueALOAD;
    opcodes[CHECKCAST] = oneTrueALOAD;

    opcodes[GETSTATIC] = zeroTrueDynamic;
    opcodes[PUTSTATIC] = oneFalseDynamic;
    opcodes[GETFIELD] = zeroTrueDynamic;
    opcodes[PUTFIELD] = oneFalseDynamic;
    opcodes[LDC] = zeroTrueDynamic;

    opcodes[NEWARRAY] = oneTrue;
    opcodes[ANEWARRAY] = oneTrue;
    opcodes[INSTANCEOF] = oneTrueALOAD;
    opcodes[NEW] = zeroTrueALOAD;

    opcodes[LCMP] = new Trivial(2, true, -LLOAD);

    fill(ILOAD, ALOAD, LCompiler::loadVar);
    fill(ISTORE, ASTORE, LCompiler::storeVal);

    Handler oneToLong = new Trivial(1, true, -LLOAD);
    Handler oneToFloat = new Trivial(1, true, -FLOAD);
    Handler oneToDouble = new Trivial(1, true, -DLOAD);
    Handler oneToInt = new Trivial(1, true, -ILOAD);
    opcodes[I2L] = oneToLong;
    opcodes[I2F] = oneToFloat;
    opcodes[I2D] = oneToDouble;
    opcodes[L2I] = oneToInt;
    opcodes[L2F] = oneToFloat;
    opcodes[L2D] = oneToDouble;
    opcodes[F2I] = oneToInt;
    opcodes[F2L] = oneToLong;
    opcodes[F2D] = oneToDouble;
    opcodes[D2I] = oneToInt;
    opcodes[D2L] = oneToLong;
    opcodes[D2F] = oneToFloat;
    opcodes[I2B] = oneToInt;
    opcodes[I2C] = oneToInt;
    opcodes[I2S] = oneToInt;

    opcodes[DUP] = LCompiler::handleDup;

    var condJumpOne = new ConditionalJump(1);
    fill(IFEQ, IFLE, condJumpOne);
    fill(IF_ICMPEQ, IF_ACMPNE, new ConditionalJump(2));
    fill(IFNULL, IFNONNULL, condJumpOne);

    opcodes[GOTO] = LCompiler::handleJump;

    Handler returnsOne = handleReturn(1);
    fill(IRETURN, ARETURN, returnsOne);
    opcodes[RETURN] = handleReturn(0); // we don't pop with return!
    opcodes[ATHROW] = returnsOne;

    fill(INVOKEVIRTUAL, INVOKEINTERFACE, (c, i) -> c.handleInvoke(((MethodInsnNode) i).desc, i));
    opcodes[INVOKEDYNAMIC] = (c, i) -> c.handleInvoke(((InvokeDynamicInsnNode) i).desc, i);
  }

  private static Handler handleReturn(int popCount) {
    return (c, i) -> {
      c.handleFrameOp(i, popCount);
      c.assertEmptyStack();
    };
  }

  interface Handler {
    void handle(LCompiler compiler, AbstractInsnNode i);
  }

  private static class Trivial implements Handler {
    final int loadOpBase; // Result load Op
    int popCount;
    boolean hasResult;

    public Trivial(int popCount, boolean hasResult, int loadOpBase) {
      this.popCount = popCount;
      this.hasResult = hasResult;
      this.loadOpBase = loadOpBase;
    }

    public Trivial(int popCount, boolean hasResult) {
      this(popCount, hasResult, -1);
    }

    public void handle(LCompiler compiler, AbstractInsnNode i) {
      var loadOp = loadOpBase < 0 ? -loadOpBase : ILOAD + i.getOpcode() - loadOpBase;
      compiler.handleOp(i, popCount, hasResult, loadOp);
    }
  }

  private static class Dynamic implements Handler {
    int popCount;
    boolean hasResult;

    public Dynamic(int popCount, boolean hasResult) {
      this.popCount = popCount;
      this.hasResult = hasResult;
    }

    @Override
    public void handle(LCompiler compiler, AbstractInsnNode i) {
      var type = getType(i);
      var loadOp = type != null ? type.getOpcode(ILOAD) : -1;
      compiler.handleOp(i, popCount, hasResult, loadOp);
    }

    private Type getType(AbstractInsnNode i) {
      Type type = null;
      if (i instanceof FieldInsnNode) {
        var fi = (FieldInsnNode) i;
        type = Type.getObjectType(fi.desc);
      } else if (i instanceof LdcInsnNode) {
        var ldci = (LdcInsnNode) i;
        var clazz = ldci.cst.getClass();
        if (clazz == Integer.class) type = Type.INT_TYPE;
        else if (clazz == Long.class) type = Type.LONG_TYPE;
        else if (clazz == Float.class) type = Type.FLOAT_TYPE;
        else if (clazz == Double.class) type = Type.DOUBLE_TYPE;
        else type = Type.getType(ldci.cst.getClass());
      }
      return type;
    }
  }

  private static class ConditionalJump implements Handler {
    int popCount;

    public ConditionalJump(int popCount) {
      this.popCount = popCount;
    }

    @Override
    public void handle(LCompiler compiler, AbstractInsnNode i) {
      compiler.handleConditionalJump(popCount, i);
    }
  }
}
