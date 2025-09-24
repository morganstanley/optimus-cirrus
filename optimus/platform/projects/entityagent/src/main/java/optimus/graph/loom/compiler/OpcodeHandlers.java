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

import static optimus.debug.InstrumentationConfig.OBJECT_TYPE;
import static optimus.graph.loom.LoomConfig.*;
import static optimus.graph.loom.compiler.LInsnDiGraphWriter.asString;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.FieldInsnNode;
import org.objectweb.asm.tree.IntInsnNode;
import org.objectweb.asm.tree.InvokeDynamicInsnNode;
import org.objectweb.asm.tree.LdcInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.TypeInsnNode;

/** Defines how to convert a bytecode instruction into an operation. */
class OpcodeHandlers implements Opcodes {
  static final Handler[] opcodes = new Handler[255];
  static final Type[] typeAsOffsets =
      new Type[] {
        Type.INT_TYPE,
        Type.LONG_TYPE,
        Type.FLOAT_TYPE,
        Type.DOUBLE_TYPE,
        OBJECT_TYPE,
        Type.BOOLEAN_TYPE,
        Type.CHAR_TYPE,
        Type.SHORT_TYPE
      };

  static void fill(int start, int end, Handler op) {
    for (int i = start; i <= end; i++) opcodes[i] = op;
  }

  static {
    Handler unknown = (c, i) -> LMessage.fatal("Unsupported bytecode: " + asString(i));
    fill(0, opcodes.length - 1, unknown);

    var oneFalse = new Trivial(1);

    fill(ICONST_M1, ICONST_5, new Trivial(0, Type.INT_TYPE));
    fill(LCONST_0, LCONST_1, new Trivial(0, Type.LONG_TYPE));
    fill(FCONST_0, FCONST_2, new Trivial(0, Type.FLOAT_TYPE));
    fill(DCONST_0, DCONST_1, new Trivial(0, Type.DOUBLE_TYPE));
    fill(BIPUSH, SIPUSH, new Trivial(0, Type.INT_TYPE));

    fill(IALOAD, SALOAD, new Primitive(2, IALOAD));
    // this overwrites AALOAD assigned above in the middle of the fill
    opcodes[AALOAD] = Parser::handleObjectArrayLoad;

    fill(IASTORE, SASTORE, Parser::handleArrayStore);

    fill(IADD, DADD, new Primitive(2, IADD));
    fill(ISUB, DSUB, new Primitive(2, ISUB));
    fill(IMUL, DMUL, new Primitive(2, IMUL));
    fill(IDIV, DDIV, new Primitive(2, IDIV));
    fill(IREM, DREM, new Primitive(2, IREM));
    fill(INEG, DNEG, new Primitive(1, INEG));

    fill(ISHL, LSHR, new Primitive(2, ISHL));
    fill(IUSHR, LUSHR, new Primitive(2, IUSHR));
    fill(IAND, LAND, new Primitive(2, IAND));
    fill(IOR, LOR, new Primitive(2, IOR));
    fill(IXOR, LXOR, new Primitive(2, IXOR));

    opcodes[IINC] = Parser::incVar;

    opcodes[POP] = oneFalse;
    opcodes[POP2] = Parser::handlePop2;

    var zeroTrueDynamic = new Dynamic(0, true);
    var oneTrueDynamic = new Dynamic(1, true);
    var oneFalseDynamic = new Dynamic(1, false);

    opcodes[ACONST_NULL] = new Trivial(0, OBJECT_TYPE);
    opcodes[CHECKCAST] = oneTrueDynamic;

    opcodes[GETSTATIC] = zeroTrueDynamic;
    opcodes[PUTSTATIC] = oneFalseDynamic;
    opcodes[GETFIELD] = oneTrueDynamic;
    opcodes[PUTFIELD] = oneFalseDynamic;
    opcodes[LDC] = zeroTrueDynamic;

    var oneToInt = new Trivial(1, Type.INT_TYPE);
    opcodes[NEWARRAY] = oneTrueDynamic;
    opcodes[ANEWARRAY] = oneTrueDynamic;
    opcodes[ARRAYLENGTH] = oneToInt;
    opcodes[INSTANCEOF] = oneToInt;
    opcodes[NEW] = zeroTrueDynamic;

    fill(ILOAD, ALOAD, Parser::loadVar);
    fill(ISTORE, ASTORE, Parser::storeVar);

    Handler oneToLong = new Trivial(1, Type.LONG_TYPE);
    Handler oneToFloat = new Trivial(1, Type.FLOAT_TYPE);
    Handler oneToDouble = new Trivial(1, Type.DOUBLE_TYPE);
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

    opcodes[DUP] = Parser::handleDup;

    fill(LCMP, DCMPG, new Trivial(2, Type.INT_TYPE));

    var condJumpOne = new ConditionalJump(1);
    fill(IFEQ, IFLE, condJumpOne);
    fill(IF_ICMPEQ, IF_ACMPNE, new ConditionalJump(2));
    fill(IFNULL, IFNONNULL, condJumpOne);

    opcodes[GOTO] = Parser::handleUnconditionalJump;
    opcodes[TABLESWITCH] = Parser::handleTableSwitch;
    opcodes[LOOKUPSWITCH] = Parser::handleLookupSwitch;

    fill(IRETURN, ARETURN, Parser::handleReturnValue);
    opcodes[RETURN] = Parser::handleReturnVoid; // we don't pop with return!
    opcodes[ATHROW] = Parser::handleThrow;

    fill(INVOKEVIRTUAL, INVOKEINTERFACE, (p, i) -> p.handleInvoke(((MethodInsnNode) i).desc, i));
    opcodes[INVOKEDYNAMIC] = (p, i) -> p.handleInvoke(((InvokeDynamicInsnNode) i).desc, i);
  }

  interface Handler {
    void handle(Parser parser, AbstractInsnNode i);
  }

  private static class Trivial implements Handler {
    final Type resultType;
    int popCount;

    public Trivial(int popCount, Type resultType) {
      this.popCount = popCount;
      this.resultType = resultType;
    }

    public Trivial(int popCount) {
      this(popCount, null);
    }

    public void handle(Parser parser, AbstractInsnNode i) {
      parser.handleOp(i, popCount, resultType != null, resultType);
    }
  }

  private static class Primitive implements Handler {
    int popCount;
    int baseOpcode;

    public Primitive(int popCount, int baseOpcode) {
      this.popCount = popCount;
      this.baseOpcode = baseOpcode;
    }

    public void handle(Parser parser, AbstractInsnNode i) {
      var tpe = typeAsOffsets[i.getOpcode() - baseOpcode];
      parser.handleOp(i, popCount, true, tpe);
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
    public void handle(Parser parser, AbstractInsnNode i) {
      var resultType = hasResult ? getType(i) : null;
      parser.handleOp(i, popCount, hasResult, resultType);
    }

    private Type getType(AbstractInsnNode i) {
      if (i instanceof FieldInsnNode) {
        var fi = (FieldInsnNode) i;
        return Type.getType(fi.desc);
      }

      if (i instanceof LdcInsnNode) {
        var ldci = (LdcInsnNode) i;
        var clazz = ldci.cst.getClass();
        if (clazz == Integer.class) return Type.INT_TYPE;
        if (clazz == Long.class) return Type.LONG_TYPE;
        if (clazz == Float.class) return Type.FLOAT_TYPE;
        if (clazz == Double.class) return Type.DOUBLE_TYPE;
        return Type.getType(ldci.cst.getClass());
      }

      if (i instanceof TypeInsnNode) {
        var ti = (TypeInsnNode) i;
        if (i.getOpcode() == ANEWARRAY) return Type.getObjectType("[L" + ti.desc + ";");
        return Type.getObjectType(ti.desc);
      }

      if (i instanceof IntInsnNode) { // specific for NEWARRAY
        // this should match what is in org.objectweb.asm.util.Printer#TYPES !
        var operand = ((IntInsnNode) i).operand;
        if (operand == 4) return ARRAY_BOOLEAN_TYPE;
        if (operand == 5) return ARRAY_CHAR_TYPE;
        if (operand == 6) return ARRAY_FLOAT_TYPE;
        if (operand == 7) return ARRAY_DOUBLE_TYPE;
        if (operand == 8) return ARRAY_BYTE_TYPE;
        if (operand == 9) return ARRAY_SHORT_TYPE;
        if (operand == 10) return ARRAY_INT_TYPE;
        if (operand == 11) return ARRAY_LONG_TYPE;
        LMessage.fatal("unknown operand " + operand + " for " + asString(i));
        return null;
      }

      LMessage.fatal("Cannot get type for instruction " + i.getClass());
      return null;
    }
  }

  private static class ConditionalJump implements Handler {
    int popCount;

    public ConditionalJump(int popCount) {
      this.popCount = popCount;
    }

    @Override
    public void handle(Parser parser, AbstractInsnNode i) {
      parser.handleConditionalJump(popCount, i);
    }
  }
}
