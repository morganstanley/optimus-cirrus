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
package optimus.debug;

import static optimus.debug.InstrumentationConfig.castRelatedExclusions;
import static optimus.debug.InstrumentationConfig.checkCastFilter;
import static optimus.debug.InstrumentationConfig.instanceOfFilter;
import optimus.debug.InstrumentationConfig.MethodRef;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Small helper visitors for rare instrumentation needs. Should move to a different file/class once
 * the usage is understood
 */
public class InstrumentationByteCodeVisitors implements Opcodes {
  private static final String HELPER_CLS = "optimus/debug/InstrumentedByteCodes";
  private static final String OOZ = "(Ljava/lang/Object;Ljava/lang/Object;)Z";

  /**
   * Should apply forwarding ?
   *
   * @param opcode CHECKCAST vs INSTANCEOF
   * @param castToClass class name being checked for by the opcode
   * @param calleeMethodRef current method being processed to be checked for the exclusion
   * @return method reference to point the casting checks to
   */
  static MethodRef mrOfCast(int opcode, String castToClass, MethodRef calleeMethodRef) {
    if (castRelatedExclusions.containsKey(calleeMethodRef)) return null;

    if (opcode == CHECKCAST) {
      if (checkCastFilter.isEmpty()) return InstrumentationConfig.defCheckCast;
      return checkCastFilter.get(castToClass);
    } else {
      if (instanceOfFilter.isEmpty()) return InstrumentationConfig.defInstanceOf;
      return instanceOfFilter.get(castToClass);
    }
  }

  public static class CV extends ClassVisitor {
    private final boolean doubleCompares;
    private final boolean refCompares;
    private final boolean castChecks;
    public boolean needTransform; // True if found the instruction that was searched

    String name;
    String source = "unknown";

    public CV(
        ClassVisitor classVisitor,
        boolean instrumentForNaNCompare,
        boolean enableEqTracing,
        boolean enableCastChecks) {
      super(ASM9, classVisitor);
      this.doubleCompares = instrumentForNaNCompare;
      this.refCompares = enableEqTracing;
      this.castChecks = enableCastChecks;
    }

    @Override
    public void visit(
        int version,
        int access,
        String name,
        String signature,
        String superName,
        String[] interfaces) {
      this.name = name;
      super.visit(version, access, name, signature, superName, interfaces);
    }

    @Override
    public void visitSource(String source, String debug) {
      this.source = source;
      super.visitSource(source, debug);
    }

    @Override
    public MethodVisitor visitMethod(
        int access, String name, String descriptor, String signature, String[] exceptions) {
      var methodVisitor = super.visitMethod(access, name, descriptor, signature, exceptions);
      return new MV(this, methodVisitor, name);
    }
  }

  public static class MV extends MethodVisitor {
    private final MethodRef curMethodRef;
    private final String methodName;
    private final CV cv;
    private boolean updatedMethod;
    private int addedStacks;
    private int line; // Last line number seen

    public MV(CV cv, MethodVisitor methodVisitor, String methodName) {
      super(Opcodes.ASM9, methodVisitor);
      this.cv = cv;
      this.methodName = methodName;
      this.curMethodRef = new MethodRef(cv.name, methodName);
    }

    @Override
    public void visitJumpInsn(int opcode, Label label) {
      if (cv.refCompares && opcode >= Opcodes.IF_ACMPEQ && opcode <= Opcodes.IF_ACMPNE) {
        cv.needTransform = true;
        if (mv != null) {
          if (opcode == IF_ACMPEQ) {
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, HELPER_CLS, "acmpeq", OOZ, false);
            mv.visitJumpInsn(Opcodes.IFNE, label);
          } else {
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, HELPER_CLS, "acmpne", OOZ, false);
            mv.visitJumpInsn(Opcodes.IFNE, label);
          }

          if (!updatedMethod) {
            updatedMethod = true;
            System.err.println("INFO: Tracking cmpeq/ne in : " + cv.name + "." + methodName);
          }
          // instead of an instruction we generate the method call
          // notice the stack for a call matches that of the instruction
          return;
        }
        updatedMethod = true;
      }
      super.visitJumpInsn(opcode, label);
    }

    @Override
    public void visitLineNumber(int line, Label start) {
      this.line = line;
      super.visitLineNumber(line, start);
    }

    @Override
    public void visitTypeInsn(int opcode, String type) {
      if (cv.castChecks && (opcode == CHECKCAST || opcode == INSTANCEOF)) {
        var mr = mrOfCast(opcode, type, curMethodRef);
        cv.needTransform = mr != null;
        if (mv != null && mr != null) {
          addedStacks = Math.max(addedStacks, 2); // for the extra args
          mv.visitInsn(DUP);
          mv.visitLdcInsn(type);
          mv.visitMethodInsn(Opcodes.INVOKESTATIC, mr.cls, mr.method, mr.descriptor, false);
          if (!updatedMethod) {
            updatedMethod = true;
            System.err.println(
                "INFO: Tracking casts in "
                    + cv.name.replace("/", ".")
                    + "."
                    + methodName
                    + " ("
                    + cv.source
                    + ":"
                    + line
                    + ") "
                    + mr.method
                    + "("
                    + type
                    + ")");
          }
        }
        updatedMethod = true;
      }
      super.visitTypeInsn(opcode, type);
    }

    @Override
    public void visitInsn(int opcode) {
      if (cv.doubleCompares && opcode >= Opcodes.DCMPL && opcode <= Opcodes.DCMPG) {
        cv.needTransform = true;
        if (mv != null) {
          if (opcode == DCMPL)
            mv.visitMethodInsn(Opcodes.INVOKESTATIC, HELPER_CLS, "dcmpl", "(DD)I", false);
          else mv.visitMethodInsn(Opcodes.INVOKESTATIC, HELPER_CLS, "dcmpg", "(DD)I", false);

          if (!updatedMethod) {
            updatedMethod = true;
            System.err.println("INFO: Tracking double compares in : " + cv.name + "." + methodName);
          }
          // instead of an instruction we generate the method call
          // notice the stack for a call matches that of the instruction
          return;
        }
        updatedMethod = true;
      }
      super.visitInsn(opcode);
    }

    @Override
    public void visitMaxs(int maxStack, int maxLocals) {
      super.visitMaxs(maxStack + addedStacks, maxLocals);
    }
  }
}
