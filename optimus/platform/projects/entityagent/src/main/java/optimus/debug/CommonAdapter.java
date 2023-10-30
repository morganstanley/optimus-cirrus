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

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;
import static optimus.debug.InstrumentationConfig.MethodPatch;
import static optimus.debug.InstrumentationConfig.FieldRef;
import static optimus.debug.InstrumentationConfig.OBJECT_ARR_DESC;
import static optimus.debug.InstrumentationConfig.OBJECT_DESC;
import static optimus.debug.InstrumentationConfig.allocateID;
import java.util.Arrays;
import java.util.List;

/** A collection of useful ASM helper functions built on top of AdviceAdapter */
public class CommonAdapter extends AdviceAdapter {
  private int methodID; // If allocation requested
  private boolean thisIsAvailable;

  private final Label __localValueStart = new Label();
  private final Label __localValueEnd = new Label();
  private int __localValue; // When local passing is enabled this will point to a slot for local var
  private Type localValueType;
  private String localValueDesc;

  protected CommonAdapter(MethodVisitor methodVisitor, int access, String name, String descriptor) {
    super(ASM9, methodVisitor, access, name, descriptor);
  }

  /** To gain access to convenience functions here we need to wrap the original MethodVisitor */
  public static CommonAdapter wrap(MethodVisitor omv, int access, String name, String desc) {
    return omv instanceof CommonAdapter
        ? (CommonAdapter) omv
        : new CommonAdapter(omv, access, name, desc);
  }

  public void resetMV(MethodVisitor newMethodVisitor) {
    if (mv == null) mv = newMethodVisitor;
    else if (mv instanceof CommonAdapter) ((CommonAdapter) mv).resetMV(newMethodVisitor);
  }

  @Override
  protected void onMethodEnter() {
    // Consider adding code to report 'this' in the case of constructor
    thisIsAvailable = true;
  }

  protected void setLocalValueTypeAndDesc(Type tpe) {
    localValueType = tpe;
    localValueDesc = tpe.getDescriptor();
  }

  protected void visitLocalValueStart() {
    visitLabel(__localValueStart);
    __localValue = newLocal(localValueType);
  }

  protected void visitLocalValueEnd() {
    visitLabel(__localValueEnd);
    mv.visitLocalVariable(
        "__locValue", localValueDesc, null, __localValueStart, __localValueEnd, __localValue);
  }

  protected String loadLocalValueIfRequested(MethodPatch patch) {
    return patch.passLocalValue ? loadLocalValue() : "";
  }

  protected String loadLocalValue() {
    mv.visitVarInsn(localValueType.getOpcode(ILOAD), __localValue);
    return localValueType.getDescriptor();
  }

  protected void storeLocalValue() {
    mv.visitVarInsn(localValueType.getOpcode(ISTORE), __localValue);
  }

  protected void ifNotZeroReturn(MethodPatch patch, FieldRef fpatch) {
    loadThis();
    mv.visitFieldInsn(GETFIELD, patch.from.cls, fpatch.name, fpatch.type);
    Label label1 = new Label();
    mv.visitJumpInsn(IFEQ, label1);
    loadThis();
    mv.visitFieldInsn(GETFIELD, patch.from.cls, fpatch.name, fpatch.type);
    mv.visitInsn(IRETURN);
    mv.visitLabel(label1);
  }

  protected String loadMethodID(MethodPatch patch) {
    if (methodID == 0) methodID = allocateID(patch.from);
    mv.visitIntInsn(SIPUSH, methodID);
    return "I";
  }

  protected String loadThisOrNull() {
    if ((methodAccess & ACC_STATIC) != 0 || (!thisIsAvailable && getName().equals("<init>")))
      mv.visitInsn(ACONST_NULL); // static will just pass null and ctor will temporarily pass null
    else loadThis();
    return OBJECT_DESC;
  }

  protected String dupReturnValueOrNullForVoid(int opcode, boolean boxValueTypes) {
    if (opcode == RETURN) visitInsn(ACONST_NULL);
    else if (opcode == ARETURN || opcode == ATHROW) dup();
    else {
      if (opcode == LRETURN || opcode == DRETURN) dup2(); // double/long take two slots
      else dup();
      if (boxValueTypes) valueOf(Type.getReturnType(this.methodDesc));
    }
    return OBJECT_DESC; // TODO (OPTIMUS-53248): For non-boxing case return proper descriptor
  }

  protected String loadArgsInlineOrAsArray(MethodPatch patch) {
    if (patch.noArgumentBoxing) {
      loadArgs();
      return ""; // Rely on descriptor explicitly supplied
    } else {
      loadArgArray();
      return OBJECT_ARR_DESC;
    }
  }

  protected void loadThisIfNonStatic() {
    // cannot load this for static methods!
    if (!hasStaticAccess(methodAccess)) loadThis();
  }

  void writeCallForward(String className, String name, int access, String desc) {
    visitCode();
    // call original native method
    loadThisIfNonStatic();
    loadArgs();

    var opcode = invokeStaticOrVirtual(access);
    visitMethodInsn(opcode, className, name, desc, false);
    var returnType = Type.getReturnType(desc);
    visitInsn(returnType.getOpcode(Opcodes.IRETURN));
    visitMaxs(0, 0);
    visitEnd();
  }

  protected void dup(Type tpe) {
    var size = tpe.getSize();
    if (size == 0) {
      // cannot DUP the void type!
      mv.visitInsn(Opcodes.NOP);
    } else if (size == 1) {
      dup();
    } else if (size == 2) {
      dup2();
    } else throw new AssertionError();
  }

  public static int invokeStaticOrVirtual(int access) {
    return hasStaticAccess(access) ? INVOKESTATIC : INVOKEVIRTUAL;
  }

  public static Boolean hasStaticAccess(int access) {
    return (access & ACC_STATIC) != 0;
  }

  public static Boolean isCCtor(int access, String name, String desc) {
    return (access & ACC_STATIC) == ACC_STATIC && "<clinit>".equals(name) && "()V".equals(desc);
  }

  private static final List<String> packagesToIgnore =
      Arrays.asList("scala/reflect", "sun/", "java/security");

  /**
   * Identifies JVM/Scala/Sun owned classes, such as ProfilerEventsWriter, as they should be
   * excluded from the transformation
   */
  public static Boolean isThirdPartyOwned(ClassLoader loader, String className) {
    if (loader == null) return true;

    for (String pkg : packagesToIgnore) {
      if (className.startsWith(pkg)) return true;
    }

    return false;
  }
}
