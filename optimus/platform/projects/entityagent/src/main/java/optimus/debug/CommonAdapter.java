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

import static optimus.debug.InstrumentationConfig.FieldRef;
import static optimus.debug.InstrumentationConfig.MethodPatch;
import static optimus.debug.InstrumentationConfig.OBJECT_ARR_DESC;
import static optimus.debug.InstrumentationConfig.OBJECT_DESC;
import static optimus.debug.InstrumentationConfig.allocateID;
import java.util.Arrays;
import java.util.List;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.Method;

/** A collection of useful ASM helper functions built on top of AdviceAdapter */
public class CommonAdapter extends AdviceAdapter implements AutoCloseable {
  private int methodID; // If allocation requested
  private boolean thisIsAvailable;

  private final Label __localValueStart = new Label();
  private final Label __localValueEnd = new Label();
  private int __localValue; // When local passing is enabled this will point to a slot for local var
  private Type localValueType;
  private String localValueDesc;

  public static Type[] asTypes(Class<?>[] parameterArray) {
    var types = new Type[parameterArray.length];
    for (int i = 0; i < parameterArray.length; i++) types[i] = Type.getType(parameterArray[i]);
    return types;
  }

  protected CommonAdapter(MethodVisitor methodVisitor, int access, String name, String descriptor) {
    super(ASM9, methodVisitor, access, name, descriptor);
  }

  /** To gain access to convenience functions here we need to wrap the original MethodVisitor */
  public static CommonAdapter wrap(MethodVisitor omv, int access, String name, String desc) {
    return omv instanceof CommonAdapter
        ? (CommonAdapter) omv
        : new CommonAdapter(omv, access, name, desc);
  }

  /** visitMethod and wraps in CommonAdapter */
  public static CommonAdapter newMethod(ClassVisitor cv, int access, String name, String desc) {
    return newMethod(cv, access, name, desc, null, null);
  }

  /** visitMethod and wraps in CommonAdapter */
  public static CommonAdapter newMethod(
      ClassVisitor cv,
      int access,
      String name,
      String desc,
      String signature,
      String[] exceptions) {
    var mw = cv.visitMethod(access, name, desc, signature, exceptions);
    var mv = new CommonAdapter(mw, access, name, desc);
    mv.visitCode();
    return mv;
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
    if (opcode == RETURN) {
      // if you're boxing arguments then you're presumably being general about which method you're
      // patching, but if you're not then you are patching a specific signature, in which case
      // why would you want to be passed a void return value?
      if (!boxValueTypes)
        throw new IllegalArgumentException(
            "It doesn't make sense to pass a known void return value as an argument to a patch!");
      visitInsn(ACONST_NULL);
    } else if (opcode == ARETURN || opcode == ATHROW) dup();
    else {
      if (opcode == LRETURN || opcode == DRETURN) dup2(); // double/long take two slots
      else dup();
      if (boxValueTypes) valueOf(Type.getReturnType(this.methodDesc));
    }
    // n.b. non-primitives will be passed as Object when boxing is enabled, since you're presumably
    // trying to use a signature-agnostic patch method.
    if (boxValueTypes) return OBJECT_DESC;
    else return Type.getReturnType(this.methodDesc).getDescriptor();
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

  public void loadArgsWithCast(Class<?>[] expectedArgClasses) {
    if (expectedArgClasses.length == 0) return;
    loadArgsWithCast(asTypes(expectedArgClasses));
  }

  public void loadArgsWithCast(Type... expectedArgTypes) {
    if (expectedArgTypes.length == 0) return;
    var argTypes = getArgumentTypes();
    if (expectedArgTypes.length != argTypes.length)
      throw new RuntimeException("Expected and actual arg types don't match!");
    for (int i = 0; i < argTypes.length; i++) {
      loadArg(i);
      if (!expectedArgTypes[i].equals(argTypes[i])) {
        unbox(expectedArgTypes[i]);
      }
    }
  }

  protected void loadThisIfNonStatic() {
    // cannot load this for static methods!
    if (!isStatic(methodAccess)) loadThis();
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

  public void getThisField(String className, String fieldName, Type fieldType) {
    loadThis();
    visitFieldInsn(GETFIELD, className, fieldName, fieldType.getDescriptor());
  }

  /** Almost the same as valueOf() call by VOID gets converted BoxedUnit.UNIT */
  public void valueOfForScala(Type type) {
    if (type == Type.VOID_TYPE)
      visitFieldInsn(GETSTATIC, "scala/runtime/BoxedUnit", "UNIT", "Lscala/runtime/BoxedUnit;");
    else super.valueOf(type);
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

  public void invokeInitCtor(Type type, String desc) {
    invokeConstructor(type, new Method("<init>", desc));
  }

  public static int makePrivate(int access) {
    return access & ~ACC_PUBLIC & ~ACC_PROTECTED | ACC_PRIVATE;
  }

  public static int makePublic(int access) {
    return access & ~ACC_PROTECTED & ~ACC_PRIVATE | ACC_PUBLIC;
  }

  public static int invokeStaticOrVirtual(int access) {
    return isStatic(access) ? INVOKESTATIC : INVOKEVIRTUAL;
  }

  public static Boolean isStatic(int access) {
    return (access & ACC_STATIC) != 0;
  }

  public static Boolean isCCtor(int access, String name, String desc) {
    return (access & ACC_STATIC) == ACC_STATIC && "<clinit>".equals(name) && "()V".equals(desc);
  }

  public static Boolean isInterface(int access) {
    return (access & ACC_INTERFACE) != 0;
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

  /** Matched arguments of 2 descriptors without allocating anything */
  public static boolean sameArguments(String desc1, String desc2) {
    var minLen = Math.min(desc1.length(), desc2.length());
    for (int i = 0; i < minLen; i++) {
      var ch1 = desc1.charAt(i);
      var ch2 = desc2.charAt(i);
      if (ch1 != ch2) return false;
      if (ch1 == ')') return true;
    }
    return false;
  }

  public static String changeReturnType(String desc, String returnType) {
    var lastBracket = desc.lastIndexOf(')');
    if (lastBracket > 0) return desc.substring(0, lastBracket + 1) + returnType;
    return desc;
  }

  public static String getReturnDesc(String methodDesc) {
    var lastBracket = methodDesc.lastIndexOf(')');
    if (lastBracket > 0) return methodDesc.substring(lastBracket + 1);
    return methodDesc;
  }

  /** Strip L and ; */
  public static String descToClass(String returnType) {
    if (returnType.startsWith("L") && returnType.endsWith(";"))
      return returnType.substring(1, returnType.length() - 1);
    else return returnType;
  }

  public static Handle dupNamed(Handle h, String name) {
    return new Handle(h.getTag(), h.getOwner(), name, h.getDesc(), h.isInterface());
  }

  public static String asJavaName(String bytecodeName) {
    return bytecodeName.replace('/', '.');
  }

  @Override
  public void close() {
    visitMaxs(0, 0);
    visitEnd();
  }
}
