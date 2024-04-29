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
package optimus.graph.rtverifier;

import java.util.concurrent.atomic.AtomicInteger;

import static optimus.debug.InstrumentationConfig.IS;
import static optimus.debug.InstrumentationConfig.CWA_INNER_NAME;
import static optimus.debug.InstrumentationConfig.CWA;
import static optimus.debug.InstrumentationConfig.OBJECT_ARR_DESC;
import static optimus.debug.InstrumentationConfig.OBJECT_CLS_NAME;
import static optimus.debug.InstrumentationConfig.OBJECT_TYPE;
import static optimus.debug.InstrumentationConfig.STRING_DESC;

import optimus.debug.CommonAdapter;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

public class CallWithArgsGenerator implements Opcodes {

  private static final AtomicInteger counter = new AtomicInteger(0);

  public static String generateClassName(String methodName) {
    return String.join("_", "optimus/CWA", methodName, Integer.toString(counter.getAndIncrement()));
  }

  public static byte[] create(
      int access,
      String className,
      Type originalOwner,
      Type[] args,
      Type returnType,
      String originalMethod) {
    ClassWriter cw =
        createClassWriter(access, className, originalOwner, args, returnType, originalMethod);
    cw.visitEnd();
    return cw.toByteArray();
  }

  private static ClassWriter createClassWriter(
      int access,
      String className,
      Type originalOwner,
      Type[] args,
      Type returnType,
      String originalMethod) {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    cw.visit(V11, ACC_PUBLIC | ACC_SUPER, className, null, CWA, null);
    cw.visitInnerClass(CWA, IS, CWA_INNER_NAME, ACC_PUBLIC | ACC_STATIC | ACC_ABSTRACT);

    Type cwaClass = Type.getObjectType(className);

    generateFields(cw, access, originalOwner, args);
    generateCtor(cw, cwaClass, access, originalOwner, args);
    generateArgsMethod(cw, cwaClass, args);
    generateReApplyMethod(cw, cwaClass, access, originalOwner, args, returnType, originalMethod);
    generateFullNameMethod(cw, originalOwner, originalMethod);
    return cw;
  }

  private static void generateFields(ClassWriter cw, int access, Type originalOwner, Type[] args) {
    // We generate the field original to hold the instance of the class only if the method is
    // non-static
    if (!CommonAdapter.isStatic(access)) generatePrivateField(cw, "original", originalOwner);

    for (int i = 0; i < args.length; i++) {
      generatePrivateField(cw, "arg" + i, args[i]);
    }
  }

  private static void generatePrivateField(ClassWriter cw, String name, Type tpe) {
    FieldVisitor fv = cw.visitField(ACC_PRIVATE, name, tpe.getDescriptor(), null, null);
    fv.visitEnd();
  }

  private static void generateCtor(
      ClassWriter cw, Type cwaClass, int access, Type originalOwner, Type[] args) {
    String descriptor = getCtrDescriptor(access, originalOwner, args);
    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "<init>", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "<init>", descriptor);

    mv.visitCode();
    mv.loadThis();
    mv.visitMethodInsn(INVOKESPECIAL, CWA, "<init>", "()V", false);
    var argSlot = 1;

    // We assign original only if the method is non-static
    if (!CommonAdapter.isStatic(access))
      argSlot += assignField(mv, cwaClass, argSlot, "original", originalOwner);

    for (int i = 0; i < args.length; i++) {
      argSlot += assignField(mv, cwaClass, argSlot, "arg" + i, args[i]);
    }

    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  public static String getCtrDescriptor(int access, Type originalOwner, Type[] args) {
    var argsToPass = args;
    // We need to also prepend the instance of the class if the method is non-static
    if (!CommonAdapter.isStatic(access)) {
      argsToPass = new Type[args.length + 1];
      argsToPass[0] = originalOwner;
      System.arraycopy(args, 0, argsToPass, 1, args.length);
    }

    return Type.getMethodDescriptor(Type.VOID_TYPE, argsToPass);
  }

  private static int assignField(
      GeneratorAdapter mv, Type cwaClass, int argSlot, String name, Type arg) {
    mv.loadThis();
    mv.visitVarInsn(arg.getOpcode(ILOAD), argSlot);
    mv.putField(cwaClass, name, arg);
    return arg.getSize();
  }

  private static void generateArgsMethod(ClassWriter cw, Type cwaClass, Type[] args) {
    String descriptor = Type.getMethodDescriptor(Type.getObjectType(OBJECT_ARR_DESC));

    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "args", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "args", descriptor);
    mv.visitCode();
    mv.push(args.length);
    mv.visitTypeInsn(ANEWARRAY, OBJECT_CLS_NAME);
    for (int i = 0; i < args.length; i++) {
      mv.dup();
      mv.push(i);
      mv.loadThis();
      mv.getField(cwaClass, "arg" + i, args[i]);
      mv.valueOf(args[i]);
      mv.visitInsn(AASTORE);
    }
    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  /* TODO (OPTIMUS-57169): Consider map from id to name instead */
  private static void generateFullNameMethod(
      ClassWriter cw, Type originalOwner, String originalMethod) {
    String descriptor = "()" + STRING_DESC;

    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "fullName", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "fullName", descriptor);
    mv.visitCode();

    var name = originalOwner.getClassName() + originalMethod;
    mv.push(name);
    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static void generateReApplyMethod(
      ClassWriter cw,
      Type cwaClass,
      int access,
      Type originalOwner,
      Type[] args,
      Type returnType,
      String originalMethod) {
    var originalDescriptor = Type.getMethodDescriptor(returnType, args);

    String descriptor = Type.getMethodDescriptor(OBJECT_TYPE);
    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "reApply", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "reApply", descriptor);

    mv.visitCode();

    // We put the class instance into the stack if the method is non-static
    if (!CommonAdapter.isStatic(access)) {
      mv.loadThis();
      mv.getField(cwaClass, "original", originalOwner);
    }

    for (int i = 0; i < args.length; i++) {
      mv.loadThis();
      mv.getField(cwaClass, "arg" + i, args[i]);
    }

    var opcode = CommonAdapter.invokeStaticOrVirtual(access);
    mv.visitMethodInsn(
        opcode, originalOwner.getInternalName(), originalMethod, originalDescriptor, false);
    mv.valueOf(returnType);

    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }
}
