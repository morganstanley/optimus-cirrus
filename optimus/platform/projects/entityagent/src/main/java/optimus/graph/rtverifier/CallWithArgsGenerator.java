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
import static optimus.debug.InstrumentationConfig.CALL_WITH_ARGS_NAME;
import static optimus.debug.InstrumentationConfig.CALL_WITH_ARGS;
import static optimus.debug.InstrumentationConfig.OBJECT_ARR_DESC;
import static optimus.debug.InstrumentationConfig.OBJECT_TYPE;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

public class CallWithArgsGenerator implements Opcodes {

  private final static AtomicInteger counter = new AtomicInteger(0);

  public static String generateClassName(String methodName) {
    return String.join("_", "optimus/CA", methodName, Integer.toString(counter.getAndIncrement()));
  }

  public static byte[] create(String className, Type originalOwner, Type[] args, Type returnType, String originalMethod) {
    ClassWriter cw = createClassWriter(className, originalOwner, args, returnType, originalMethod);
    cw.visitEnd();
    return cw.toByteArray();
  }

  private static ClassWriter createClassWriter(String className, Type originalOwner, Type[] args, Type returnType, String originalMethod) {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    cw.visit(V11, ACC_PUBLIC | ACC_SUPER, className, null, CALL_WITH_ARGS, null);
    cw.visitInnerClass(CALL_WITH_ARGS, IS, CALL_WITH_ARGS_NAME, ACC_PUBLIC | ACC_STATIC | ACC_ABSTRACT);
    generateFields(cw, originalOwner, args);
    generateCtor(cw, className, originalOwner, args);
    generateArgsMethod(cw, className, args);
    generateReApplyMethod(cw, className, originalOwner, args, returnType, originalMethod);
    return cw;
  }

  private static void generateFields(ClassWriter cw, Type originalOwner, Type[] args) {
    generatePrivateField(cw, "original", originalOwner);
    for (int i = 0; i < args.length; i++) {
      generatePrivateField(cw, "arg" + i, args[i]);
    }
  }

  private static void generatePrivateField(ClassWriter cw, String name, Type tpe) {
    FieldVisitor fv = cw.visitField(ACC_PRIVATE, name, tpe.getDescriptor(), null, null);
    fv.visitEnd();
  }

  private static void generateCtor(ClassWriter cw, String className, Type originalOwner, Type[] args) {
    String descriptor = getCtrDescriptor(originalOwner, args);
    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "<init>", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "<init>", descriptor);

    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, CALL_WITH_ARGS, "<init>", "()V", false);
    var argSlot = 1;

    argSlot += assignField(mv, className, argSlot, "original", originalOwner);
    for (int i = 0; i < args.length; i++) {
      argSlot += assignField(mv, className, argSlot, "arg" + i, args[i]);
    }

    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  public static String getCtrDescriptor(Type originalOwner, Type[] args) {
    var argsToPass = new Type[args.length + 1];
    argsToPass[0] = originalOwner;
    System.arraycopy(args, 0, argsToPass, 1, args.length);

    return Type.getMethodDescriptor(Type.VOID_TYPE, argsToPass);
  }

  private static int assignField(MethodVisitor mv, String className, int argSlot, String name, Type arg) {
    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(arg.getOpcode(ILOAD), argSlot);
    mv.visitFieldInsn(PUTFIELD, className, name, arg.getDescriptor());
    return arg.getSize();
  }

  private static void generateArgsMethod(ClassWriter cw, String className, Type[] args) {
    String descriptor = Type.getMethodDescriptor(Type.getObjectType(OBJECT_ARR_DESC));

    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "args", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "args", descriptor);
    mv.visitCode();
    mv.visitIntInsn(BIPUSH, args.length);
    mv.visitTypeInsn(ANEWARRAY, "java/lang/Object");
    for (int i = 0; i < args.length; i++) {
      mv.dup();
      mv.visitIntInsn(BIPUSH, i);
      mv.loadThis();
      mv.visitFieldInsn(GETFIELD, className, "arg" + i, args[i].getDescriptor());
      mv.box(args[i]);
      mv.visitInsn(AASTORE);
    }
    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static void generateReApplyMethod(ClassWriter cw, String className, Type originalOwner, Type[] args, Type returnType, String originalMethod) {
    var originalDescriptor = Type.getMethodDescriptor(returnType, args);

    String descriptor = Type.getMethodDescriptor(OBJECT_TYPE);
    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "reApply", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "reApply", descriptor);

    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitFieldInsn(GETFIELD, className, "original", originalOwner.getDescriptor());

    for (int i = 0; i < args.length; i++) {
      mv.visitVarInsn(ALOAD, 0);
      mv.visitFieldInsn(GETFIELD, className, "arg" + i, args[i].getDescriptor());
    }

    mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, originalOwner.getInternalName(), originalMethod, originalDescriptor, false);
    mv.valueOf(returnType);

    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }
}
