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

  public static byte[] create(String className, Type[] args) {
    ClassWriter cw = createClassWriter(className, args);
    cw.visitEnd();
    return cw.toByteArray();
  }

  private static ClassWriter createClassWriter(String className, Type[] args) {
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    cw.visit(V11, ACC_PUBLIC | ACC_SUPER, className, null, CALL_WITH_ARGS, null);
    cw.visitInnerClass(CALL_WITH_ARGS, IS, CALL_WITH_ARGS_NAME, ACC_PUBLIC | ACC_STATIC | ACC_ABSTRACT);
    generateFields(cw, args);
    generateCtor(cw, className, args);
    generateArgsMethod(cw, className, args);
    return cw;
  }

  private static void generateFields(ClassWriter cw, Type[] args) {
    for (int i = 0; i < args.length; i++) {
      FieldVisitor fv = cw.visitField(ACC_PRIVATE, "arg" + i, args[i].getDescriptor(), null, null);
      fv.visitEnd();
    }
  }

  private static void generateCtor(ClassWriter cw, String className, Type[] args) {
    var descriptor = Type.getMethodDescriptor(Type.VOID_TYPE, args);
    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", descriptor, null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitMethodInsn(INVOKESPECIAL, CALL_WITH_ARGS, "<init>", "()V", false);
    var argSlot = 1;
    for (int i = 0; i < args.length; i++) {
      mv.visitVarInsn(ALOAD, 0);
      mv.visitVarInsn(args[i].getOpcode(ILOAD), argSlot);
      mv.visitFieldInsn(PUTFIELD, className, "arg" + i, args[i].getDescriptor());
      argSlot += args[i].getSize();
    }
    mv.visitInsn(RETURN);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static void generateArgsMethod(ClassWriter cw, String className, Type[] args) {
    var descriptor = "()[Ljava/lang/Object;";
    MethodVisitor mvWriter = cw.visitMethod(ACC_PUBLIC, "args", descriptor, null, null);
    var mv = new GeneratorAdapter(mvWriter, ACC_PUBLIC, "args", descriptor);
    mv.visitCode();
    mv.visitIntInsn(BIPUSH, args.length);
    mv.visitTypeInsn(ANEWARRAY, "java/lang/Object");
    for (int i = 0; i < args.length; i++) {
      mv.visitInsn(DUP);
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
}
