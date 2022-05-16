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
package optimus.testidle;

import static org.objectweb.asm.Opcodes.*;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;

/**
 * this transformer modifies the RunNotifier so that we can listen to Junit test execution
 * <p>
 * the attached listener - a TestManager will emit a thread dump if this stream of messages becomes idle
 */
public class TestIdleTransformer implements ClassFileTransformer {
  final private static String
      C_RunNotifier    = "org/junit/runner/notification/RunNotifier",
      C_TestManager    = "optimus/platform/tests/common/TestManager",
      M_recordNotifier = "recordNotifier",
      D_recordNotifier = "(L" + C_RunNotifier + ";)V",
      M_ctor           = "<init>";

  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
    if (C_RunNotifier.equals(className)) {
      boolean hasTestManager = loader.getResource(C_TestManager + ".class") != null;
      if (hasTestManager) {
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        TestIdleClassVisitor visitor = new TestIdleClassVisitor(writer);
        ClassReader reader = new ClassReader(classfileBuffer);
        reader.accept(visitor, ClassReader.SKIP_FRAMES);
        return writer.toByteArray();
      } else {
        System.err.println("Skipping TestManager instrumentation as it is not on the classpath!");
        return null;
      }
    } else return null;
  }

  private static class TestIdleClassVisitor extends ClassVisitor {
    TestIdleClassVisitor(ClassVisitor cv) { super(ASM9, cv); }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
      MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
      if (!name.equals(M_ctor)) return mv;
      else return new MethodVisitor(ASM9, mv) {
        @Override
        public void visitInsn(int opcode) {
          if (opcode == RETURN) {
            super.visitVarInsn(ALOAD, 0);
            super.visitMethodInsn(INVOKESTATIC, C_TestManager, M_recordNotifier, D_recordNotifier, false);
          }
          super.visitInsn(opcode);
        }
      };
    }
  }
}
