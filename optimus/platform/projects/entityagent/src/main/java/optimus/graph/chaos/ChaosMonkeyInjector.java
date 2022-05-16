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
package optimus.graph.chaos;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import optimus.graph.DiagnosticSettings;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Rewrites methods in matching classes to call ChaosMonkeyRuntime.chaosHook.
 * Allows injection of code for testing purposes.
 */
public class ChaosMonkeyInjector implements ClassFileTransformer {
  private static final String OUR_PACKAGE = "optimus/graph/chaos/";

  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
    if (!className.startsWith(OUR_PACKAGE) && matchesFilter(className)) {
      ClassReader classReader = new ClassReader(classfileBuffer);
      ClassWriter classWriter = new ClassWriter(classReader, 0);
      ClassVisitor visitor = new ClassVisitor(Opcodes.ASM9, classWriter) {
        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
          MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);
          return new MethodVisitor(Opcodes.ASM9, mv) {
            @Override
            public void visitCode() {
              super.visitMethodInsn(Opcodes.INVOKESTATIC, OUR_PACKAGE + "ChaosMonkeyRuntime", "chaosHook", "()V", false);
            }
          };
        }
      };

      classReader.accept(visitor, 0);
      return classWriter.toByteArray();
    }
    else return null;
  }

  // n.b. can't use Java8 streams because it crashes the classloader
  private boolean matchesFilter(String className) {
    for (String prefix : DiagnosticSettings.chaosClassnamePrefixExclusions) {
      if (className.startsWith(prefix)) return false;
    }
    for (String prefix : DiagnosticSettings.chaosClassnamePrefixes) {
      if (className.startsWith(prefix)) return true;
    }
    return false;
  }
}
