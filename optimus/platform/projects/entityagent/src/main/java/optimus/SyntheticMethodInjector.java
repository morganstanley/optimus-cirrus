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
package optimus;

import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Class transformer for marking graph / internal methods as synthetic so that they can easily be hidden from the
 * debugger view
 */
public class SyntheticMethodInjector implements ClassFileTransformer {

  private String[] classPrefixExcludeList = new String[]{
      "optimus/graph/OG",
      "optimus/graph/Scheduler",
      "optimus/graph/Node",
      "optimus/graph/Tweak",
      "optimus/graph/Evaluation"
  };

  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {

    ClassReader classReader = new ClassReader(classfileBuffer);

    if (classNameInExcludeList(classReader.getClassName())) {
      ClassWriter classWriter = new ClassWriter(classReader, 0);
      ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM9, classWriter) {
        @Override
        public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
          return super.visitMethod(access | ACC_SYNTHETIC, name, desc, signature, exceptions);
        }
      };

      classReader.accept(classVisitor,0);
      return classWriter.toByteArray();
    }

    return null;
  }

  private boolean classNameInExcludeList(String className) {
    for (String prefix : classPrefixExcludeList) {
      if (className.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }
}
