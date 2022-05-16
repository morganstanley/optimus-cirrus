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
package optimus.graph;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * In an emergency situation this code allows you to inject 'synchronized' method prefix before native methods
 * It was used against SWIG generated wrappers to synchronize C/C++ methods suspected of not being thread safe
 * Manually typing synchronized keyword in auto-generated methods is not easy :)
 *
 * lock=className
 */
public class LockInjector {
  public static byte[] inject(byte[] bytes) {
    ClassReader crSource = new ClassReader(bytes);
    ClassWriter cw = new ClassWriter(crSource, 0);
    ClassVisitor cv = new AddLockInjectionAdapter(cw);
    crSource.accept(cv, 0);
    return cw.toByteArray();
  }
}

class AddLockInjectionAdapter extends ClassVisitor implements Opcodes {
  AddLockInjectionAdapter(ClassVisitor cv) {
    super(ASM9, cv);
  }

  public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
    if ((access & ACC_NATIVE) != 0)
      return cv.visitMethod(access | ACC_SYNCHRONIZED, name, desc, signature, exceptions);
    else
      return cv.visitMethod(access, name, desc, signature, exceptions);
  }
}
