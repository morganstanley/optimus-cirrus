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
package optimus.deps;

import static org.objectweb.asm.Opcodes.ACC_ABSTRACT;
import static org.objectweb.asm.Opcodes.ACC_INTERFACE;
import static org.objectweb.asm.Opcodes.ACC_NATIVE;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public abstract class BaseClassVisitor extends ClassVisitor {
  // ASM API
  public static final int ASM_API = Opcodes.ASM9;

  protected final VisitContext context;

  BaseClassVisitor(
      VisitContext context, ClassVisitor classVisitor) {
    super(ASM_API, classVisitor);

    this.context = context;
  }

  protected abstract MethodVisitor createMethodVisitor(MethodVisitor mv, String name, String desc, String signature, String[] exceptions);

  @Override
  public final MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
    MethodVisitor mv = super.visitMethod(access, name, desc, signature, exceptions);

    if (((access & ACC_NATIVE) != 0) || ((access & ACC_ABSTRACT) != 0) || ((access & ACC_INTERFACE) != 0)) {
      return mv;
    }

    // Walk through instructions
    return createMethodVisitor(mv, name, desc, signature, exceptions);
  }
}
