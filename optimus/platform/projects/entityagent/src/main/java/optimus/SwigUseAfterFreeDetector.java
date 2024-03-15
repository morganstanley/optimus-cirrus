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

import static org.objectweb.asm.Opcodes.*;
import java.lang.instrument.ClassFileTransformer;
import java.security.ProtectionDomain;
import org.objectweb.asm.*;

public class SwigUseAfterFreeDetector implements ClassFileTransformer {
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer) {
    var swigPtr = new String[] {null};

    // We instrument every class with a long field with a name containing "swigCPtr".
    var findFields =
        new ClassVisitor(ASM9, null) {
          @Override
          public FieldVisitor visitField(
              int access, String name, String desc, String signature, Object value) {
            if ((access & ACC_STATIC) == 0 && name.contains("swigCPtr") && desc.equals("J")) {
              swigPtr[0] = name;
            }
            return null;
          }
        };

    var cr = new ClassReader(classfileBuffer);
    cr.accept(findFields, 0);

    if (swigPtr[0] != null) {
      var cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
      var cv =
          new ClassVisitor(ASM9, cw) {
            @Override
            public MethodVisitor visitMethod(
                int access, String name, String desc, String signature, String[] exceptions) {
              var mv = super.visitMethod(access, name, desc, signature, exceptions);
              // Deleting the pointer twice is fine because we already guard against double frees.
              // Finding "delete" and "finalize" keywords isn't the optimal way of doing this, but
              // it is fast to write.
              if ((name.equals("delete") || name.equals("finalize"))
                  && ((access & ACC_STATIC) != ACC_STATIC)
                  && ((access & ACC_SYNCHRONIZED) == ACC_SYNCHRONIZED)) return mv;

              return new MethodVisitor(ASM9, mv) {
                boolean hasntChecked = true;

                @Override
                public void visitFieldInsn(
                    int opcode, String owner, String name, String descriptor) {

                  // load the field onto the stack
                  mv.visitFieldInsn(opcode, owner, name, descriptor);

                  if (hasntChecked && opcode == GETFIELD && name.equals(swigPtr[0])) {
                    // Duplicate the stack
                    mv.visitInsn(DUP2);

                    // If it's our SWIG friend, call the verification method while we have the
                    // pointer on the stack.
                    mv.visitMethodInsn(
                        INVOKESTATIC,
                        "optimus/graph/MemoryChecksSupport",
                        "checkPtrLoad",
                        "(J)V",
                        false);

                    hasntChecked = false; // We do it only once per method.
                  }
                }
              };
            }
          };
      cr.accept(cv, 0);
      return cw.toByteArray();
    } else {
      return null;
    }
  }
}
