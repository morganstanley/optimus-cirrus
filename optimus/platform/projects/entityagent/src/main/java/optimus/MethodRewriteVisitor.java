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

import org.objectweb.asm.*;

import static org.objectweb.asm.Opcodes.*;

/**
 * Rewrites bytecode that calls a method to call a different one and
 * reports if any changes were applied.
 */
public final class MethodRewriteVisitor extends ClassVisitor {

    private final String fromClass, fromMethod, toClass, toMethod;

    // allows the transformer to decide to skip the changes of applying this visitor
    private boolean changed;

    public boolean isChanged() {
        return changed;
    }

    public MethodRewriteVisitor(ClassVisitor cv,
                         String fromClass,
                         String fromMethod,
                         String toClass,
                         String toMethod) {
        super(ASM9, cv);
        this.fromClass = fromClass;
        this.fromMethod = fromMethod;
        this.toClass = toClass;
        this.toMethod = toMethod;
    }

    @Override
    public MethodVisitor visitMethod(int access,
                                     String name,
                                     String desc,
                                     String signature,
                                     String[] exceptions) {
        MethodVisitor visitor = super.visitMethod(access, name, desc, signature, exceptions);

        return new MethodVisitor(Opcodes.ASM9, visitor) {
            @Override
            public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
                // only supporting a subset of invocation types...
                if ((opcode == INVOKESTATIC || opcode == INVOKEVIRTUAL || opcode == INVOKEINTERFACE)
                    && owner.equals(fromClass)
                    && name.equals(fromMethod)) {
                    changed = true;
                    super.visitMethodInsn(opcode, toClass, toMethod, desc, itf);
                } else {
                    super.visitMethodInsn(opcode, owner, name, desc, itf);
                }
            }

        };
    }

}
