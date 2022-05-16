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

import java.lang.instrument.ClassFileTransformer;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.security.ProtectionDomain;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.objectweb.asm.*;

final class StrictMathTransformer implements ClassFileTransformer {
  private static final String
      C_Math = Type.getInternalName(Math.class),
      C_StrictMath = Type.getInternalName(StrictMath.class);

  // All methods common between Math and StrictMath (apparently some like incrementExact are missing)
  private static final Set<String> methods;
  static {
    Set<String> mathMethods = new HashSet<>(), strictMathMethods = new HashSet<>();
    for (Method method : Math.class.getMethods()) {
      if (Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()))
        mathMethods.add(method.getName());
    }
    for (Method method : Math.class.getMethods()) {
      if (Modifier.isStatic(method.getModifiers()) && Modifier.isPublic(method.getModifiers()))
        strictMathMethods.add(method.getName());
    }
    mathMethods.removeIf(m -> !strictMathMethods.contains(m));
    methods = Collections.unmodifiableSet(mathMethods);
  }

  @Override
  public byte[] transform(ClassLoader loader, String name, Class<?> prev, ProtectionDomain pd, byte[] cbuf) {

    ClassWriter cw = new ClassWriter(0);
    MathStrictener cv = new MathStrictener(cw);
    ClassReader cr = new ClassReader(cbuf);
    cr.accept(cv, 0);

    if (cv.changed) {
      return cw.toByteArray();
    } else return null;
  }

  private static final class MathStrictener extends ClassVisitor {
    boolean changed = false;

    MathStrictener(ClassVisitor cv) {
      super(Opcodes.ASM9, cv);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String sig, String[] exns) {
      return new MethodVisitor(Opcodes.ASM9, super.visitMethod(access, name, desc, sig, exns)) {
        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean iface) {
          if (owner.equals(C_Math) && methods.contains(name)) {
            changed = true;
            super.visitMethodInsn(opcode, C_StrictMath, name, desc, iface);
          } else {
            super.visitMethodInsn(opcode, owner, name, desc, iface);
          }
        }
      };
    }
  }
}
