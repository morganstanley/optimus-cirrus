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
package optimus.debug;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Helper method visitor to replace calls to OLD_BASE.super.function with NEW_BASE.super.function
 * Useful when changing the base class
 *  */
class SuperConstructorCallReplacement extends CommonAdapter {
  private final String originalBase;
  private final String newBase;

  SuperConstructorCallReplacement(MethodVisitor mv,  int access, String name, String descriptor, String originalBase, String newBase) {
    super(mv, access, name, descriptor);
    this.originalBase = originalBase;
    this.newBase = newBase;
  }

  @Override
  public void visitMethodInsn(int opcodeAndSource, String owner, String name, String descriptor, boolean isInterface) {
    if (opcodeAndSource == INVOKESPECIAL && owner.equals(originalBase))
      owner = newBase;
    super.visitMethodInsn(opcodeAndSource, owner, name, descriptor, isInterface);
  }
}
