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
package org.objectweb.asm;

import static org.objectweb.asm.Opcodes.*;
import optimus.debug.CommonAdapter;

public class ClassWriterEx extends ClassWriter {
  public ClassWriterEx(int flags) {
    super(flags);
  }

  public void writeField(String name, Type type) {
    var fv = visitField(ACC_PUBLIC, name, type.getDescriptor(), null, null);
    fv.visitEnd();
  }

  public void writeAccessor(String className, String name, Type type) {
    writeAccessor(className, name, type, type);
  }

  public void writeAccessor(String className, String name, Type type, Type returnType) {
    String descriptor = Type.getMethodDescriptor(returnType);
    var access = ACC_PUBLIC | ACC_FINAL;
    try (var mv = CommonAdapter.newMethod(this, access, name, descriptor)) {
      mv.getThisField(className, name, type);
      mv.returnValue();
    }
  }
}
