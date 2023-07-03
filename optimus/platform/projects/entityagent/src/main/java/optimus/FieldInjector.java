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

import java.util.HashMap;
import java.util.List;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

class AddFieldsAdapter extends ClassVisitor implements Opcodes {
  private HashMap<String, String> accessors = new HashMap<>();
  private HashMap<String, String> fields = new HashMap<>();
  private String cname;

  AddFieldsAdapter(List<String> properties, ClassVisitor cv) {
    super(ASM9, cv);
    for (String field : properties) {
      String backingField = "_" + field;
      accessors.put("get" + field, backingField);
      accessors.put("set" + field, backingField);
    }
  }

  public void visit(
      int version,
      int access,
      String name,
      String signature,
      String superName,
      String[] interfaces) {
    cname = name;
    cv.visit(version, access, name, signature, superName, interfaces);
  }

  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    String fieldName = accessors.get(name);
    if (fieldName == null) return cv.visitMethod(access, name, desc, signature, exceptions);

    Type returnType = Type.getReturnType(desc);
    if (returnType != Type.VOID_TYPE) {
      String fieldDesc = returnType.getDescriptor();
      fields.put(fieldName, fieldDesc);
      {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);
        mv.visitCode();
        Label l0 = new Label();
        mv.visitLabel(l0);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitFieldInsn(GETFIELD, cname, fieldName, fieldDesc);
        mv.visitInsn(returnType.getOpcode(IRETURN));
        Label l1 = new Label();
        mv.visitLabel(l1);
        mv.visitLocalVariable("this", "L" + cname + ";", null, l0, l1, 0);
        mv.visitMaxs(2, 1);
        mv.visitEnd();
      }
    } else {
      Type[] args = Type.getArgumentTypes(desc);
      if (args.length != 1) throw new RuntimeException("Only 1 argument is supported");
      Type fieldType = args[0];
      String fieldDesc = fieldType.getDescriptor();
      fields.put(fieldName, fieldDesc);
      {
        MethodVisitor mv = cv.visitMethod(access, name, desc, signature, exceptions);

        mv.visitCode();
        Label l0 = new Label();
        mv.visitLabel(l0);
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(fieldType.getOpcode(ILOAD), 1);
        mv.visitFieldInsn(PUTFIELD, cname, fieldName, fieldDesc);
        Label l1 = new Label();
        mv.visitLabel(l1);
        mv.visitInsn(RETURN);
        Label l2 = new Label();
        mv.visitLabel(l2);
        mv.visitLocalVariable("this", "L" + cname + ";", null, l0, l2, 0);
        mv.visitLocalVariable("v", fieldDesc, null, l0, l2, 1);
        mv.visitMaxs(3, 3);
        mv.visitEnd();
      }
    }
    return null; // We took care of getter/setter above
  }

  public void visitEnd() {
    for (String field : fields.keySet()) {
      String fieldDesc = fields.get(field);
      cv.visitField(ACC_PRIVATE | ACC_TRANSIENT, field, fieldDesc, null, null);
    }
    cv.visitEnd();
  }
}
