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
package optimus.graph.cleaner;

import static optimus.debug.InstrumentationConfig.CLEANABLE_FIELD_NAME;
import static optimus.debug.InstrumentationConfig.CLEANABLE_TYPE;

import optimus.debug.CommonAdapter;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

class CleanerInjectorAdapter extends ClassVisitor implements Opcodes {
  public static final String DELETE = "delete";
  public static final String DELETE_DESC = "()V";
  private final Type classType;
  int callSiteID = -1;
  private final MethodInvokeFinder deleteParser = new MethodInvokeFinder("delete_", "(J)V");

  CleanerInjectorAdapter(String className, ClassVisitor cv) {
    super(ASM9, cv);
    this.classType = Type.getObjectType(className);
  }

  @Override
  public void visit(
      int version,
      int access,
      String name,
      String signature,
      String superName,
      String[] interfaces) {
    super.visit(version, access, name, signature, superName, interfaces);
  }

  @Override
  public void visitEnd() {
    generateCleanableField();
    if (callSiteID >= 0) {
      CleanerSupport.registerCallSiteAtCompile(
          deleteParser.foundOwner, deleteParser.foundMethod, callSiteID);
      // write the new implementation
      writeDeleteMethodBody();
    }
    super.visitEnd();
  }

  private void generateCleanableField() {
    var fv =
        visitField(ACC_PRIVATE, CLEANABLE_FIELD_NAME, CLEANABLE_TYPE.getDescriptor(), null, null);
    fv.visitEnd();
  }

  @Override
  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    if (name.equals("finalize") && desc.equals("()V"))
      return null; // remove the method! disables finalization!!!!

    if (name.equals(DELETE) && desc.equals(DELETE_DESC)) {
      // find delete method and ignore the original code the method by not chaining to writer!
      return deleteParser;
    }
    var methodWriter = super.visitMethod(access, name, desc, signature, exceptions);
    var mv = CommonAdapter.wrap(methodWriter, access, name, desc);
    if (name.equals("<init>") && desc.equals("(JZ)V")) {
      callSiteID = CleanerSupport.reserveCallSiteAtCompile();
    }
    return new CleanerInjectorMethodVisitor(mv, classType, callSiteID, access, name, desc);
  }

  private void writeDeleteMethodBody() {
    var methodWriter = super.visitMethod(ACC_PUBLIC, DELETE, DELETE_DESC, null, null);
    var mv = CommonAdapter.wrap(methodWriter, ACC_PUBLIC, DELETE, DELETE_DESC);
    mv.visitCode();

    Label stopLabel = new Label();

    mv.loadThis();
    mv.getField(classType, CLEANABLE_FIELD_NAME, CLEANABLE_TYPE);

    mv.dup();
    mv.ifNull(stopLabel);
    mv.visitMethodInsn(INVOKEINTERFACE, CLEANABLE_TYPE.getInternalName(), "clean", "()V", true);
    if (deleteParser.superOwner != null) {
      mv.loadThis();
      mv.visitMethodInsn(
          INVOKESPECIAL, deleteParser.superOwner, deleteParser.superMethod, DELETE_DESC, false);
    }
    mv.returnValue();

    mv.visitLabel(stopLabel);
    mv.returnValue();

    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static class MethodInvokeFinder extends MethodVisitor {
    private final String nameStart;
    private final String descriptor;
    public String foundOwner;
    public String foundMethod;
    public String superOwner;
    public String superMethod;

    protected MethodInvokeFinder(String nameStart, String descriptor) {
      super(Opcodes.ASM9);
      this.nameStart = nameStart;
      this.descriptor = descriptor;
    }

    @Override
    public void visitMethodInsn(
        int opcode, String owner, String name, String desc, boolean isInterface) {
      if (opcode == INVOKESTATIC && name.startsWith(nameStart) && desc.equals(descriptor)) {
        foundOwner = owner;
        foundMethod = name;
      }
      if (opcode == INVOKESPECIAL) {
        superOwner = owner;
        superMethod = name;
      }
    }
  }
}
