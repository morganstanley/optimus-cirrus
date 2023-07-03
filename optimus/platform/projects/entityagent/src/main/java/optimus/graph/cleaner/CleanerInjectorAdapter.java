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
  private final Type classType;
  private int callSiteID = -1;

  CleanerInjectorAdapter(String className, ClassVisitor cv) {
    super(ASM9, cv);
    this.classType = Type.getObjectType(className);
  }

  @Override
  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    var isFinalizeMethod = name.equals("finalize") && desc.equals("()V");
    if (isFinalizeMethod) return null; // remove the method!

    var isDeleteMethod = name.equals("delete") && desc.equals("()V");
    if (isDeleteMethod) {
      var methodFinder = new MethodInvokeFinder(INVOKESTATIC, "delete_", "(J)V");
      callSiteID =
          CleanerSupport.registerCallSiteAtCompile(
              methodFinder.foundOwner, methodFinder.foundMethod, callSiteID);
      return null; // remove the method!
    }

    var isPointerCtor = name.equals("<init>") && desc.equals("(JZ)V");
    if (isPointerCtor) callSiteID = CleanerSupport.reserveCallSiteAtCompile(callSiteID);

    var methodWriter = super.visitMethod(access, name, desc, signature, exceptions);
    var mv = CommonAdapter.wrap(methodWriter, access, name, desc);

    var isDisposeMethod = name.equals("dispose") && desc.equals("()V");
    if (isDisposeMethod) {
      callCleanableClean(mv); // write the new implementation
      return null; // remove the old implementation
    }

    return new CleanerInjectorMethodVisitor(mv, classType, callSiteID, access, name, desc);
  }

  private void callCleanableClean(CommonAdapter mv) {
    mv.visitCode();

    Label stopLabel = new Label();

    mv.loadThis();
    mv.getField(classType, CLEANABLE_FIELD_NAME, CLEANABLE_TYPE);

    mv.dup();
    mv.ifNull(stopLabel);
    mv.visitMethodInsn(INVOKEVIRTUAL, CLEANABLE_TYPE.getInternalName(), "clean", "()V", false);
    mv.returnValue();

    mv.visitLabel(stopLabel);
    mv.returnValue();

    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  private static class MethodInvokeFinder extends MethodVisitor {
    private final int opcode;
    private final String nameStart;
    private final String descriptor;
    public String foundOwner;
    public String foundMethod;

    protected MethodInvokeFinder(int opcode, String nameStart, String descriptor) {
      super(Opcodes.ASM9);
      this.opcode = opcode;
      this.nameStart = nameStart;
      this.descriptor = descriptor;
    }

    @Override
    public void visitMethodInsn(
        int code, String owner, String name, String desc, boolean isInterface) {
      var matches = code == opcode && name.startsWith(nameStart) && desc.equals(descriptor);
      if (matches) {
        foundOwner = owner;
        foundMethod = name;
      }
    }
  }
}
