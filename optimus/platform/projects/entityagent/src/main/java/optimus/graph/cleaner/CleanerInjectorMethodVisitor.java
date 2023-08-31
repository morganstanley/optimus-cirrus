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

import static optimus.debug.InstrumentationConfig.OBJECT_TYPE;
import static optimus.debug.InstrumentationConfig.CLEANABLE_TYPE;
import static optimus.debug.InstrumentationConfig.CLEANABLE_FIELD_NAME;

import optimus.debug.CommonAdapter;
import org.objectweb.asm.Type;

class CleanerInjectorMethodVisitor extends CommonAdapter {

  private final Type classType;
  private final int callSiteID;
  private final String name;
  private final String descriptor;

  CleanerInjectorMethodVisitor(
      CommonAdapter mv,
      Type classType,
      int callSiteID,
      int access,
      String name,
      String descriptor) {
    super(mv, access, name, descriptor);
    this.classType = classType;
    this.callSiteID = callSiteID;
    this.name = name;
    this.descriptor = descriptor;
  }

  @Override
  public void onMethodExit(int opcode) {
    var isPointerCtor = name.equals("<init>") && descriptor.equals("(JZ)V");
    // NOTE: we adding to the existing implementation here
    if (isPointerCtor) assignCleanableField();
  }

  private static final String registerDesc =
      Type.getMethodDescriptor(
          CLEANABLE_TYPE,
          OBJECT_TYPE /* this */,
          Type.LONG_TYPE /* pointer*/,
          Type.BOOLEAN_TYPE /* ownership */,
          Type.INT_TYPE /* callSiteID */);
  private static final String CLEANER_SUPPORT = "optimus/graph/cleaner/CleanerSupport";

  private void assignCleanableField() {
    loadThis();
    dup();
    loadArgs();
    visitLdcInsn(callSiteID);
    visitMethodInsn(INVOKESTATIC, CLEANER_SUPPORT, "register", registerDesc, false);
    putField(classType, CLEANABLE_FIELD_NAME, CLEANABLE_TYPE);
  }
}
