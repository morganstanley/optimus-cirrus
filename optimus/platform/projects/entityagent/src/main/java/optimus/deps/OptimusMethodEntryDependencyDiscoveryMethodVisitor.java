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

import static optimus.deps.BaseClassVisitor.ASM_API;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

import org.objectweb.asm.MethodVisitor;

// == Optimus dependency discovery ============================
//
// We care to map out in details which Optimus class depends on what Optimus class. We do so
// via dynamic tracing of called method on Optimus classes.
//
public class OptimusMethodEntryDependencyDiscoveryMethodVisitor extends MethodVisitor {
  private final VisitContext context;

  OptimusMethodEntryDependencyDiscoveryMethodVisitor(VisitContext context, MethodVisitor mv) {
    super(ASM_API, mv);

    this.context = context;
  }

  @Override
  public void visitCode() {
    super.visitCode();

    // Only log this class usage once per method
    addCallToLoggerForCurrentClassUsage(mv);
  }

  private void addCallToLoggerForCurrentClassUsage(MethodVisitor mv) {
    // First param is always already loaded and is a class name
    mv.visitLdcInsn(context.classResourceName);
    mv.visitMethodInsn(
        INVOKESTATIC,
        "optimus/ClassMonitorInjector",
        "logClassUsage",
        "(Ljava/lang/String;)V",
        false);
  }
}
