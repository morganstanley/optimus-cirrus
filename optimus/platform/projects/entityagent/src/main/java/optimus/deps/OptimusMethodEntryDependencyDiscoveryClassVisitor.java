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

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;

// == Optimus dependency discovery ============================
//
// We care to map out in details which Optimus class depends on what Optimus class. We do so
// via dynamic tracing of called method on Optimus classes.
//
public class OptimusMethodEntryDependencyDiscoveryClassVisitor extends BaseClassVisitor {
  public OptimusMethodEntryDependencyDiscoveryClassVisitor(
      VisitContext context, ClassVisitor classVisitor) {
    super(context, classVisitor);
  }

  @Override
  protected MethodVisitor createMethodVisitor(
      MethodVisitor mv, String name, String desc, String signature, String[] exceptions) {
    return new OptimusMethodEntryDependencyDiscoveryMethodVisitor(context, mv);
  }
}
