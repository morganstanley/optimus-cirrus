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

// == Dynamic dependency discovery ============================
//
// We care to map out dynamically which Optimus class is used, but not third-parties.
//
// We care what resources are used dynamically, from no matter where.
public class DynamicDependencyDiscoveryClassVisitor extends BaseClassVisitor {
  public DynamicDependencyDiscoveryClassVisitor(VisitContext context, ClassVisitor classVisitor) {
    super(context, classVisitor);
  }

  protected MethodVisitor createMethodVisitor(MethodVisitor mv, String name, String desc, String signature, String[] exceptions) {
    return new DynamicDependencyDiscoveryMethodVisitor(context, mv);
  }
}
