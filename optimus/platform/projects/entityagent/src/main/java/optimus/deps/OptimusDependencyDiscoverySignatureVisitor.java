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

import org.objectweb.asm.signature.SignatureVisitor;

public class OptimusDependencyDiscoverySignatureVisitor extends SignatureVisitor {
  private final VisitContext context;

  public OptimusDependencyDiscoverySignatureVisitor(VisitContext context) {
    super(ASM_API);

    this.context = context;
  }

  // This is the only method we care as ultimately it always lead to this one
  @Override
  public void visitClassType(String name) {
    super.visitClassType(name);
    context.addClassDependency(name);
  }

  // formal type parameters and type variables are not interesting, as they correspond to the names
  // provided in generics (e.g. T, U)

  @Override
  public void visitInnerClassType(String name) {
    super.visitInnerClassType(name);
    context.addClassDependency(name);
  }
}
