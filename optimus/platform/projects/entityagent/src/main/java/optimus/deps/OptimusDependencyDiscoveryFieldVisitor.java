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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.TypePath;

public class OptimusDependencyDiscoveryFieldVisitor extends FieldVisitor {
  private final VisitContext context;

  public OptimusDependencyDiscoveryFieldVisitor(VisitContext context, FieldVisitor fv) {
    super(ASM_API, fv);

    this.context = context;
  }

  @Override
  public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor), super.visitAnnotation(descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String descriptor, boolean visible) {
    return context.visitIfInterested(context.addClassDependencyFromSimpleTypeDesc(descriptor), super.visitTypeAnnotation(typeRef, typePath, descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public void visitAttribute(Attribute attribute) {
    super.visitAttribute(attribute);
  }
}
