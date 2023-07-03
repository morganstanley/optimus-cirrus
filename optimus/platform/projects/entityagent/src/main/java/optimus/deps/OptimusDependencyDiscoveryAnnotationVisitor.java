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

public class OptimusDependencyDiscoveryAnnotationVisitor extends AnnotationVisitor {
  private final VisitContext context;

  public OptimusDependencyDiscoveryAnnotationVisitor(VisitContext context, AnnotationVisitor av) {
    super(ASM_API, av);

    this.context = context;
  }

  @Override
  public void visit(String name, Object value) {
    super.visit(name, value);
    context.addClassDependencyFromValue(value);
  }

  @Override
  public void visitEnum(String name, String descriptor, String value) {
    super.visitEnum(name, descriptor, value);
    context.addClassDependencyFromSimpleTypeDesc(descriptor);
  }

  @Override
  public AnnotationVisitor visitArray(String name) {
    return new OptimusDependencyDiscoveryAnnotationVisitor(context, super.visitArray(name));
  }

  @Override
  public AnnotationVisitor visitAnnotation(String name, String descriptor) {
    String annotationName = context.addClassDependencyFromSimpleTypeDesc(descriptor);
    return context.visitIfInterested(
        annotationName,
        super.visitAnnotation(name, descriptor),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }
}
