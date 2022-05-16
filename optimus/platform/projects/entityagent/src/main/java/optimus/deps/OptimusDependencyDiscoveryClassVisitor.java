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

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;

// == Optimus dependency discovery ============================
//
// We care to map out in details which Optimus class depends on what Optimus class. We do so
// via static analysis.
//
public class OptimusDependencyDiscoveryClassVisitor extends BaseClassVisitor {
  public OptimusDependencyDiscoveryClassVisitor(VisitContext context, ClassVisitor classVisitor) {
    super(context, classVisitor);
  }

  @Override
  public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
    return context.visitIfInterested(context.addClassDependencyFromSimpleTypeDesc(descriptor), super.visitAnnotation(descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor),
        super.visitTypeAnnotation(typeRef, typePath, descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
    super.visit(version, access, name, signature, superName, interfaces);
    context.addClassDependency(superName);
    context.addClassDependenciesFromSignature(signature);
    context.addClassDependencies(interfaces);
  }

  @Override
  public void visitOuterClass(String owner, String name, String descriptor) {
    super.visitOuterClass(owner, name, descriptor);
    if (!owner.equals(context.className)) {
      context.addClassDependency(owner);
    }
    if (descriptor != null) {
      context.addClassDependency(Type.getType(descriptor));
    }
  }

  @Override
  public void visitInnerClass(String name, String outerName, String innerName, int access) {
    super.visitInnerClass(name, outerName, innerName, access);
    // We are capturing static dependencies specified to fields and method local variables
    context.addClassDependency(name);
    context.addClassDependency(outerName);
    // innerName makes little sense as we want fully qualified names
  }

  @Override
  public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
    FieldVisitor fv = super.visitField(access, name, descriptor, signature, value);
    context.addClassDependenciesFromSignature(signature);
    context.addClassDependencyFromSimpleTypeDesc(descriptor);
    context.addClassDependencyFromValue(value);
    return new OptimusDependencyDiscoveryFieldVisitor(context, fv);
  }

  @Override
  protected MethodVisitor createMethodVisitor(MethodVisitor mv, String name, String desc, String signature, String[] exceptions) {
    // Doing both the signature and the description is redundant: the signature is richer
    if (signature != null) {
      context.addClassDependenciesFromSignature(signature);
    }
    else {
      context.addClassDependencyFromMethodDesc(desc);
    }
    context.addClassDependencies(exceptions); // Signatures do not generally include exceptions

    return new OptimusDependencyDiscoveryMethodVisitor(context, mv);
  }
}
