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
import static org.objectweb.asm.Opcodes.INVOKEDYNAMIC;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;

// == Optimus dependency discovery ============================
//
// We care to map out in details which Optimus class depends on what Optimus class. We do so
// via static analysis.
//
public class OptimusDependencyDiscoveryMethodVisitor extends MethodVisitor {
  private final VisitContext context;

  public OptimusDependencyDiscoveryMethodVisitor(VisitContext context, MethodVisitor mv) {
    super(ASM_API, mv);

    this.context = context;
  }

  @Override
  public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor),
        super.visitAnnotation(descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public AnnotationVisitor visitAnnotationDefault() {
    return new OptimusDependencyDiscoveryAnnotationVisitor(context, super.visitAnnotationDefault());
  }

  @Override
  public AnnotationVisitor visitTypeAnnotation(int typeRef, TypePath typePath, String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor),
        super.visitTypeAnnotation(typeRef, typePath, descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public AnnotationVisitor visitParameterAnnotation(int parameter, String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor),
        super.visitParameterAnnotation(parameter, descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public AnnotationVisitor visitInsnAnnotation(int typeRef, TypePath typePath, String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor),
        super.visitInsnAnnotation(typeRef, typePath, descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public AnnotationVisitor visitTryCatchAnnotation(int typeRef, TypePath typePath, String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor),
        super.visitTryCatchAnnotation(typeRef, typePath, descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public AnnotationVisitor visitLocalVariableAnnotation(int typeRef, TypePath typePath, Label[] start, Label[] end,
      int[] index, String descriptor, boolean visible) {
    return context.visitIfInterested(
        context.addClassDependencyFromSimpleTypeDesc(descriptor),
        super.visitLocalVariableAnnotation(typeRef, typePath, start, end, index, descriptor, visible),
        OptimusDependencyDiscoveryAnnotationVisitor::new);
  }

  @Override
  public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {
    super.visitTryCatchBlock(start, end, handler, type);
    context.addClassDependency(type);
  }

  // Handle methods which returns Class, resource URL, or new class instance
  public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf) {
    super.visitMethodInsn(opcode, owner, name, desc, itf);
    if (name.equals("$init$") || name.equals("<clinit>") || name.equals(
        "<init>") || opcode == INVOKESTATIC || opcode == INVOKEINTERFACE || opcode == INVOKEVIRTUAL || opcode == INVOKEDYNAMIC || opcode == INVOKESPECIAL) {
      if (!owner.equals(context.className)) {
        context.addClassDependency(owner);
      }
      context.addClassDependencyFromMethodDesc(desc);
    }
  }

  @Override
  public void visitTypeInsn(int opcode, String type) {
    super.visitTypeInsn(opcode, type);
    context.addClassDependency(type);
  }

  @Override
  public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
    super.visitFieldInsn(opcode, owner, name, descriptor);
    if (!owner.equals(context.className)) {
      context.addClassDependency(owner);
    }
    context.addClassDependency(Type.getType(descriptor));
  }

  @Override
  public void visitMultiANewArrayInsn(String descriptor, int numDimensions) {
    super.visitMultiANewArrayInsn(descriptor, numDimensions);
    context.addClassDependency(Type.getType(descriptor));
  }

  @Override
  public void visitLocalVariable(String name, String descriptor, String signature, Label start, Label end, int index) {
    super.visitLocalVariable(name, descriptor, signature, start, end, index);
    if (!name.equals("this")) {
      if (signature != null) {
        context.addClassDependenciesFromSignature(signature);
      } else {
        context.addClassDependency(Type.getType(descriptor));
      }
    }
  }

  @Override
  public void visitLdcInsn(Object constant) {
    super.visitLdcInsn(constant);
    if (constant instanceof Type) {
      Type tpe = (Type) constant;
      switch (tpe.getSort()) {
      case Type.OBJECT:
      case Type.ARRAY:
      case Type.METHOD:
        context.addClassDependency(tpe);
        break;
      }
    } else if (constant instanceof Handle) {
      Handle handle = (Handle) constant;
      if (!handle.getOwner().equals(context.className)) {
        context.addClassDependency(handle.getOwner());
      }
      context.addClassDependencyFromMethodDesc(handle.getDesc());
    } else if (constant instanceof ConstantDynamic) {
      ConstantDynamic constDyn = (ConstantDynamic) constant;
      context.addClassDependencyFromSimpleTypeDesc(constDyn.getDescriptor());
      context.addClassDependencyFromMethodDesc(constDyn.getBootstrapMethod().getDesc());
    }
  }
}
