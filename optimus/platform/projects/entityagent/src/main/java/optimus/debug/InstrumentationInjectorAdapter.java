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
package optimus.debug;

import static optimus.debug.InstrumentationConfig.OBJECT_DESC;
import static optimus.debug.InstrumentationConfig.OBJECT_TYPE;
import static optimus.debug.InstrumentationConfig.patchForCachingMethod;
import static optimus.debug.InstrumentationConfig.patchForLzyCompute;
import static optimus.debug.InstrumentationConfig.patchForNativeRecoding;
import static optimus.debug.InstrumentationConfig.patchForSuffixAsNode;
import static optimus.debug.InstrumentationInjector.ENTITY_DESC;
import static optimus.debug.InstrumentationInjector.SCALA_NOTHING;
import optimus.debug.InstrumentationConfig.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import optimus.EntityAgent;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

class InstrumentationInjectorAdapter extends ClassVisitor implements Opcodes {
  private final ClassPatch classPatch;
  private final String className;
  private final Set<MethodForward> methodForwardsToCall;

  private boolean hasCCtor;

  InstrumentationInjectorAdapter(ClassPatch patch, String className, ClassVisitor cv) {
    super(ASM9, cv);
    this.classPatch = patch;
    this.className = className;

    // copying forwarders here, so that we can mutate them as we visit
    // the class without modifying the given patch
    this.methodForwardsToCall = new HashSet<>(patch.methodForwardsIfMissing);
  }

  @Override
  public void visit(
      int version,
      int access,
      String name,
      String signature,
      String superName,
      String[] interfaces) {
    var finalInterfaces = interfaces;
    if (!classPatch.interfacePatches.isEmpty()) {
      finalInterfaces =
          Arrays.copyOf(interfaces, interfaces.length + classPatch.interfacePatches.size());
      var index = interfaces.length;
      for (var newInterface : classPatch.interfacePatches) finalInterfaces[index++] = newInterface;
    }
    var useSuperName =
        classPatch.replaceObjectAsBase != null
                && (access & ACC_INTERFACE) == 0
                && OBJECT_TYPE.getInternalName().equals(superName)
            ? classPatch.replaceObjectAsBase
            : superName;
    super.visit(version, access, name, signature, useSuperName, finalInterfaces);
  }

  private static boolean maybeSimpleGetter(int access, String name, String desc) {
    // 1. Only care about "normal" vals
    if ((access & ACC_STATIC) != 0) return false;

    // 2. Takes no args
    if (!desc.startsWith("()")) return false;

    // 3. Some hidden method we probably don't care about
    if (name.contains("$")) return false;

    // 4. Return type can't be void or scala.Nothing
    Type fieldType = Type.getReturnType(desc);
    return fieldType != Type.VOID_TYPE && !SCALA_NOTHING.equals(fieldType);
  }

  private boolean isCreateEntityMethod(String desc) {
    var returnType = Type.getReturnType(desc).getInternalName();
    return returnType.equals(className.substring(0, className.length() - 1));
  }

  /** Generates the forward call that matches all the arguments */
  private void writeNativeWrapper(CommonAdapter mv, int access, String name, String desc) {
    mv.visitCode();
    mv.writeCallForward(className, EntityAgent.nativePrefix(name), access, desc);
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  public MethodVisitor visitMethod(
      int access, String name, String desc, String signature, String[] exceptions) {
    // this relies on us visiting the class ctor before other methods
    // which unfortunately it is not always guaranteed.
    // We are ok with accepting this limitation, as this just for reporting purposes only
    // and we really don't want to traverse each method twice!
    if (CommonAdapter.isCCtor(access, name, desc)) hasCCtor = true;

    // the method has been correctly overridden, so we are not going to forward it!
    methodForwardsToCall.removeIf(forwarder -> name.equals(forwarder.from.method));

    var isNativeCall = (access & ACC_NATIVE) != 0;
    CommonAdapter mv = null; // Build up a chain of MethodVisitor to satisfy the transforms
    MethodPatch methodPatch = classPatch.forMethod(name, desc);

    if (methodPatch != null && methodPatch.prefixIsFullReplacement) {
      try (var nmv = CommonAdapter.newMethod(cv, access, name, desc)) {
        var fwd = methodPatch.prefix;
        var callType = methodPatch.forwardToCallIsStatic ? ACC_STATIC : 0;
        var fwdDesc = nmv.getFwdDescriptor(fwd.descriptor, callType, className);
        nmv.writeCallForward(fwd.cls, fwd.method, callType, fwdDesc);
      }

      if (methodPatch.keepOriginalMethodAs != null)
        return cv.visitMethod(
            access | ACC_PUBLIC, methodPatch.keepOriginalMethodAs, desc, signature, exceptions);
      else return null; // Don't call super.visitMethod() as we don't want the original method
    }

    if (classPatch.replaceObjectAsBase != null)
      mv =
          new SuperConstructorCallReplacement(
              mv,
              access,
              name,
              desc,
              OBJECT_TYPE.getInternalName(),
              classPatch.replaceObjectAsBase);

    if (classPatch.allMethodsPatch != null && classPatch.allMethodsPatch.shouldInject(desc, access))
      mv =
          new InstrumentationInjectorMethodVisitor(
              classPatch.allMethodsPatch, mv, access, name, desc);

    if (methodPatch != null && methodPatch.shouldInject(desc, access))
      mv = new InstrumentationInjectorMethodVisitor(methodPatch, mv, access, name, desc);

    if (classPatch.wrapNativeCalls && isNativeCall)
      mv =
          new InstrumentationInjectorMethodVisitor(
              patchForNativeRecoding(className, name), mv, access, name, desc);

    if (classPatch.cacheAllApplies && name.equals("apply") && isCreateEntityMethod(desc))
      mv =
          new InstrumentationInjectorMethodVisitor(
              patchForCachingMethod(className, name), mv, access, name, desc);

    if (classPatch.traceValsAsNodes && maybeSimpleGetter(access, name, desc))
      mv =
          new InstrumentationInjectorMethodVisitor(
              patchForSuffixAsNode(className, name), mv, access, name, desc);

    if (classPatch.bracketAllLzyComputes && name.endsWith("$lzycompute") && hasCCtor)
      mv =
          new InstrumentationInjectorMethodVisitor(
              patchForLzyCompute(className, name), mv, access, name, desc);

    // If no transformations were requested just do the default (could have called
    // super.visitMethod())
    if (mv == null) return cv.visitMethod(access, name, desc, signature, exceptions);

    // If native method was requested to be transformed (we can't)
    // generate a wrapper and transform the wrapper
    mv.resetMV(cv.visitMethod(access & ~ACC_NATIVE, name, desc, signature, exceptions));

    if (isNativeCall) {
      writeNativeWrapper(mv, access, name, desc);
      // and write out prefixed native method (the outside will finish writing this on)
      return cv.visitMethod(access, EntityAgent.nativePrefix(name), desc, signature, exceptions);
    } else {
      return mv; // Return original or the combined chain!
    }
  }

  private void writeGetterMethod(GetterMethod getter) {
    var desc = getter.mRef.descriptor == null ? "()" + OBJECT_DESC : getter.mRef.descriptor;
    MethodVisitor mv = cv.visitMethod(ACC_PUBLIC, getter.mRef.method, desc, null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitFieldInsn(GETFIELD, className, getter.field.name, getter.field.type);
    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0); // The real values will be populated automatically
    mv.visitEnd();
  }

  private void writeEqualsForCachingOverride() {
    var mv =
        cv.visitMethod(
            ACC_PUBLIC, "equalsForCachingInternal", "(" + ENTITY_DESC + ")Z", null, null);
    mv.visitCode();
    Label label0 = new Label();
    mv.visitLabel(label0);
    mv.visitVarInsn(ALOAD, 0);
    mv.visitVarInsn(ALOAD, 1);
    Label label1 = new Label();
    mv.visitJumpInsn(IF_ACMPNE, label1);
    mv.visitInsn(ICONST_1);
    Label label2 = new Label();
    mv.visitJumpInsn(GOTO, label2);
    mv.visitLabel(label1);
    mv.visitFrame(Opcodes.F_SAME, 0, null, 0, null);
    mv.visitInsn(ICONST_0);
    mv.visitLabel(label2);
    mv.visitFrame(Opcodes.F_SAME1, 0, null, 1, new Object[] {Opcodes.INTEGER});
    mv.visitInsn(IRETURN);
    Label label3 = new Label();
    mv.visitLabel(label3);
    mv.visitLocalVariable("this", "L" + className + ";", null, label0, label3, 0);
    mv.visitLocalVariable("other", ENTITY_DESC, null, label0, label3, 1);
    mv.visitMaxs(0, 0); // The real values will be populated automatically
    mv.visitEnd();
  }

  private void implementWithForwardCall(MethodForward forwards) {
    assert forwards.from.descriptor != null;
    var mv = cv.visitMethod(ACC_PUBLIC, forwards.from.method, forwards.from.descriptor, null, null);
    var mv2 = new GeneratorAdapter(mv, ACC_PUBLIC, forwards.from.method, forwards.from.descriptor);
    var argTypes = mv2.getArgumentTypes();
    var forwardsToArgs = new ArrayList<Type>();
    forwardsToArgs.add(OBJECT_TYPE);

    mv2.loadThis();
    for (int i = 0; i < argTypes.length; i++) {
      mv2.loadArg(i);
      forwardsToArgs.add(argTypes[i]);
    }

    Type forwardsToType =
        Type.getMethodType(mv2.getReturnType(), forwardsToArgs.toArray(new Type[0]));
    mv2.visitMethodInsn(
        INVOKESTATIC, forwards.to.cls, forwards.to.method, forwardsToType.getDescriptor(), false);
    mv2.returnValue();
    mv2.visitMaxs(0, 0); // The real values will be populated automatically
    mv2.visitEnd();
  }

  @Override
  public void visitEnd() {
    // implementing functions that should have been overridden using their forwarder methods
    methodForwardsToCall.forEach(this::implementWithForwardCall);

    if (classPatch.getterMethod != null) writeGetterMethod(classPatch.getterMethod);

    if (classPatch.poisonCacheEquality) writeEqualsForCachingOverride();

    for (var fpatch : classPatch.fieldRefs) {
      var type = fpatch.type;
      cv.visitField(0, fpatch.name, type, null, null);
    }
    super.visitEnd();
  }
}
