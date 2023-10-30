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

import static optimus.debug.EntityInstrumentationType.markScenarioStack;
import static optimus.debug.EntityInstrumentationType.none;
import static optimus.debug.EntityInstrumentationType.recordConstructedAt;
import static optimus.debug.InstrumentationConfig.*;
import static optimus.debug.InstrumentationInjector.ENTITY_DESC;
import static optimus.debug.InstrumentationInjector.SCALA_NOTHING;

import static org.objectweb.asm.Opcodes.ACC_INTERFACE;
import static org.objectweb.asm.Opcodes.ASM9;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiPredicate;

import optimus.DynamicClassLoader;
import optimus.EntityAgent;
import optimus.graph.rtverifier.CallWithArgsGenerator;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

/** In doubt see: var asm = BiopsyLab.byteCodeAsAsm(cw.toByteArray()); */
public class InstrumentationInjector implements ClassFileTransformer {
  static final Type SCALA_NOTHING = Type.getType("Lscala/runtime/Nothing$;");
  static final String ENTITY_DESC = "Loptimus/platform/storable/Entity;";
  private static final String ENTITY_COMPANION_BASE =
      "Loptimus/platform/storable/EntityCompanionBase;";

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] bytes)
      throws IllegalClassFormatException {
    ClassPatch patch = forClass(className);
    if (patch == null && !instrumentAnyGroups()) return bytes;

    ClassReader crSource = new ClassReader(bytes);

    var entityInstrType = instrumentAllEntities;
    if (entityInstrType != none && shouldInstrumentEntity(className, crSource.getSuperName())) {
      if (entityInstrType == markScenarioStack) addMarkScenarioStackAsInitializing(className);
      else if (entityInstrType == recordConstructedAt) recordConstructorInvocationSite(className);
      if (patch == null) patch = forClass(className); // Re-read reconfigured value
    }

    var addHashCode = shouldOverrideHashCode(loader, crSource, className);
    if (addHashCode) {
      if (patch == null) patch = new ClassPatch();
      setForwardMethodToNewHashCode(className, patch);
    }

    var addEquals = shouldOverrideEquals(loader, crSource, className);
    if (addEquals) {
      if (patch == null) patch = new ClassPatch();
      setForwardMethodToNewEquals(className, patch);
    }

    if (instrumentAllNativePackagePrefixes != null
        && className.startsWith(instrumentAllNativePackagePrefixes)) {
      if (patch == null) patch = new ClassPatch();
      patch.wrapNativeCalls = true;
    }

    if (instrumentAllEntityApplies && shouldCacheApplyMethods(crSource, className)) {
      if (patch == null) patch = new ClassPatch();
      patch.cacheAllApplies = true;
    }

    if (instrumentAllModuleConstructors && shouldInstrumentModuleCtor(loader, className))
      patch = addModuleConstructionIntercept(className);

    if (instrumentAllDerivedClasses != null) {
      var selfOrDerived =
          instrumentAllDerivedClasses.cls.equals(className)
              || isDerivedClass(className, crSource.getSuperName());
      if (selfOrDerived)
        findNodeMethodsAndAddTimers(instrumentAllDerivedClasses.method, new ClassReader(bytes));
    }

    if (patch == null) return bytes;

    ClassWriter cw = new ClassWriter(crSource, ClassWriter.COMPUTE_FRAMES);
    ClassVisitor cv = new InstrumentationInjectorAdapter(patch, className, cw);
    crSource.accept(cv, ClassReader.SKIP_FRAMES);
    return cw.toByteArray();
  }

  private void findNodeMethodsAndAddTimers(String methodName, ClassReader crSource) {
    final var expectedClassPrefix = "$" + methodName + "$node";

    /*
     Don't instrument synthetic bridge method! [SEE_SKIP_SYNTHETIC_BRIDGE]
     We generate a synthetic bridge to the real return type of func:
        public final synthetic bridge func()Ljava/lang/Object;

     This has the following instruction (e.g. for the node method InstrumentationEntity.presentValue)
        INVOKEVIRTUAL optimus/profiler/InstrumentationEntity$$presentValue$node$1.func ()I

     To call the method with the real return type:
        public final func()I

     The instrumentation should only go on the 'real' func, ie, not on the bridge one
    */
    final BiPredicate<String, Integer> skipSyntheticBridge =
        (ignored, flags) -> (flags & Opcodes.ACC_BRIDGE) == 0;

    var cv =
        new ClassVisitor(ASM9) {
          @Override
          public void visitInnerClass(String name, String outerName, String innerName, int access) {
            if (innerName.startsWith(expectedClassPrefix)) {
              var mp1 = addStartCounterAndTimer(new MethodRef(name, "funcFSM"));
              var mp2 = addStartCounterAndTimer(new MethodRef(name, "func"));
              mp1.predicate = skipSyntheticBridge;
              mp2.predicate = skipSyntheticBridge;
            }
          }
        };
    crSource.accept(cv, ClassReader.SKIP_CODE);
  }

  private boolean shouldCacheApplyMethods(ClassReader crSource, String className) {
    if (!className.endsWith("$")) return false; // Looking for companion objects
    String[] interfaces = crSource.getInterfaces();
    for (String iface : interfaces) if (ENTITY_COMPANION_BASE.equals(iface)) return true;
    return false;
  }

  private boolean shouldInstrumentEntity(String className, String superName) {
    return isEntity(className, superName) && !isModuleOrEntityExcluded(className);
  }

  private boolean shouldInstrumentModuleCtor(ClassLoader loader, String className) {
    var isModuleCtor = loader != null && className.endsWith("$") && !className.startsWith("scala");
    return isModuleCtor && !isModuleOrEntityExcluded(className);
  }

  private boolean shouldOverrideHashCode(
      ClassLoader loader, ClassReader crSource, String className) {
    if (!instrumentAllHashCodes) return false;

    // Interfaces are not included
    if ((crSource.getAccess() & ACC_INTERFACE) != 0) return false;

    // Scala objects are singleton in nature, therefore they always have a stable hashCode
    if (crSource.getClassName().endsWith("$")) return false;

    // Only class with base class Object should be patched
    if (!crSource.getSuperName().equals(OBJECT_TYPE.getInternalName())) return false;

    // Presumably we know what we are doing, also ProfilerEventsWriter for sure needs to be ignored
    return !CommonAdapter.isThirdPartyOwned(loader, className);
  }

  private void setForwardMethodToNewHashCode(String className, ClassPatch patch) {
    var mrHashCode = new MethodRef(className, "hashCode", "()I");
    patch.methodForwardsIfMissing.add(
        new MethodForward(mrHashCode, InstrumentedHashCodes.mrHashCode));
  }

  private boolean shouldOverrideEquals(ClassLoader loader, ClassReader crSource, String className) {
    if (!instrumentEquals) return false;

    // Interfaces are not included
    if ((crSource.getAccess() & ACC_INTERFACE) != 0) return false;

    // Scala objects are singleton in nature, therefore they always have a stable equality
    if (crSource.getClassName().endsWith("$")) return false;

    // Only class with base class Object should be patched
    if (!crSource.getSuperName().equals(OBJECT_TYPE.getInternalName())) return false;

    return !CommonAdapter.isThirdPartyOwned(loader, className);
  }

  private void setForwardMethodToNewEquals(String className, ClassPatch patch) {
    var equalsDef = new MethodRef(className, "equals", "(Ljava/lang/Object;)Z");
    patch.methodForwardsIfMissing.add(new MethodForward(equalsDef, instrumentedEquals));
  }
}

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
    mv.writeCallForward(className, EntityAgent.nativePrefix(name), access, desc);
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
    CommonAdapter mv =
        null; // Build up a chain of MethodVisitor to satisfy the transformation requirement
    MethodPatch methodPatch = classPatch.forMethod(name, desc);

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

class InstrumentationInjectorMethodVisitor extends CommonAdapter {
  private final MethodPatch patch;
  private String callWithArgsCtorDescriptor;
  private String callWithArgsType;
  private final Label tryBlockStart = new Label();
  private final Label tryBlockEnd = new Label();
  private final Label catchBlockStart = new Label();
  private final Label catchBlockEnd = new Label();

  InstrumentationInjectorMethodVisitor(
      MethodPatch patch, MethodVisitor mv, int access, String name, String descriptor) {
    super(mv, access, name, descriptor);
    this.patch = patch;
  }

  private void injectMethodPrefix() {
    if (patch.cacheInField != null) ifNotZeroReturn(patch, patch.cacheInField);

    if (patch.prefix == null && !patch.localValueIsCallWithArgs)
      return; // prefix is implied for localValueIsCallWithArgs

    MethodRef prefix = patch.prefix;

    if (patch.passLocalValue) visitLocalValueStart();

    var descriptor = "(";
    if (patch.prefixWithID) descriptor += loadMethodID(patch);

    if (patch.prefixWithThis) descriptor += loadThisOrNull();

    if (patch.prefixWithArgs) descriptor += loadArgsInlineOrAsArray(patch);

    if (patch.passLocalValue || patch.storeToField != null) descriptor += ")" + OBJECT_DESC;
    else descriptor += ")V";

    if (patch.localValueIsCallWithArgs) {
      mv.visitTypeInsn(NEW, callWithArgsType);
      dup();
      loadThisIfNonStatic();
      loadArgs();
      mv.visitMethodInsn(
          INVOKESPECIAL, callWithArgsType, "<init>", callWithArgsCtorDescriptor, false);
    }

    if (prefix != null) {
      // If descriptor was supplied just use that
      if (prefix.descriptor != null) descriptor = prefix.descriptor;

      mv.visitMethodInsn(INVOKESTATIC, prefix.cls, prefix.method, descriptor, false);
    }

    if (patch.passLocalValue) {
      storeLocalValue();
    } else if (patch.storeToField != null) {
      loadThis();
      swap();
      mv.visitFieldInsn(PUTFIELD, patch.from.cls, patch.storeToField.name, OBJECT_DESC);
    }

    if (patch.checkAndReturn) {
      loadLocalValue();
      mv.visitFieldInsn(GETFIELD, CACHED_VALUE_TYPE, "hasResult", "Z");
      var continueLabel = new Label();
      mv.visitJumpInsn(IFEQ, continueLabel);
      loadLocalValue();
      mv.visitFieldInsn(GETFIELD, CACHED_VALUE_TYPE, "result", OBJECT_DESC);
      mv.visitTypeInsn(CHECKCAST, Type.getReturnType(methodDesc).getInternalName());
      mv.visitInsn(ARETURN);
      mv.visitLabel(continueLabel);
    }

    if (patch.wrapWithTryCatch) {
      mv.visitTryCatchBlock(tryBlockStart, tryBlockEnd, catchBlockStart, THROWABLE);
      mv.visitLabel(tryBlockStart);
    }
  }

  @Override
  public void visitCode() {
    if (patch.passLocalValue && !patch.localValueIsCallWithArgs) {
      var localValueType =
          patch.prefix.descriptor != null
              ? Type.getMethodType(patch.prefix.descriptor).getReturnType()
              : OBJECT_TYPE;
      setLocalValueTypeAndDesc(localValueType);
    }

    if (patch.localValueIsCallWithArgs) {
      // we generate a CallWithArgs instance...
      Type thisOwner = Type.getObjectType(patch.from.cls);
      callWithArgsType = CallWithArgsGenerator.generateClassName(getName());
      byte[] newBytes =
          CallWithArgsGenerator.create(
              methodAccess,
              callWithArgsType,
              thisOwner,
              getArgumentTypes(),
              getReturnType(),
              getName());
      DynamicClassLoader.loadClassInCurrentClassLoader(newBytes);
      callWithArgsCtorDescriptor =
          CallWithArgsGenerator.getCtrDescriptor(methodAccess, thisOwner, getArgumentTypes());
      // ...and we pass our CallWithArgs instance to the suffix calls
      setLocalValueTypeAndDesc(Type.getObjectType(InstrumentationConfig.CWA));
    }
    super.visitCode();
    injectMethodPrefix();
  }

  @Override
  protected void onMethodExit(int opcode) {
    if (opcode == ATHROW && (patch.noArgumentBoxing || patch.wrapWithTryCatch))
      return; // do not generate exit call at the point of exception throw

    if (patch.cacheInField != null) {
      dup();
      loadThis();
      swap();
      mv.visitFieldInsn(PUTFIELD, patch.from.cls, patch.cacheInField.name, patch.cacheInField.type);
    }

    if (patch.suffix == null) return;

    var descriptor = "(";

    if (patch.suffixWithReturnValue)
      descriptor += dupReturnValueOrNullForVoid(opcode, !patch.noArgumentBoxing);

    if (patch.passLocalValue) descriptor += loadLocalValue();

    if (patch.suffixWithID) descriptor += loadMethodID(patch);

    if (patch.suffixWithThis) descriptor += loadThisOrNull();

    if (patch.suffixWithArgs) descriptor += loadArgsInlineOrAsArray(patch);

    descriptor += ")V";

    // If descriptor was supplied just use that
    if (patch.suffix.descriptor != null) descriptor = patch.suffix.descriptor;

    mv.visitMethodInsn(INVOKESTATIC, patch.suffix.cls, patch.suffix.method, descriptor, false);
  }

  @Override
  public void visitMaxs(int maxStack, int maxLocals) {
    if (patch.wrapWithTryCatch) {
      visitLabel(tryBlockEnd);
      mv.visitInsn(NOP);
      visitLabel(catchBlockStart);

      var suffixOnException = patch.suffixOnException;
      if (suffixOnException != null) {
        dup();
        var descriptor =
            "(" + THROWABLE_TYPE.getDescriptor() + loadLocalValueIfRequested(patch) + ")V";
        mv.visitMethodInsn(
            INVOKESTATIC, suffixOnException.cls, suffixOnException.method, descriptor, false);
      }
      throwException();
      visitLabel(catchBlockEnd);
    }

    if (patch.passLocalValue) visitLocalValueEnd();

    super.visitMaxs(maxStack, maxLocals);
  }
}
