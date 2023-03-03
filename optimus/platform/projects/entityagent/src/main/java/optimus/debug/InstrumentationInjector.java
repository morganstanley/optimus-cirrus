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

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;

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
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.GeneratorAdapter;

/** In doubt see: var asm = BiopsyLab.byteCodeAsAsm(cw.toByteArray()); */
public class InstrumentationInjector implements ClassFileTransformer {
  final static Type SCALA_NOTHING = Type.getType("Lscala/runtime/Nothing$;");
  final static String ENTITY_DESC = "Loptimus/platform/storable/Entity;";
  private final static String ENTITY_COMPANION_BASE = "Loptimus/platform/storable/EntityCompanionBase;";

  @Override
  public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined, ProtectionDomain protectionDomain,
                          byte[] bytes) throws IllegalClassFormatException {

    ClassPatch patch = forClass(className);
    if (instrumentAllNativePackagePrefixes != null && className.startsWith(instrumentAllNativePackagePrefixes)) {
      patch = new ClassPatch();
      patch.wrapNativeCalls = true;
    }
    if (patch == null && !instrumentAnyGroups())
      return bytes;

    ClassReader crSource = new ClassReader(bytes);

    var entityInstrType = instrumentAllEntities;
    if (entityInstrType != none && shouldInstrumentEntity(className, crSource.getSuperName())) {
      if (entityInstrType == markScenarioStack) addMarkScenarioStackAsInitializing(className);
      else if (entityInstrType == recordConstructedAt) recordConstructorInvocationSite(className);
      if (patch == null)
        patch = forClass(className);  // Re-read reconfigured value
    }

    boolean addHashCode = shouldAddHashCode(loader, crSource, className);
    if (patch != null && patch.methodForward == null && addHashCode)
      setForwardMethodToNewHashCode(className, patch);

    if (patch == null && addHashCode) {
      patch = new ClassPatch();
      setForwardMethodToNewHashCode(className, patch);
    }

    if (instrumentAllEntityApplies && shouldCacheApplyMethods(crSource, className)) {
      if (patch == null)
        patch = new ClassPatch();
      patch.cacheAllApplies = true;
    }

    if (instrumentAllModuleConstructors && shouldInstrumentModuleCtor(loader, className))
      patch = addModuleConstructionIntercept(className);

    if (patch == null)
      return bytes;

    ClassWriter cw = new ClassWriter(crSource, ClassWriter.COMPUTE_FRAMES);
    ClassVisitor cv = new InstrumentationInjectorAdapter(patch, className, cw);
    crSource.accept(cv, ClassReader.SKIP_FRAMES);
    return cw.toByteArray();
  }

  private boolean shouldCacheApplyMethods(ClassReader crSource, String className) {
    if (!className.endsWith("$"))
      return false;    // Looking for companion objects
    String[] interfaces = crSource.getInterfaces();
    for (String iface : interfaces)
      if (ENTITY_COMPANION_BASE.equals(iface))
        return true;
    return false;
  }


  private boolean shouldInstrumentEntity(String className, String superName) {
    return isEntity(className, superName) && !isModuleOrEntityExcluded(className);
  }

  private boolean shouldInstrumentModuleCtor(ClassLoader loader, String className) {
    var isModuleCtor = loader != null && className.endsWith("$") && !className.startsWith("scala");
    return isModuleCtor && !isModuleOrEntityExcluded(className);
  }

  private boolean shouldAddHashCode(ClassLoader loader, ClassReader crSource, String className) {
    if (!instrumentAllHashCodes)
      return false;
    // Interfaces are not included
    if ((crSource.getAccess() & ACC_INTERFACE) != 0)
      return false;
    // Only class with base class Object should be patched
    if (!crSource.getSuperName().equals(OBJECT_TYPE.getInternalName()))
      return false;

    if (loader == null)
      return false;

    // Probably should be extracted
    if(className.startsWith("scala/reflect"))
      return false;

    // Presumably we know what we are doing, also ProfilerEventsWriter for sure needs to be ignored
    return !className.startsWith("sun/") && !className.startsWith("java/security");
  }

  private void setForwardMethodToNewHashCode(String className, ClassPatch patch) {
    var mrHashCode = new MethodRef(className, "hashCode", "()I");
    patch.methodForward = new MethodForward(mrHashCode, InstrumentedHashCodes.mrHashCode);
  }
}

class InstrumentationInjectorAdapter extends ClassVisitor implements Opcodes {
  private final ClassPatch classPatch;
  private final String className;
  private boolean seenForwardedMethod;

  InstrumentationInjectorAdapter(ClassPatch patch, String className, ClassVisitor cv) {
    super(ASM9, cv);
    this.classPatch = patch;
    this.className = className;
  }

  @Override
  public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
    var finalInterfaces = interfaces;
    if (!classPatch.interfacePatches.isEmpty()) {
      finalInterfaces = Arrays.copyOf(interfaces, interfaces.length + classPatch.interfacePatches.size());
      var index = interfaces.length;
      for (var newInterface : classPatch.interfacePatches)
        finalInterfaces[index++] = newInterface;
    }
    var useSuperName = classPatch.replaceObjectAsBase != null && (access & ACC_INTERFACE) == 0 &&  OBJECT_TYPE.getInternalName().equals(superName)
                       ? classPatch.replaceObjectAsBase : superName;
    super.visit(version, access, name, signature, useSuperName, finalInterfaces);
  }

  private static boolean maybeSimpleGetter(int access, String name, String desc) {
    if((access & ACC_STATIC) != 0)    // 1. Only care about "normal" vals
      return false;
    if (!desc.startsWith("()"))       // 2. Takes no args
      return false;
    if (name.contains("$"))           // 3. Some hidden method we probably don't care about
      return false;

    // 4. Return type can't be void or scala.Nothing
    Type fieldType = Type.getReturnType(desc);
    return fieldType != Type.VOID_TYPE && !SCALA_NOTHING.equals(fieldType);
  }

  private boolean isCreateEntityMethod(String desc) {
    var returnType = Type.getReturnType(desc).getInternalName();
    return returnType.equals(className.substring(0, className.length() - 1));
  }

  // TODO (OPTIMUS-53248): Make this more generic and generate CallWithArgs
  private void writeNativeMethodCall(String name, String desc) {
    ClassWriter cw = new ClassWriter(0);
    var clsName = "call_" + name;
    cw.visit(V11, ACC_PUBLIC | ACC_SUPER, clsName, null, "optimus/graph/InstrumentationSupport$CallWithArgs", null);
    cw.visitInnerClass("optimus/graph/InstrumentationSupport$CallWithArgs",
                       "optimus/graph/InstrumentationSupport",
                       "CallWithArgs",
                       ACC_PUBLIC | ACC_STATIC | ACC_ABSTRACT);
    {
      var mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
      mv.visitCode();
      mv.visitVarInsn(ALOAD, 0);
      mv.visitMethodInsn(INVOKESPECIAL, "optimus/graph/InstrumentationSupport$CallWithArgs", "<init>", "()V", false);
      mv.visitInsn(RETURN);
      mv.visitMaxs(1, 1);
      mv.visitEnd();
    }
    {
      var argTypes = Type.getArgumentTypes(desc);
      var mv = cw.visitMethod(ACC_PROTECTED, "apply", "(JLjava/lang/Object;JLjava/lang/Object;)Z", null, null);
      mv.visitCode();
      mv.visitVarInsn(LLOAD, 1);
      mv.visitVarInsn(ALOAD, 3);
      mv.visitTypeInsn(CHECKCAST, argTypes[1].getInternalName());
      mv.visitVarInsn(LLOAD, 4);
      mv.visitVarInsn(ALOAD, 6);
      mv.visitTypeInsn(CHECKCAST, argTypes[1].getInternalName());
      mv.visitMethodInsn(INVOKESTATIC, className, EntityAgent.nativePrefix(name), desc, false);
      mv.visitInsn(IRETURN);
      mv.visitMaxs(6, 7);
      mv.visitEnd();
    }
    cw.visitEnd();
    new DynamicClassLoader().createInstance(cw.toByteArray());
  }

  // TODO (OPTIMUS-53248): Make this more generic and generate CallWithArgs
  private void writeNativeWrapper(int access, String name, String desc, String signature, String[] exceptions) {
    var useAccess = access & ~ACC_NATIVE;
    var mvWriter = cv.visitMethod(useAccess, name, desc, signature, exceptions);
    var mv = new GeneratorAdapter(mvWriter, useAccess, name, desc);

    mv.visitCode();
    mv.visitMethodInsn(INVOKESTATIC,  nativePrefix.cls, nativePrefix.method, nativePrefix.descriptor, false);

    // call original method
    mv.loadArgs();
    mv.visitMethodInsn(INVOKESTATIC, className, EntityAgent.nativePrefix(name), desc, false);


    if(className.equals("AIOP/Java/VolSwigJNI") && name.equals("VolNode_isEqualTo")) {
      mv.dup();
      mv.loadArgs();
      var descX= "(ZJ"+ OBJECT_DESC + "J" + OBJECT_DESC + ")V";
      mv.visitMethodInsn(INVOKESTATIC,  nativeSuffix.cls, nativeSuffix.method, descX, false);
      writeNativeMethodCall(name, desc);
    } else
     mv.visitMethodInsn(INVOKESTATIC,  nativeSuffix.cls, nativeSuffix.method, "()V", false);

    mv.returnValue();
    mv.visitMaxs(0, 0);
    mv.visitEnd();
  }

  public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
    if (classPatch.methodForward != null && name.equals(classPatch.methodForward.from.method))
      seenForwardedMethod = true;

    MethodVisitor mv;
    if (classPatch.wrapNativeCalls && (access & ACC_NATIVE) != 0) {
      writeNativeWrapper(access, name, desc, signature, exceptions);
      mv = cv.visitMethod(access, EntityAgent.nativePrefix(name), desc, signature, exceptions);
    } else
      mv = cv.visitMethod(access, name, desc, signature, exceptions);

    if (classPatch.allMethodsPatch != null)
      return new InstrumentationInjectorMethodVisitor(classPatch.allMethodsPatch, mv, access, name, desc);
    MethodPatch methodPatch = classPatch.forMethod(name, desc);
    if (methodPatch != null)
      return new InstrumentationInjectorMethodVisitor(methodPatch, mv, access, name, desc);
    else if (classPatch.cacheAllApplies && name.equals("apply") && isCreateEntityMethod(desc))
      return new InstrumentationInjectorMethodVisitor(patchForCachingMethod(className, name), mv, access, name, desc);
    else if (classPatch.traceValsAsNodes && maybeSimpleGetter(access, name, desc))
      return new InstrumentationInjectorMethodVisitor(patchForSuffixAsNode(className, name), mv, access, name, desc);
    else if (classPatch.bracketAllLzyComputes && name.endsWith("$lzycompute"))
      return new InstrumentationInjectorMethodVisitor(patchForBracketingLzyCompute(className, name), mv, access, name, desc);
    else
      return mv; // Just copy the entire method
  }

  private void writeGetterMethod(GetterMethod getter) {
    var desc = getter.mRef.descriptor == null ? "()" + OBJECT_DESC : getter.mRef.descriptor;
    MethodVisitor mv = cv.visitMethod(ACC_PUBLIC, getter.mRef.method, desc, null, null);
    mv.visitCode();
    mv.visitVarInsn(ALOAD, 0);
    mv.visitFieldInsn(GETFIELD, className, getter.field.name, getter.field.type);
    mv.visitInsn(ARETURN);
    mv.visitMaxs(0, 0);  // The real values will be populated automatically
    mv.visitEnd();
  }

  private void writeEqualsForCachingOverride() {
    var mv = cv.visitMethod(ACC_PUBLIC, "equalsForCachingInternal", "(" + ENTITY_DESC + ")Z", null, null);
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
    mv.visitFrame(Opcodes.F_SAME1, 0, null, 1, new Object[] { Opcodes.INTEGER });
    mv.visitInsn(IRETURN);
    Label label3 = new Label();
    mv.visitLabel(label3);
    mv.visitLocalVariable("this", "L" + className + ";", null, label0, label3, 0);
    mv.visitLocalVariable("other", ENTITY_DESC, null, label0, label3, 1);
    mv.visitMaxs(0, 0); // The real values will be populated automatically
    mv.visitEnd();
  }

  private void writeImplementForwardCall(MethodForward forwards) {
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

    Type forwardsToType = Type.getMethodType(mv2.getReturnType(), forwardsToArgs.toArray(new Type[0]));
    mv2.visitMethodInsn(INVOKESTATIC, forwards.to.cls, forwards.to.method, forwardsToType.getDescriptor(), false);
    mv2.returnValue();
    mv2.visitMaxs(0,0); // The real values will be populated automatically
    mv2.visitEnd();
  }

  @Override
  public void visitEnd() {
    if (classPatch.methodForward != null && !seenForwardedMethod)
      writeImplementForwardCall(classPatch.methodForward);

    if (classPatch.getterMethod != null)
      writeGetterMethod(classPatch.getterMethod);

    if (classPatch.poisonCacheEquality)
      writeEqualsForCachingOverride();

    for (var fpatch : classPatch.fieldRefs) {
      var type = fpatch.type;
      cv.visitField(0, fpatch.name, type, null, null);
    }
    super.visitEnd();
  }
}

class InstrumentationInjectorMethodVisitor extends AdviceAdapter implements Opcodes {
  private final MethodPatch patch;
  private final Label __localValueStart = new Label();
  private final Label __localValueEnd = new Label();
  private int __localValue;     // When local passing is enabled this will point to a slot for local var
  private Type localValueType;
  private String localValueDesc;
  private String callWithArgsCtorDescriptor;
  private String callWithArgsType;
  private int methodID;         // If allocation requested
  private boolean thisIsAvailable;
  private final Label tryBlockStart = new Label();
  private final Label tryBlockEnd = new Label();
  private final Label catchBlockStart = new Label();
  private final Label catchBlockEnd = new Label();
  private final boolean doInject;

  InstrumentationInjectorMethodVisitor(MethodPatch patch, MethodVisitor mv, int access,
                                       String name, String descriptor) {
    super(ASM9, mv, access, name, descriptor);
    this.patch = patch;
    this.doInject = patch.predicate == null || patch.predicate.test(name, methodDesc);

    if (patch.passLocalValue && !patch.localValueIsCallWithArgs) {
      localValueType = patch.prefix.descriptor != null
                       ? Type.getMethodType(patch.prefix.descriptor).getReturnType()
                       : OBJECT_TYPE;
      localValueDesc = localValueType.getDescriptor();
    }

    if (patch.localValueIsCallWithArgs) {
      // we generate a CallWithArgs instance...
      Type thisOwner = Type.getObjectType(patch.from.cls);
      callWithArgsType = CallWithArgsGenerator.generateClassName(getName());
      byte[] newBytes = CallWithArgsGenerator.create(callWithArgsType, thisOwner, getArgumentTypes(), getReturnType(), getName());
      DynamicClassLoader.loadClassInCurrentClassLoader(newBytes);
      callWithArgsCtorDescriptor = CallWithArgsGenerator.getCtrDescriptor(thisOwner, getArgumentTypes());
      // ...and we pass our CallWithArgs instance to the suffix calls
      localValueType = Type.getObjectType(InstrumentationConfig.CWA);
      localValueDesc = localValueType.getDescriptor();
    }
  }

  private void dupReturnValueOrNullForVoid(int opcode, boolean boxValueTypes) {
    if (opcode == RETURN)
      visitInsn(ACONST_NULL);
    else if (opcode == ARETURN || opcode == ATHROW)
      dup();
    else {
      if (opcode == LRETURN || opcode == DRETURN)
        dup2(); // double/long take two slots
      else
        dup();
      if (boxValueTypes)
        box(Type.getReturnType(this.methodDesc));
    }
  }

  private String loadMethodID() {
    if (methodID == 0)
      methodID = allocateID(patch.from);
    mv.visitIntInsn(SIPUSH, methodID);
    return "I";
  }

  private String loadThisOrNull() {
    if ((methodAccess & ACC_STATIC) != 0 || (!thisIsAvailable && getName().equals("<init>")))
      mv.visitInsn(ACONST_NULL);    // static will just pass null and ctor will temporarily pass null
    else
      loadThis();
    return OBJECT_DESC;
  }

  private String loadArgsInlineOrAsArray() {
    if (patch.noArgumentBoxing) {
      loadArgs();
      return "";  // Rely on descriptor explicitly supplied
    } else {
      loadArgArray();
      return OBJECT_ARR_DESC;
    }
  }

  private String loadLocalValueIfRequested() {
    if (patch.passLocalValue) {
      mv.visitVarInsn(localValueType.getOpcode(ILOAD), __localValue);
      return localValueType.getDescriptor();
    }
    return "";
  }

  private void ifNotZeroReturn(FieldRef fpatch) {
    loadThis();
    mv.visitFieldInsn(GETFIELD, patch.from.cls, fpatch.name, fpatch.type);
    Label label1 = new Label();
    mv.visitJumpInsn(IFEQ, label1);
    loadThis();
    mv.visitFieldInsn(GETFIELD, patch.from.cls, fpatch.name, fpatch.type);
    mv.visitInsn(IRETURN);
    mv.visitLabel(label1);
  }

  private void injectMethodPrefix() {
    if (patch.cacheInField != null)
      ifNotZeroReturn(patch.cacheInField);

    if (patch.prefix == null && !patch.localValueIsCallWithArgs)
      return; // prefix is implied for localValueIsCallWithArgs

    MethodRef prefix = patch.prefix;

    if (patch.passLocalValue) {
      visitLabel(__localValueStart);
      __localValue = newLocal(localValueType);
    }

    var descriptor = "(";
    if (patch.prefixWithID)
      descriptor += loadMethodID();

    if(patch.prefixWithThis)
      descriptor += loadThisOrNull();

    if (patch.prefixWithArgs)
      descriptor += loadArgsInlineOrAsArray();

    if (patch.passLocalValue || patch.storeToField != null)
      descriptor += ")" + OBJECT_DESC;
    else
      descriptor += ")V";

    if (patch.localValueIsCallWithArgs) {
      mv.visitTypeInsn(NEW, callWithArgsType);
      dup();
      loadThis();
      loadArgs();
      mv.visitMethodInsn(INVOKESPECIAL, callWithArgsType, "<init>", callWithArgsCtorDescriptor, false);
    }

    if (prefix != null) {
      if (prefix.descriptor != null) // If descriptor was supplied just use that
        descriptor = prefix.descriptor;

      mv.visitMethodInsn(INVOKESTATIC, prefix.cls, prefix.method, descriptor, false);
    }

    if (patch.passLocalValue) {
      mv.visitVarInsn(localValueType.getOpcode(ISTORE), __localValue);
    } else if (patch.storeToField != null) {
      loadThis();
      swap();
      mv.visitFieldInsn(PUTFIELD, patch.from.cls, patch.storeToField.name, OBJECT_DESC);
    }

    if (patch.checkAndReturn) {
      mv.visitVarInsn(ALOAD, __localValue);
      mv.visitFieldInsn(GETFIELD, CACHED_VALUE_TYPE, "hasResult", "Z");
      var continueLabel = new Label();
      mv.visitJumpInsn(IFEQ, continueLabel);
      mv.visitVarInsn(ALOAD, __localValue);
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
    super.visitCode();
    if (doInject)
      injectMethodPrefix();
  }

  @Override
  protected void onMethodEnter() {
    // Consider adding code to report 'this' in the case of constructor
    thisIsAvailable = true;
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

    if (patch.suffix == null || !doInject)
      return;

    var descriptor = "(";

    if (patch.suffixWithReturnValue) {
      dupReturnValueOrNullForVoid(opcode, !patch.noArgumentBoxing);
      descriptor += OBJECT_DESC;
    }

    descriptor += loadLocalValueIfRequested();

    if (patch.suffixWithID)
      descriptor += loadMethodID();

    if (patch.suffixWithThis)
      descriptor += loadThisOrNull();

    if (patch.suffixWithArgs)
      descriptor += loadArgsInlineOrAsArray();

    descriptor += ")V";

    // If descriptor was supplied just use that
    if (patch.suffix.descriptor != null)
      descriptor = patch.suffix.descriptor;

    mv.visitMethodInsn(INVOKESTATIC, patch.suffix.cls, patch.suffix.method, descriptor, false);
  }

  @Override
  public void visitMethodInsn(int opcodeAndSource, String owner, String name, String descriptor, boolean isInterface) {
    if (patch.classPatch != null && patch.classPatch.replaceObjectAsBase != null &&
        opcodeAndSource == INVOKESPECIAL && owner.equals(OBJECT_TYPE.getInternalName()))
      owner = patch.classPatch.replaceObjectAsBase;
    super.visitMethodInsn(opcodeAndSource, owner, name, descriptor, isInterface);
  }

  @Override
  public void visitMaxs(int maxStack, int maxLocals) {
    if (!doInject) {
      super.visitMaxs(maxStack, maxLocals);
      return;
    }
    if (patch.wrapWithTryCatch) {
      visitLabel(tryBlockEnd);
      mv.visitInsn(NOP);
      visitLabel(catchBlockStart);

      var suffixOnException = patch.suffixOnException;
      if (suffixOnException != null) {
        dup();
        var descriptor = "(" + THROWABLE_TYPE.getDescriptor() + loadLocalValueIfRequested() + ")V";
        mv.visitMethodInsn(INVOKESTATIC, suffixOnException.cls, suffixOnException.method, descriptor, false);
      }
      throwException();
      visitLabel(catchBlockEnd);
    }

    if (patch.passLocalValue) {
      visitLabel(__localValueEnd);
      mv.visitLocalVariable("__locValue", localValueDesc, null, __localValueStart, __localValueEnd, __localValue);
    }

    super.visitMaxs(maxStack, maxLocals);
  }
}