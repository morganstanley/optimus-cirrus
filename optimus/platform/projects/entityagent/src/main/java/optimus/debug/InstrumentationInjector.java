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

import static org.objectweb.asm.Opcodes.ACC_INTERFACE;
import static org.objectweb.asm.Opcodes.ASM9;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;
import java.util.function.BiPredicate;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassReaderEx;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

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

    var crSource = new ClassReaderEx(bytes);

    var entityInstrType = instrumentAllEntities;
    if (entityInstrType != none && shouldInstrumentEntity(className, crSource)) {
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

  private boolean shouldInstrumentEntity(String className, ClassReaderEx crSource) {
    return crSource.hasAnnotation(ENTITY_ANNOTATION) && !isModuleOrEntityExcluded(className);
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

    var superName = crSource.getSuperName();
    // java/lang/Object has superName set to null!
    if (superName == null) return false;

    // Only class with base class Object should be patched
    if (!superName.equals(OBJECT_TYPE.getInternalName())) return false;

    return !CommonAdapter.isThirdPartyOwned(loader, className);
  }

  private void setForwardMethodToNewEquals(String className, ClassPatch patch) {
    var equalsDef = new MethodRef(className, "equals", "(Ljava/lang/Object;)Z");
    patch.methodForwardsIfMissing.add(new MethodForward(equalsDef, instrumentedEquals));
  }
}
