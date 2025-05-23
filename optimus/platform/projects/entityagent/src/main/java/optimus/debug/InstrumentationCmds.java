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

import static optimus.EntityAgent.logException;
import static optimus.debug.InstrumentationConfig.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import optimus.graph.DiagnosticSettings;

@SuppressWarnings({"unused", "WeakerAccess"})
// A number of methods in the class are called via reflection when loaded from a config
public class InstrumentationCmds {

  private static class ScalaWorksheetAlmostParser extends StringTokenizer {
    static final String Q = "\"";
    static final String D = " \t\n\r\f,()";
    String function;
    ArrayList<Object> args = new ArrayList<>();
    ArrayList<Class<?>> argTypes = new ArrayList<>();

    ScalaWorksheetAlmostParser(String text) {
      super(text, D);
    }

    void parse() {
      if (hasMoreTokens()) function = nextToken();
      while (hasMoreTokens()) {
        var lastToken = nextToken(D);
        if (lastToken.startsWith(Q) && lastToken.endsWith(Q)) {
          args.add(lastToken.substring(1, lastToken.length() - 1));
          argTypes.add(String.class);
        } else if (lastToken.startsWith("\"")) {
          var endToken = nextToken(Q);
          nextToken(D); // eat the quote
          args.add(lastToken.substring(1) + endToken);
          argTypes.add(String.class);
        } else if (lastToken.equals("true")) {
          args.add(Boolean.TRUE);
          argTypes.add(boolean.class);
        } else if (lastToken.equals("false")) {
          args.add(Boolean.FALSE);
          argTypes.add(boolean.class);
        }
      }
    }

    void invoke() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      if (function != null) {
        var method = InstrumentationCmds.class.getMethod(function, argTypes.toArray(new Class[0]));
        method.invoke(null, args.toArray());
      }
    }
  }

  /**
   * either from file (instrumentationConfig) or inline from system property (instrumentationCache)
   */
  public static void loadCommands() {
    if (DiagnosticSettings.instrumentationCache != null) {
      for (var function : DiagnosticSettings.instrumentationCache) cache(function);
    }
    loadAndParseConfig();
  }

  private static void loadAndParseConfig() {
    try {
      if (DiagnosticSettings.enableRTVerifier) parseAndApplyConfig(readResource("rt_verifier.sc"));

      if (DiagnosticSettings.modifyScalaHashCollectionIterationOrder)
        parseAndApplyConfig(readResource("modify_scala212_hash_improver.sc"));

      // custom config goes last so it can overwrite settings if desired
      if (DiagnosticSettings.instrumentationConfig != null)
        parseAndApplyConfig(
            Files.readAllLines(Paths.get(DiagnosticSettings.instrumentationConfig)));

    } catch (Exception e) {
      logException("error while loading config:", e);
    }
  }

  private static void parseAndApplyConfig(List<String> lines)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    if (lines == null) return;

    boolean inComment = false; // Reading lines between /* and */
    for (String line : lines) {
      if (line.startsWith("import ") || line.startsWith("//")) continue;
      if (line.startsWith("/*")) {
        inComment = true;
        continue;
      }
      if (inComment && line.endsWith("*/")) {
        inComment = false;
        continue;
      }
      var parser = new ScalaWorksheetAlmostParser(line);
      parser.parse();
      parser.invoke();
    }
  }

  private static List<String> readResource(String filename) throws IOException {
    List<String> lines;
    try (var resource = ClassLoader.getSystemResourceAsStream(filename)) {
      assert resource != null;
      var reader = new BufferedReader(new InputStreamReader(resource, StandardCharsets.UTF_8));
      lines = reader.lines().collect(Collectors.toList());
    }
    return lines;
  }

  /**
   * @param fieldName new field to be injected to class on which methodName is defined
   * @param methodName fully qualified method name to intercept
   */
  public static void injectCurrentNodeAndStackToField(String fieldName, String methodName) {
    InstrumentationConfig.addRecordPrefixCallIntoMemberWithStackTrace(
        fieldName, asMethodRef(methodName), traceCurrentNodeAndStack);
  }

  /**
   * Inject field containing construction site stack into class className
   *
   * @param className class to trace construction of
   */
  public static void recordConstructorInvocationSite(String className) {
    InstrumentationConfig.recordConstructorInvocationSite(className);
  }

  /** Inject field containing construction site stack into all entities */
  public static void recordAllEntityConstructorInvocationSites() {
    instrumentAllEntities = EntityInstrumentationType.recordConstructedAt;
  }

  /**
   * Make class className extend interfaceToAdd Used to add marker interfaces on the fly
   *
   * @param className class to be extended
   * @param interfaceToAdd interface or trait to add
   */
  public static void addInterface(String className, String interfaceToAdd) {
    var jvmClassName = className.replace('.', '/');
    var jvmInterfaceName = interfaceToAdd.replace('.', '/');
    InstrumentationConfig.addInterfacePatch(jvmClassName, jvmInterfaceName);
  }

  /**
   * Injects all natives call with default prefix, suffix, and .
   *
   * @param packagePrefix the prefix of the package to inject
   */
  public static void wrapAllNative(String packagePrefix) {
    wrapAllNative(packagePrefix, cwaPrefix, cwaSuffix, cwaSuffixOnException);
  }

  /**
   * Injects all natives call from a package with prefix, suffix, and suffix on exception calls.
   *
   * @param packagePrefix the prefix of the package to inject
   * @param prefixCall method to call before the execution
   * @param suffixCall method to call after successful execution
   * @param onException method to call if exception is thrown during the execution
   */
  public static void wrapAllNative(
      String packagePrefix, MethodRef prefixCall, MethodRef suffixCall, MethodRef onException) {
    InstrumentationConfig.instrumentAllNativePackagePrefixes = packagePrefix.replace('.', '/');
    InstrumentationConfig.instrumentNativePrefix = prefixCall;
    InstrumentationConfig.instrumentNativeSuffix = suffixCall;
    InstrumentationConfig.instrumentNativeSuffixOnException = onException;
  }

  /**
   * Inject into the Entity identified by className a method equalsForCaching that compares by
   * identity
   *
   * <p>The reason to do this, is to get cache hits only on exactly the same entity instance. This
   * in some cases avoids the issue with the entity caching some unstable value and re-using it in
   * wrong context
   */
  public static void addCacheIdentityPoisoning(String className) {
    var clsPatch = putIfAbsentClassPatch(className);
    clsPatch.poisonCacheEquality = true;
  }

  /**
   * Instrument hashCode() override to cache it's value in a member variable called __hashCode
   * Assumes 0 is non-computed value
   */
  public static void cacheHashCode(String className) {
    var mref = new MethodRef(className, "hashCode");
    var patch = putIfAbsentMethodPatch(mref);
    patch.cacheInField = addField(className, "__hashCode", "I");
  }

  /**
   * Instrument @methodRef to record the invocation as a leaf node
   *
   * @param methodRef Full name reference name Such package.className.methodName
   */
  public static void traceAsNode(String methodRef) {
    MethodRef from = asMethodRef(methodRef);
    addTraceEntryAsNode(from);
  }

  /**
   * Instrument @methodRef to record the invocation as a leaf node, and trigger all parents to be
   * recorded
   *
   * @param methodRef Full name reference name Such package.className.methodName
   */
  public static void traceAsNodeAndParents(String methodRef) {
    MethodRef from = asMethodRef(methodRef);
    addTraceEntryAsNodeAndParents(from);
  }

  /**
   * Instrument @methodToPatch to call @methodToCall by forwarding all the arguments
   *
   * @param methodToPatch full name package1.class2.methodName1
   * @param methodToCall full name package1.class2.methodName2
   * @param toStatic true if method to call is static (or virtual otherwise)
   * @param saveOrgSuffix if non-null save the original function renaming it by appending this value
   */
  public static void forward(
      String methodToPatch, String methodToCall, boolean toStatic, String saveOrgSuffix) {
    MethodRef from = asMethodRef(methodToPatch);
    MethodRef to = asMethodRef(methodToCall);
    var pre = InstrumentationConfig.addPrefixCall(from, to, false, true);
    pre.prefixIsFullReplacement = true;
    pre.noArgumentBoxing = true;
    pre.keepOriginalMethodAs = from.method + saveOrgSuffix;
    pre.forwardToCallIsStatic = toStatic;
  }

  /**
   * Instrument @methodToPatch to call @methodToCall by forwarding all the arguments
   *
   * @param methodToPatch full name package1.class2.methodName1
   * @param methodToCall full name package1.class2.methodName2
   */
  public static void forward(String methodToPatch, String methodToCall) {
    forward(methodToPatch, methodToCall, true, FWD_ORG_DEFAULT_SUFFIX);
  }

  /**
   * Instrument @methodToPatch to call @methodToCall by forwarding all the argument
   *
   * @param methodToPatch full name package1.class2.methodName1
   * @param methodToCall full name package1.class2.methodName2
   */
  public static void forwardToVirtual(String methodToPatch, String methodToCall) {
    forward(methodToPatch, methodToCall, false, null);
  }

  /**
   * Instrument @methodToPatch to call @methodToCall as the first call
   *
   * @param methodToPatch full name package1.class2.methodName1
   * @param methodToCall full name package1.class2.methodName2
   */
  public static void prefixCall(String methodToPatch, String methodToCall) {
    MethodRef from = asMethodRef(methodToPatch);
    MethodRef to = asMethodRef(methodToCall);
    InstrumentationConfig.addPrefixCall(from, to, false, false);
  }

  /**
   * Instrument @methodToPatch to call @methodToCall as the first call
   *
   * @param methodToPatch full name package1.class2.methodName1
   * @param methodToCall full name package1.class2.methodName2
   */
  public static void suffixCall(String methodToPatch, String methodToCall) {
    MethodRef from = asMethodRef(methodToPatch);
    MethodRef to = asMethodRef(methodToCall);
    var suffix = InstrumentationConfig.addSuffixCall(from, to);
    suffix.suffixWithThis = true;
  }

  /**
   * Instrument @methodToPatch to call @methodToCall as the first call. <br>
   * The methodToCall has to be a public static method with signature staring with return value and
   * the rest matching the methodToPatch, <br>
   * If methodToPatch is not static, the second argument of methodToCall will be the instance of the
   * class
   *
   * @param methodToPatch full name package1.class2.methodName1
   * @param methodToCall full name package1.class2.methodName2
   */
  public static void suffixCallTyped(String methodToPatch, String methodToCall) {
    MethodRef from = asMethodRef(methodToPatch);
    MethodRef to = asMethodRef(methodToCall);
    var suffix = InstrumentationConfig.addSuffixCall(from, to);
    suffix.suffixWithReturnValue = true; // Will be passed as first argument
    suffix.suffixWithThis = true; // Will be passed as second argument if not static
    suffix.suffixWithArgs = true; // Rest of the arguments
    suffix.noArgumentBoxing = true; // No boxing
    suffix.suffixReplacesReturnValue = true; // Replaces the return value
  }

  /**
   * Instrument @methodToPatch to dump node and jvm stack if called while marked entity is
   * constructing
   *
   * @see InstrumentationCmds#markScenarioStackAsInitializing(java.lang.String)
   * @param methodToPatch full name package1.class2.methodName1
   */
  public static void prefixCallWithDumpOnEntityConstructing(String methodToPatch) {
    prefixCall(methodToPatch, "optimus.graph.InstrumentationSupport.dumpStackIfEntityConstructing");
  }

  /**
   * Instrument @methodToPatch to report node and jvm stack if called from a cacheable node
   * (transitively)
   *
   * @param methodToPatch full name package1.class2.methodName1
   */
  public static void reportIfTransitivelyCached(String methodToPatch) {
    prefixCall(methodToPatch, "optimus.debug.InstrumentedNotRTFunction.trigger");
  }

  /**
   * Inject prefix call InstrumentedModuleCtor.trigger in EvaluationContext.current Only useful if
   * you also executed markAllModuleCtors
   *
   * @see InstrumentationCmds#markAllModuleCtors()
   * @see InstrumentedModuleCtor#trigger()
   * @see RTVerifierCategory#MODULE_CTOR_EC_CURRENT
   * @see RTVerifierCategory#MODULE_CTOR_SI_NODE
   * @see RTVerifierCategory#MODULE_LAZY_VAL_EC_CURRENT
   */
  public static void prefixECCurrentWithTriggerIfInModuleCtor() {
    var moduleCtorTrigger = "optimus.debug.InstrumentedModuleCtor.trigger";
    prefixCall("optimus.graph.OGSchedulerContext.current", moduleCtorTrigger);
    /* eventually we will detect the methods with an annotation. Until then:
      @see optimus.graph.OGSchedulerContext#_TRACESUPPORT_unsafe_current()

    prefixCall("optimus.platform.ScenarioStack.getNode", moduleCtorTrigger);
    prefixCall("optimus.platform.ScenarioStack.env", moduleCtorTrigger);
    prefixCall("optimus.platform.ScenarioStack.getTrackingNodeID", moduleCtorTrigger);
    prefixCall("optimus.platform.ScenarioStack.getParentTrackingNode", moduleCtorTrigger);
    prefixCall("optimus.platform.ScenarioStack.pluginTags", moduleCtorTrigger);
    prefixCall("optimus.platform.ScenarioStack.findPluginTag", moduleCtorTrigger);
    */
  }

  /**
   * When markAllModuleCtors is requested this function allows additions to the exclusion list
   *
   * @param className JVM class name of the module
   * @see InstrumentationCmds#markAllModuleCtors()
   * @see InstrumentationCmds#markAllEntityCtorsForSIDetection()
   */
  public static void excludeFromModuleOrEntityCtorReporting(String className) {
    var jvmName = className.replace('.', '/');
    InstrumentationConfig.addModuleOrEntityExclusion(jvmName);
  }

  /**
   * When markAllModuleCtors or individual module bracketing is enabled, some call stacks can be
   * disabled
   *
   * @param methodToPatch fully specified method reference
   * @see InstrumentationCmds#markAllModuleCtors()
   * @see InstrumentationConfig#addModuleConstructionIntercept
   */
  public static void excludeMethodFromModuleCtorReporting(String methodToPatch) {
    MethodRef mref = asMethodRef(methodToPatch);
    var patch = addPrefixCall(mref, InstrumentationConfig.imcPause, false, false);
    addSuffixCall(mref, InstrumentationConfig.imcResume);
    patch.passLocalValue = true;
  }

  /**
   * Instrument entity @className constructors to call a prefix/postfix methods to mark/unmark
   * entity ctor as running
   *
   * @see InstrumentationCmds#markAllEntityCtorsForSIDetection()
   * @see InstrumentationCmds#prefixCallWithDumpOnEntityConstructing(java.lang.String)
   * @param className Fully specified package.subpackage.className
   */
  public static void markScenarioStackAsInitializing(String className) {
    var jvmName = className.replace('.', '/');
    InstrumentationConfig.addMarkScenarioStackAsInitializing(jvmName);
  }

  /**
   * Instrument all entity constructors to call a prefix/postfix methods to mark/unmark entity ctors
   * as running
   *
   * @see InstrumentationCmds#reportFindingTweaksInEntityConstructor()
   * @see InstrumentationCmds#reportTouchingTweakableInEntityConstructor()
   * @see RTVerifierCategory#TWEAK_IN_ENTITY_CTOR
   * @see RTVerifierCategory#TWEAKABLE_IN_ENTITY_CTOR
   */
  public static void markAllEntityCtorsForSIDetection() {
    instrumentAllEntities = EntityInstrumentationType.markScenarioStack;
  }

  /**
   * Instrument all module constructors to call a prefix/postfix methods to mark/unmark module ctors
   * as running
   *
   * @see RTVerifierCategory#MODULE_CTOR_EC_CURRENT
   * @see RTVerifierCategory#MODULE_CTOR_SI_NODE
   * @see RTVerifierCategory#MODULE_LAZY_VAL_EC_CURRENT
   */
  public static void markAllModuleCtors() {
    instrumentAllModuleConstructors = true;
  }

  /**
   * Instrument all classes that don't implement (and base class doesn't either) their own hashCode.
   * Therefore relying on identity hashCodes with calls to
   * InstrumentedHashCodes#hashCode(java.lang.Object)
   *
   * @apiNote Use to flag values that use identity hashCode while being used as a key in property
   *     caching
   * @see InstrumentedHashCodes#hashCode(java.lang.Object)
   */
  public static void reportSuspiciousHashCodesCalls() {
    instrumentAllHashCodes = true;
  }

  public static void reportSuspiciousEqualityCallsIfEnabled() {
    if (DiagnosticSettings.rtvNodeRerunnerSkipBadEquality) reportSuspiciousEqualityCalls();
  }

  /**
   * Instrument all classes that don't implement (and base class doesn't either) their own equals.
   *
   * @apiNote Use to flag values that use identity types that rely on Object.equals
   * @see InstrumentedEquals#equals(java.lang.Object, java.lang.Object)
   */
  public static void reportSuspiciousEqualityCalls() {
    instrumentEquals = true;
  }

  /**
   * Instrument callouts and report touching tweakables or entity ctor (which should be RT)
   *
   * @see InstrumentationCmds#markAllEntityCtorsForSIDetection()
   * @see RTVerifierCategory#TWEAKABLE_IN_ENTITY_CTOR
   */
  public static void reportTouchingTweakableInEntityConstructor() {
    InstrumentationConfig.addVerifyScenarioStackCalls();
    reportTouchingTweakable = true;
  }

  /**
   * Instrument callouts and report touching tweaked values or entity ctor (which should be RT)
   *
   * @see InstrumentationCmds#markAllEntityCtorsForSIDetection()
   * @see RTVerifierCategory#TWEAK_IN_ENTITY_CTOR
   */
  public static void reportFindingTweaksInEntityConstructor() {
    InstrumentationConfig.addVerifyScenarioStackCalls();
    reportFindingTweaks = true;
  }

  /**
   * Cache all entity creations. Instrument all def apply(): E on E$ extends EntityCompanion to
   * cache their values
   *
   * @apiNote Use to check the suspicions that entity have mutable fields OR refer to unique values
   *     that don't participate in Entity identity
   */
  public static void cacheAllEntities() {
    instrumentAllEntityApplies = true;
  }

  /** Instrument a given @methodRef to be cached */
  public static void cache(String methodRef) {
    var cacheFunction = asMethodRef(methodRef);
    addCacheFunction(cacheFunction);
  }

  /** Instrument Exception constructor to call NodeTrace */
  public static void traceSelfAndParentOnException() {
    InstrumentationConfig.addTraceSelfAndParentOnException();
  }

  /**
   * When traceSelfAndParentOnException or individual exception reporting is enabled, some call
   * stacks can be disabled
   *
   * @param methodToPatch fully specified method reference
   * @see InstrumentationCmds#traceSelfAndParentOnException()
   */
  public static void excludeMethodFromExceptionReporting(String methodToPatch) {
    MethodRef mref = asMethodRef(methodToPatch);
    var patch = addPrefixCall(mref, InstrumentationConfig.iecPause, false, false);
    addSuffixCall(mref, InstrumentationConfig.iecResume);
    patch.passLocalValue = true;
  }

  /**
   * Sometimes developers inadvertanly write non-RT constructors. Enabling this probe will often
   * find those issues
   */
  public static void verifyEntityDeepEqualityDuringCaching() {
    var suffix = addSuffixCall(equalsHook, expectEquals);
    suffix.suffixWithThis = true;
    suffix.suffixWithReturnValue = true;
    suffix.suffixWithArgs = true;
    suffix.noArgumentBoxing = true;
  }

  /**
   * Scala 2.13 has new implementations of HashSet and HashMap which give different iteration order
   * to those in Scala 2.12. In order to facilitate easy testing and debugging of hash iteration
   * order problems without fully recompiling to 2.13, this modification will alter the Scala 2.12
   * hash ordering (not to match 2.13, but to be different from normal 2.12).
   */
  public static void modifyScala212HashImprover(String cls) {
    var suffix =
        addSuffixCall(
            InstrumentationConfig.asMethodRef(cls, "improve"),
            InstrumentationConfig.asMethodRef(HashImproverModification.class, "modify"));
    // our patch will take the return value of improve and return a replacement return value,
    // without boxing
    suffix.suffixWithReturnValue = true;
    suffix.suffixReplacesReturnValue = true;
    suffix.noArgumentBoxing = true;
  }

  public static final class HashImproverModification {
    public static int modify(int hcode) {
      return ~hcode;
    }
  }
}
