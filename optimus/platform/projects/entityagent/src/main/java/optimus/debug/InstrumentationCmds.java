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
import static optimus.debug.InstrumentationConfig.*;


import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import optimus.graph.DiagnosticSettings;

@SuppressWarnings("unused") // A number of methods in the class are called via reflection when loaded from a config
public class InstrumentationCmds {

  private static class ScalaWorksheetAlmostParser extends StringTokenizer {
    final static String Q = "\"";
    final static String D = " \t\n\r\f,()";
    String function;
    ArrayList<Object> args = new ArrayList<>();
    ArrayList<Class<?>> argTypes = new ArrayList<>();

    ScalaWorksheetAlmostParser(String text) { super(text, D); }

    void parse() {
      if (hasMoreTokens())
        function = nextToken();
      while (hasMoreTokens()) {
        var lastToken = nextToken(D);
        if (lastToken.startsWith(Q) && lastToken.endsWith(Q)) {
          args.add(lastToken.substring(1, lastToken.length() - 1));
          argTypes.add(String.class);
        } else if (lastToken.startsWith("\"")) {
          var endToken = nextToken(Q);
          nextToken(D);   // eat the quote
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

  /** either from file (instrumentationConfig) or inline from system property (instrumentationCache) */
  static void loadCommands() {
    if (DiagnosticSettings.instrumentationCache != null) {
      for (var function : DiagnosticSettings.instrumentationCache)
        cache(function);
    }

    if (DiagnosticSettings.instrumentationConfig == null)
      return;
    try {
      List<String> lines = Files.readAllLines(Paths.get(DiagnosticSettings.instrumentationConfig));
      boolean inComment = false;  // Reading lines between /* and */
      for (String line : lines) {
        if (line.startsWith("import ") || line.startsWith("//"))
          continue;
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
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Inject into the Entity identified by className a method equalsForCaching that compares by identity
   * <p>The reason to do this, is to get cache hits only on exactly the same entity instance.
   * This in some cases avoids the issue with the entity caching some unstable value and re-using it in wrong context</p>
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
   * Instrument @methodToPatch to call @methodToCall as the first call
   * @param methodToPatch full name package1.class2.methodName1
   * @param methodToCall full name package1.class2.methodName2
   */
  public static void prefixCall(String methodToPatch, String methodToCall) {
    MethodRef from = asMethodRef(methodToPatch);
    MethodRef to = asMethodRef(methodToCall);
    InstrumentationConfig.addPrefixCall(from, to, false, false);
  }

  /**
   * Instrument @methodToPatch to dump node and jvm stack if called while marked entity is constructing
   * @see InstrumentationCmds#markScenarioStackAsInitializing(java.lang.String)
   * @param methodToPatch full name package1.class2.methodName1
   */
  public static void prefixCallWithDumpOnEntityConstructing(String methodToPatch) {
    prefixCall(methodToPatch, "optimus.graph.InstrumentationSupport.dumpStackIfEntityConstructing");
  }

  /**
   * Instrument @methodToPatch to dump node and jvm stack if called from a cacheable node (transitively)
   * @param methodToPatch full name package1.class2.methodName1
   */
  public static void prefixCallWithDumpOnTransitivelyCached(String methodToPatch) {
    prefixCall(methodToPatch, "optimus.graph.InstrumentationSupport.dumpStackIfTransitivelyCached");
  }

  /**
   * Inject prefix call InstrumentedModuleCtor.trigger in EvaluationContext.current
   * Only useful if you also executed markAllModuleCtors
   * @see optimus.debug.InstrumentationCmds#markAllModuleCtors()
   * @see optimus.debug.InstrumentedModuleCtor#trigger()
   */
  public static void prefixECCurrentWithTriggerIfInModuleCtor() {
    prefixCall("optimus.graph.OGSchedulerContext.current", "optimus.debug.InstrumentedModuleCtor.trigger");
  }

  /**
   * When markAllModuleCtors is requested this function allows additions to the exclusion list
   * @param className JVM class name of the module
   * @see optimus.debug.InstrumentationCmds#markAllModuleCtors()
   */
  public static void excludeModuleFromModuleCtorReporting(String className) {
    var jvmName = className.replace('.', '/');
    InstrumentationConfig.addModuleExclusion(jvmName);
  }

  /**
   * When markAllModuleCtors or individual module bracketing is enabled, some call stacks can be disabled
   * @param methodToPatch fully specified method reference
   * @see optimus.debug.InstrumentationCmds#markAllModuleCtors()
   * @see optimus.debug.InstrumentationConfig#addModuleConstructionIntercept
   */
  public static void excludeMethodFromModuleCtorReporting(String methodToPatch) {
    MethodRef mref = asMethodRef(methodToPatch);
    var patch = addPrefixCall(mref, InstrumentedModuleCtor.mrPause, false, false);
    addSuffixCall(mref, InstrumentedModuleCtor.mrResume);
    patch.passLocalValue = true;
  }

  /**
   * Instrument entity @className constructors to call a prefix/postfix methods to mark/unmark entity ctor as running
   * @see InstrumentationCmds#markAllEntityCtors()
   * @see InstrumentationCmds#prefixCallWithDumpOnEntityConstructing(java.lang.String)
   * @param className Fully specified package.subpackage.className
   */
  public static void markScenarioStackAsInitializing(String className) {
    var jvmName = className.replace('.', '/');
    InstrumentationConfig.addMarkScenarioStackAsInitializing(jvmName);
  }

  /**
   * Instrument all entity constructors to call a prefix/postfix methods to mark/unmark entity ctors as running
   */
  public static void markAllEntityCtors() {
    instrumentAllEntities = true;
  }

  /**
   * Instrument all module constructors to call a prefix/postfix methods to mark/unmark module ctors as running
   */
  public static void markAllModuleCtors() {
    instrumentAllModuleConstructors = true;
  }

  /**
   * Instrument all classes that don't implement (and base class doesn't either) their own hashCode.
   * Therefore relying on identity hashCodes with calls to optimus.debug.InstrumentedHashCodes#hashCode(java.lang.Object)
   * @apiNote Use to flag values that use identity hashCode while being used as a key in property caching
   * @see optimus.debug.InstrumentedHashCodes#hashCode(java.lang.Object)
   */
  public static void reportSuspiciousHashCodesCalls() {
    instrumentAllHashCodes = true;
  }

  /**
   * Cache all entity creations.
   * Instrument all def apply(): E on E$ extends EntityCompanion to cache their values
   * @apiNote Use to check the suspicions that entity have mutable fields OR refer to unique values that don't participate
   * in Entity identity
   */
  public static void cacheAllEntities() {
    instrumentAllEntityApplies = true;
  }

  /**
   * Instrument a given @methodRef to be cached
   */
  public static void cache(String methodRef) {
    var cacheFunction = asMethodRef(methodRef);
    addCacheFunction(cacheFunction);
  }
}