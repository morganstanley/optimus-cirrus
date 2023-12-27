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
package optimus;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.lang.instrument.UnmodifiableClassException;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarFile;

import optimus.debug.InstrumentationConfig;
import optimus.debug.InstrumentationInjector;
import optimus.graph.DiagnosticSettings;
import optimus.graph.LockInjector;
import optimus.graph.chaos.ChaosMonkeyInjector;
import optimus.graph.cleaner.CleanerInjector;
import optimus.junit.CachingJunitRunnerInjector;
import optimus.junit.OptimusTestWorkerClientInjector;
import optimus.systemexit.ExitInterceptProp;
import optimus.testidle.TestIdle;
import optimus.testidle.TestIdleTransformer;

public class EntityAgent {
  private static final boolean silent = Boolean.getBoolean("optimus.entityagent.silent");
  private static Instrumentation instrumentation;

  private static final String nativePrefix = "entityagent_";

  public static String nativePrefix(String name) {
    return nativePrefix + name;
  }

  private static final ConcurrentHashMap<String, ClassFileTransformer> customTransformers =
      new ConcurrentHashMap<>();
  private static final String VERSION_STRING = "Entity Agent(v8)";

  public static void logMsg(String msg) {
    if (!silent) {
      System.out.println(VERSION_STRING + ": " + msg);
    }
  }

  public static void logErrMsg(String msg) {
    if (!silent) {
      System.err.println(VERSION_STRING + ": " + msg);
    }
  }

  private static class EntityAgentTransformer implements ClassFileTransformer {
    boolean canTransformCoreClasses;

    EntityAgentTransformer(boolean canTransformCoreClasses) {
      this.canTransformCoreClasses = canTransformCoreClasses;
    }

    NodeMethodsInjector nmi = new NodeMethodsInjector();
    ChaosMonkeyInjector chaosInjector = new ChaosMonkeyInjector();
    CollectionInjector collInjector = new CollectionInjector();
    SyntheticMethodInjector syntheticMethodInjector = new SyntheticMethodInjector();
    CachingJunitRunnerInjector cachingJunitInjector = new CachingJunitRunnerInjector();
    OptimusTestWorkerClientInjector optimusTestWorkerClientInjector =
        new OptimusTestWorkerClientInjector();
    InstrumentationInjector instrInjector = new InstrumentationInjector();
    HotCodeReplaceTransformer hotCodeReplaceTransformer =
        new HotCodeReplaceTransformer(instrumentation);
    CleanerInjector cleanerInjector = new CleanerInjector();

    {
      if (DiagnosticSettings.enableHotCodeReplace) {
        hotCodeReplaceTransformer.startPolling();
      }
    }

    @Override
    public byte[] transform(
        ClassLoader loader,
        String clsName,
        Class<?> clsRedefined,
        ProtectionDomain domain,
        byte[] classfileBuffer)
        throws IllegalClassFormatException {
      if (clsName == null) return null; // Handle nameless classes

      byte[] transformed = classfileBuffer;

      if (DiagnosticSettings.enableHotCodeReplace) {
        transformed =
            safeTransform(
                hotCodeReplaceTransformer, loader, clsName, clsRedefined, domain, transformed);
      }

      if (DiagnosticSettings.rewriteDisposable) {
        transformed =
            safeTransform(cleanerInjector, loader, clsName, clsRedefined, domain, transformed);
      }

      if (DiagnosticSettings.chaosEnabled) {
        transformed =
            safeTransform(chaosInjector, loader, clsName, clsRedefined, domain, transformed);
      }

      if (InstrumentationConfig.isEnabled() & canTransformCoreClasses)
        transformed =
            safeTransform(instrInjector, loader, clsName, clsRedefined, domain, transformed);

      ClassFileTransformer cft = customTransformers.get(clsName);

      if (cft != null) {
        transformed = safeTransform(cft, loader, clsName, clsRedefined, domain, transformed);
      }

      if (DiagnosticSettings.injectNodeMethods) {
        transformed = safeTransform(nmi, loader, clsName, clsRedefined, domain, transformed);
      }

      if (DiagnosticSettings.collectionTraceEnabled) {
        transformed =
            safeTransform(collInjector, loader, clsName, clsRedefined, domain, transformed);
      }

      if (DiagnosticSettings.markGraphMethodsAsSynthetic) {
        transformed =
            safeTransform(
                syntheticMethodInjector, loader, clsName, clsRedefined, domain, transformed);
      }

      if (DiagnosticSettings.isClassMonitorEnabled) {
        if (DiagnosticSettings.enableJunitRunnerMonitorInjection) {
          transformed =
              safeTransform(
                  cachingJunitInjector, loader, clsName, clsRedefined, domain, transformed);
        }
        transformed =
            safeTransform(
                optimusTestWorkerClientInjector,
                loader,
                clsName,
                clsRedefined,
                domain,
                transformed);
      }

      // This transformation should be last in order so we won't miss usage of some class added by
      // previous transformations
      if (DiagnosticSettings.isClassMonitorEnabled
          && !clsName.equals(ClassMonitorInjector.class.getName())) {
        // The agent should load the app's CMI, and not use its own, since its own CMI is unaware of
        // optimus classes
        ClassFileTransformer classMonitorInjector = ClassMonitorInjector.instance(loader);
        if (classMonitorInjector != null) {
          transformed =
              safeTransform(
                  classMonitorInjector, loader, clsName, clsRedefined, domain, transformed);
        }
      }

      // Not a transformation
      if (DiagnosticSettings.classDumpEnabled) {
        if (DiagnosticSettings.classDumpClasses != null
            && DiagnosticSettings.classDumpClasses.contains(clsName)) {
          try {
            BiopsyLab.dumpClass(clsName, transformed == null ? classfileBuffer : transformed);
          } catch (IOException ioe) {
            throw new RuntimeException(clsName, ioe);
          }
        }
        if (DiagnosticSettings.classBiopsyClasses != null
            && DiagnosticSettings.classBiopsyClasses.contains(clsName)) {
          BiopsyLab.takeBiopsyOfClass(clsName, classfileBuffer, transformed);
        }
      }

      // the contract is that we return null if we didn't modify the class
      if (transformed == classfileBuffer) {
        return null;
      } else {
        return transformed;
      }
    }

    // the transform method is annoyingly non-chainable because it returns null if it isn't
    // interested in the class.
    // this method adapts it to return the input instead
    private byte[] safeTransform(
        ClassFileTransformer transformer,
        ClassLoader loader,
        String className,
        Class<?> classBeingRedefined,
        ProtectionDomain protectionDomain,
        byte[] input)
        throws IllegalClassFormatException {
      try {
        byte[] transformed =
            transformer.transform(loader, className, classBeingRedefined, protectionDomain, input);
        if (transformed == null) {
          return input;
        } else {
          return transformed;
        }
      } catch (IllegalClassFormatException e) {
        throw e;
      } catch (Throwable t) {
        logErrMsg("safeTransform error: '" + transformer.getClass().getName() + "': " + t);
        return input;
      }
    }
  }

  public static void premain(String agentArgs, Instrumentation instrumentation) throws Exception {
    if (EntityAgent.instrumentation != null) {
      logMsg("already loaded.");
      return;
    }

    InstrumentationConfig.init();

    // To patch java core classes to call our methods, we need to explicitly put the jar containing
    // those methods on the
    // bootstrap class loader path.  To find that jar, we let our current classloader search for a
    // class we added just to be
    // searched for.

    var allowCorePatches = false;
    try {
      var cn = "optimus.DoNotUseMeOutsideEntityAgent";
      var extJar = Class.forName(cn).getProtectionDomain().getCodeSource().getLocation().getPath();
      instrumentation.appendToBootstrapClassLoaderSearch(new JarFile(extJar));
      logMsg("Added " + extJar + " on classpath to bootstrap path.");
      allowCorePatches = true;
    } catch (Throwable e) {
      var agentPath =
          EntityAgent.class.getProtectionDomain().getCodeSource().getLocation().getPath();
      var possibleEAExt =
          new File(agentPath)
              .getParentFile()
              .listFiles((dir, name) -> name.contains("entityagent-ext"));
      if (possibleEAExt != null && possibleEAExt.length > 0) {
        var extJar = possibleEAExt[0];
        instrumentation.appendToBootstrapClassLoaderSearch(new JarFile(extJar));
        logMsg("Added " + extJar + " from directory to bootstrap path");
        allowCorePatches = true;
      }
    }
    if (!allowCorePatches) logMsg("Cannot patch core classes.");

    System.setProperty("ENTITY_AGENT_VERSION", VERSION_STRING);
    EntityAgent.instrumentation = instrumentation;
    logMsg(
        "supports "
            + (instrumentation.isRetransformClassesSupported() ? "retransform " : " ")
            + new Date()
            + " trace: "
            + DiagnosticSettings.traceAvailable
            + " isClassMonitorEnabled: "
            + DiagnosticSettings.isClassMonitorEnabled);
    String propOverride = System.getProperty("optimus.entityagent.props.override");
    if (propOverride != null) {
      agentArgs = propOverride;
    }
    String[] props = agentArgs == null ? new String[0] : agentArgs.split(";");
    if (propOverride != null) {
      props = propOverride.split(";");
    }
    String lockPrefix = "lock=";
    for (String prop : props) {
      if (prop.equals("doublebox")) {
        insertDoubleBoxTransformer();
      } else if (prop.startsWith(lockPrefix)) {
        String className = prop.substring(lockPrefix.length());
        // (don't use a lambda here because ClassFileTransformer isn't a SAM interface in Java 9+)
        customTransformers.put(
            className,
            new ClassFileTransformer() {
              @Override
              public byte[] transform(
                  ClassLoader loader,
                  String name,
                  Class<?> redef,
                  ProtectionDomain protectionDomain,
                  byte[] classfileBuffer) {
                logMsg("injecting locks on native methods in " + className);
                return LockInjector.inject(classfileBuffer);
              }
            });
      }
    }

    customTransformers.put("optimus/graph/NodeTask", new NodeTaskTransformer());

    if (DiagnosticSettings.enablePerNodeTPDMask) {
      customTransformers.put("optimus/core/TPDMask", new TPDMaskTransfomer());
    }

    if (System.getProperty(TestIdle.monitorIdlePropertyName) != null) {
      instrumentation.addTransformer(new TestIdleTransformer(), true);
    }

    if (DiagnosticSettings.useStrictMath) {
      strictenMath();
    }
    // org/apache/logging/log4j/core/lookup/JndiLookup is used in LogJam exploit
    instrumentation.addTransformer(new JndiDefanger());

    EntityAgentTransformer transformer = new EntityAgentTransformer(allowCorePatches);
    instrumentation.addTransformer(transformer, true);

    if (System.getProperty(ExitInterceptProp.name) != null) {
      InstrumentationConfig.addSystemExitPrefix();
      instrumentation.retransformClasses(java.lang.System.class);
    }

    if (instrumentation.isNativeMethodPrefixSupported())
      instrumentation.setNativeMethodPrefix(transformer, nativePrefix);
    else logErrMsg("Native Method Prefix not supported. Skipping...");
  }

  /*
   * java.lang.Double.valueOf does not intern common double values,
   * and analysis of several business applications have shown that this
   * can result in as much as 25% of the heap being used to store,
   * literally, zero: e.g. OPTIMUS-48561
   *
   * This agent replaces all calls to Double.valueOf with a call to an
   * interning replacement.
   *
   * Although it is possible to fix this at the scala level, much of
   * the boxing is still occurring in Java middleware and unaffected
   * by fixing it at a Scala level.
   */
  private static void insertDoubleBoxTransformer() {
    logMsg(
        "rewriting java.lang.Double.valueOf to optimus.DoubleBox.boxToDouble for selected classes.");

    List<String> doubleBoxTargets = new ArrayList<>();
    // these catch all boxings in scala code (even our dependencies)
    doubleBoxTargets.add("scala/Predef$");
    doubleBoxTargets.add("scala/runtime/BoxesRunTime");
    // See OPTIMUS-48561 for details.
    // a significant amount of zeros are coming from business model code generated by JAXB
    // and are being inserted by...
    //   com/sun/xml/bind/v2/model/impl/RuntimeBuiltinLeafInfoImpl$20
    // which is brittle, so we match on the containing class.
    doubleBoxTargets.add("com/sun/xml/bind/v2/model/impl/RuntimeBuiltinLeafInfoImpl");

    DoubleBox.boxToDouble(0.0); // avoid NoClassDefFoundError later
    ClassFileTransformer doubleBoxTransformer =
        new MethodRewriteTransformer(
            doubleBoxTargets, "java/lang/Double", "valueOf", "optimus/DoubleBox", "boxToDouble");
    instrumentation.addTransformer(doubleBoxTransformer, true);
  }

  private static void strictenMath() {
    logMsg("rewriting java.lang.Math calls to java.lang.StrictMath");
    instrumentation.addTransformer(new StrictMathTransformer(), false);
  }

  public static void retransform(Class<?> cls) {
    if (instrumentation != null && instrumentation.isModifiableClass(cls)) {
      try {
        instrumentation.retransformClasses(cls);
      } catch (UnmodifiableClassException e) {
        e.printStackTrace();
      }
    }
  }
}
