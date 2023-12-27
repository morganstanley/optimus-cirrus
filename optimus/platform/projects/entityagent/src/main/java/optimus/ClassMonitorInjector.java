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
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import optimus.deps.CmiCollectedDependencies;
import optimus.deps.CmiCollectionStateSnapshot;
import optimus.deps.CmiTransformationStateSnapshot;
import optimus.deps.DynamicDependencyDiscoveryClassVisitor;
import optimus.deps.OptimusDependencyDiscoveryClassVisitor;
import optimus.deps.OptimusMethodEntryDependencyDiscoveryClassVisitor;
import optimus.deps.ResourceAccessType;
import optimus.deps.ResourceDependency;
import optimus.deps.TransformationStatistics;
import optimus.deps.VisitContext;

import optimus.graph.DiagnosticSettings;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Type;

/**
 * This class is used to monitor all class usage across any program that is run with it enabled.
 * Thanks to it we can discover which classes were used for any particular test run, which enables
 * features like caching tests with a much finer granularity.
 *
 * <p>It is realized by injecting logging methods inside existing code every time: - new class is
 * created - getResource, getResourcesAsStream, loadClass or forName methods are called - class name
 * is used in annotation - static class method is called - static class field is accessed - classOf
 * or instanceof is used
 *
 * <p>When that happens we are logging which class depends on what (together with their sha1s and
 * URLs). Those results are stored in global map, which is persistent across multiple test runs. For
 * each test run we are also storing classes which were accessed during that particular run. At the
 * end of any test run we can find all classes on which given test depends by gathering: - all
 * classes which were accessed directly - all dependencies of that classes, all classDependencies of
 * classDependencies, and so on, recursively
 *
 * <p>Because of that map of dependencies we do not need to reload all classes for every single test
 * run, thus we can archive as small granularity as we want with a constant overhead (caused by
 * additional logging methods).
 *
 * <p>This solution still has some known deficiencies which can be fixed later on. Especially: - we
 * cannot discover shadowing without using classloaders on the cached test side - with multiple
 * class loaders we need to have way to uniquely store logging calls results (or a way to accumulate
 * results from multiple class loaders)
 *
 * <p>See optimus.dtc.DTC for more details.
 */
public class ClassMonitorInjector implements ClassFileTransformer {
  // Out-of-process UI tests typically use 40-45k (as of early 2020)
  private static final int estimatedNumberOfClasses = 50000;

  // Set to 0 to disable
  private static final int NOT_COUNTING_HITS = 0;
  private static final int showTopN = DiagnosticSettings.showThisNumberOfTopUsedClasses;

  private static final boolean isIJ = System.getenv().containsKey("STRATO_INTELLIJ");

  // WARNING: Source of Thread Contention
  // very frequent, repeated access across all threads
  private static final Map<String, Integer> usedClasses =
      new ConcurrentHashMap<>(estimatedNumberOfClasses);

  // cache for optimus class check
  // -- only populated during transformation
  private static final Map<String, ClassLoader> allTransformedOptimusClassesAndTheirClassloader =
      new ConcurrentHashMap<>();
  // mapping of a class and its dependencies (other classes)
  // -- populated during transformation, except for some resources when it happens during execution
  private static final ConcurrentDependencyMap<String> classDependencies =
      new ConcurrentDependencyMap<>();
  // mapping of a class and its dependencies (resources)
  // -- only populated during execution
  private static final ConcurrentDependencyMap<ResourceDependency> resourceDependencies =
      new ConcurrentDependencyMap<>();

  // cache for optimus jar check
  // -- populated during transformation and dependencies grinding (after cache lookup)
  private static final Map<String, Boolean> cachedIsOptimusJar = new ConcurrentHashMap<>();
  private static final Map<String, Boolean> cachedIsOptimusClass = new ConcurrentHashMap<>();

  public static final String javaLangClass = "java/lang/Class";
  public static final String javaLangClassLoader = "java/lang/ClassLoader";
  public static final String classExtension = ".class";

  // This is to prevent lumping test classes by accident. This is meant only for production code.
  public static class ExemptedPackage {
    final String packageBase;
    final String buildObtJarPrefix;
    final String installJarName;

    ExemptedPackage(String packageBase, String buildObtJarPrefix, String installJarName) {
      this.packageBase = packageBase;
      this.buildObtJarPrefix = buildObtJarPrefix;
      this.installJarName = installJarName;
    }

    public boolean isExempted(ClassLoader classLoader, String classResourceName) {
      if (classResourceName.startsWith(packageBase)) {
        URL resourceUrl = classLoader.getResource(classResourceName);
        if (resourceUrl != null) {
          String jarfileName = resourceUrl.getPath().split("!")[0];
          return jarfileName.contains(buildObtJarPrefix) || jarfileName.endsWith(installJarName);
        }
      }
      return false;
    }
  }

  // Exempt are DTC, CMI and their related structures
  // This has positive performance impact, eliminates thread contention and even CHM deadlock.
  public static final Set<String> instrumentationExemptedClasses =
      new HashSet<>(
          Arrays.asList(
              "optimus/ClassMonitorInjector",
              "optimus/deps/CmiCollectedDependencies",
              "optimus/deps/CmiCollectionStateSnapshot",
              "optimus/deps/CmiTransformationStateSnapshot",
              "optimus/deps/ResourceAccessType",
              "optimus/deps/ResourceDependency",
              "optimus/deps/VisitContext",
              "optimus/dtc/DependenciesCollator$",
              "optimus/dtc/DependenciesCollector",
              "optimus/dtc/DependenciesCollector$",
              "optimus/dtc/DependenciesGrinder",
              "optimus/dtc/DependenciesGrinder$",
              "optimus/dtc/DependenciesNarrator",
              "optimus/dtc/DependenciesNarrator$",
              "optimus/dtc/DependenciesNormalizer",
              "optimus/dtc/DependenciesSanitizer",
              "optimus/dtc/DependencyCleanUpEffect",
              "optimus/dtc/DtcContextualizedArgument",
              "optimus/dtc/DtcContextualizedDependencies",
              "optimus/dtc/DtcContextualizedEnvironmentalDependencies",
              "optimus/dtc/DtcContextualizedJar",
              "optimus/dtc/DtcContextualizedResource",
              "optimus/dtc/DtcContextualizedValue",
              "optimus/dtc/DtcMarker$",
              "optimus/dtc/DtcNormalizedDependencies",
              "optimus/dtc/DtcNormalizedDependencies$",
              "optimus/dtc/DtcNormalizedResource",
              "optimus/dtc/DtcNormalizedResource$",
              "optimus/dtc/Hasher",
              "optimus/dtc/cache/CachedRunsLookupError",
              "optimus/dtc/cache/CachedRunsManager",
              "optimus/dtc/cache/CachedRunsManager$",
              "optimus/dtc/cache/CachedRunsManagerDhtClient",
              "optimus/dtc/cache/CachedRunsManagerDhtClient$",
              "optimus/dtc/comparison/CacheVsLocalComparator",
              "optimus/dtc/comparison/CacheVsLocalComparator$",
              "optimus/dtc/comparison/Difference",
              "optimus/dtc/comparison/Difference$",
              "optimus/dtc/comparison/DifferenceReporter",
              "optimus/dtc/comparison/KeyValueChange",
              "optimus/dtc/comparison/MissingKeyValueLocally",
              "optimus/dtc/comparison/MissingLocally",
              "optimus/dtc/comparison/NewKeyValueLocally",
              "optimus/dtc/comparison/NewLocally",
              "optimus/dtc/comparison/OrderingDiscrepancy",
              // optimus/dtc/crumbs: We ignore as it is not on any hot loop
              "optimus/dtc/dht/DhtRegion$",
              "optimus/dtc/model/ApplicationArgumentsHelper$",
              "optimus/dtc/model/Argument",
              "optimus/dtc/model/ArgumentDependency",
              "optimus/dtc/model/ArgumentDependency$",
              "optimus/dtc/model/Classpath$",
              "optimus/dtc/model/ClasspathDependency",
              "optimus/dtc/model/ClasspathDependency$",
              "optimus/dtc/model/Dependency",
              "optimus/dtc/model/DtcCollectedHashedDependencies",
              "optimus/dtc/model/EnrichedOptimusCachedRun",
              "optimus/dtc/model/EnrichedOptimusCachedRun$",
              "optimus/dtc/model/EnvironmentVariableDependency",
              "optimus/dtc/model/EnvironmentVariableDependency$",
              "optimus/dtc/model/EnvironmentVariablesHelper",
              "optimus/dtc/model/FeatureFlagsString",
              "optimus/dtc/model/FeatureFlagsString$",
              "optimus/dtc/model/GenericArgumentsHelper$",
              "optimus/dtc/model/HashedDependency",
              "optimus/dtc/model/LookupResultType",
              "optimus/dtc/model/LookupResultType$",
              "optimus/dtc/model/NamedDependency",
              "optimus/dtc/model/OptimusCachedRun",
              "optimus/dtc/model/OptimusCachedRun$",
              "optimus/dtc/model/OptimusCachedRunPruner",
              "optimus/dtc/model/OptimusCachedRunPruner$",
              "optimus/dtc/model/OptimusCachedRunStats",
              "optimus/dtc/model/PathAndValueNormalizationHelper",
              "optimus/dtc/model/PathAndValueNormalizationHelper$",
              "optimus/dtc/model/PathSubstitution",
              "optimus/dtc/model/RecordedFiles",
              "optimus/dtc/model/RecordedResults",
              "optimus/dtc/model/ResourceDependency",
              "optimus/dtc/model/ResourceDependency$",
              "optimus/dtc/model/RunDependencies",
              "optimus/dtc/model/Substitution",
              "optimus/dtc/model/SystemPropertiesHelper",
              "optimus/dtc/model/SystemPropertyDependency",
              "optimus/dtc/model/SystemPropertyDependency$",
              "optimus/dtc/model/ValueSubstitution",
              "optimus/dtc/runners/DTCRunListener",
              "optimus/dtc/runners/DTCRunListener$",
              "optimus/dtc/runners/DefensiveDescriptionHelper$",
              "optimus/dtc/runners/RecordedRun",
              "optimus/dtc/runners/RunRecorder",
              "optimus/dtc/runners/TestAssumptionFailure",
              "optimus/dtc/runners/TestFailure",
              "optimus/dtc/runners/TestFinished",
              "optimus/dtc/runners/TestIgnored",
              "optimus/dtc/runners/TestRunFinished",
              "optimus/dtc/runners/TestRunStarted",
              "optimus/dtc/runners/TestStarted",
              "optimus/dtc/runners/UnitTestResult",
              "optimus/dtc/utils/CachedTestResults",
              "optimus/dtc/utils/CachedTestResults$",
              "optimus/dtc/utils/CmiEventsDescription",
              "optimus/dtc/utils/CmiEventsHelper$",
              "optimus/dtc/utils/DHTFailSafeExecution",
              "optimus/dtc/utils/DTCConstants$",
              "optimus/dtc/utils/DTCRuntimeContext$",
              "optimus/dtc/utils/FailSafeExecution",
              "optimus/dtc/utils/RunDependenciesUtils",
              "optimus/dtc/utils/RunDependenciesUtils$",
              "optimus/utils/Explanation",
              "optimus/utils/Explanation$",
              "optimus/utils/ExplanationState",
              "optimus/utils/ExplanationState$",
              "optimus/utils/StringDiffExplainer",
              "optimus/utils/StringDiffExplainer$",
              "optimus/utils/Tabulator",
              "optimus/utils/Tabulator$"));
  public static final List<ExemptedPackage> instrumentationExemptedPackages =
      Arrays.asList(
          new ExemptedPackage("optimus/graph/", "optimus.platform.core.main", "core.jar"),
          new ExemptedPackage(
              "optimus/dtc/", "optimus.platform.dtc_collector.", "dtc_collector.jar"),
          new ExemptedPackage("optimus/dtc/", "optimus.platform.dtc_runners.", "dtc_runners.jar"));
  // CAUTION: This list is dynamic and cannot be used to analyze cached dependencies!
  //          Used only by DTC and CMI internals
  public static final List<String> classesFromExemptedPackages = new CopyOnWriteArrayList<>();

  // internal error/warning tracking--can't use logback or console
  // -- populated during transformation and execution
  private static final List<String> internalEvents = new CopyOnWriteArrayList<>();

  public static final String CMI_ERROR = "[CMI:error]";
  public static final String CMI_WARN = "[CMI:warn]";
  public static final String CMI_INFO = "[CMI:info]";
  private static final Path localJarPathPrefix;
  private static final Path rejectedLocalJarPathPrefix;

  static {
    Path prefix;
    Path rejectedPrefix;
    try {
      prefix = getCodetreeArtifactPrefix();
      rejectedPrefix = prefix != null ? prefix.resolve("ide_config") : prefix;
    } catch (Throwable t) {
      prefix = null;
      rejectedPrefix = null;
    }
    localJarPathPrefix = prefix;
    rejectedLocalJarPathPrefix = rejectedPrefix;
  }

  private void markTransformationUnsafe(String transformedClassName, String throwableText) {
    recordInternalEvent(
        String.format(
            "%s error during transformation of class %s: %s",
            CMI_ERROR, transformedClassName, throwableText));
  }

  private static final TransformationStatistics statistics = new TransformationStatistics();

  public static CmiTransformationStateSnapshot getTransformationStateSnapshot() {
    return new CmiTransformationStateSnapshot(
        statistics.ignored.get(),
        statistics.unchanged.get(),
        statistics.visited.get(),
        statistics.instrumented.get(),
        statistics.optimusClasses.get(),
        statistics.failures.get());
  }

  private static Path getCodetreeArtifactPrefix() throws MalformedURLException, URISyntaxException {
    URL resourceURL =
        ClassMonitorInjector.class
            .getClassLoader()
            .getResource(constructDependencyName(ClassMonitorInjector.class.getName()));
    if (resourceURL != null) {
      Path thisJarPath = Paths.get(new URL(resourceURL.getPath().split("!")[0]).toURI());
      boolean isBuildDirJar = thisJarPath.getFileName().toString().contains("HASH");
      if (isBuildDirJar) {
        return thisJarPath.getParent().getParent().getParent().getParent();
      }
    }
    // null otherwise as we can do the check using manifest values
    return null;
  }

  // When a class is loaded, it means it was truly needed.
  // `cachedIsOptimusClass` will contain more entries than
  // `allTransformedOptimusClassesAndTheirClassloader` as
  // we discover more static dependencies than are dynamically exercised. We care only about the
  // actual dependency
  // with the least overhead possible: we do end up erring on the side of caution with more
  // dependencies than truly are.
  public static boolean isLoadedOptimusClass(String className) {
    return allTransformedOptimusClassesAndTheirClassloader.containsKey(className);
  }

  public static void recordInternalEvent(String event) {
    internalEvents.add(event);
  }

  private static String transformToClassResourceName(String className) {
    return className.replace("[]", "") + classExtension;
  }

  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer) {

    // Deal with arrays of some type by removing the square brackets
    String classResourceName = transformToClassResourceName(className);
    // Lack of protection domain suggest jdk classes
    // Exclude jdk internals (hiding being official APIs) and a few frameworks
    if (protectionDomain == null
        || className.startsWith("jdk/")
        || className.startsWith("junit/")
        || className.startsWith("org/junit/")
        || className.startsWith("org/scalatest/")
        || className.startsWith("scala/collection/")
        || (isIJ && className.startsWith("com/intellij/"))) {
      // Do not instrument these
      statistics.ignored.incrementAndGet();
      return classfileBuffer;
    }
    try {
      boolean optimusClass = isOptimusClass(loader, classResourceName);
      if (optimusClass) {
        // we don't calculate the SHA here because we may not need them
        // (e.g. if run in grid engine, we only pass the class names back)
        allTransformedOptimusClassesAndTheirClassloader.put(classResourceName, loader);
        statistics.optimusClasses.incrementAndGet();
      }

      List<Class<? extends ClassVisitor>> visitors = new ArrayList<>();
      final boolean instrumentClass;
      if (optimusClass) {
        visitors.add(OptimusDependencyDiscoveryClassVisitor.class);

        // Instrumenting (i.e. adding instructions) is optional, but we still need to collect static
        // dependencies!
        boolean exemptedPackage =
            instrumentationExemptedPackages.stream()
                .anyMatch(iep -> iep.isExempted(loader, classResourceName));
        instrumentClass = !instrumentationExemptedClasses.contains(className) && !exemptedPackage;
        if (instrumentClass) {
          visitors.add(OptimusMethodEntryDependencyDiscoveryClassVisitor.class);
        }
        if (exemptedPackage) {
          classesFromExemptedPackages.add(classResourceName);
        }
      } else {
        // Always instrument third party classes for resources and dynamic class usage
        instrumentClass = true;
      }
      if (instrumentClass) {
        visitors.add(DynamicDependencyDiscoveryClassVisitor.class);
        statistics.instrumented.incrementAndGet();
      } else {
        statistics.unchanged.incrementAndGet();
      }
      if (visitors.size() > 0) {
        VisitContext context = new VisitContext(loader, className, classResourceName);
        ClassReader classReader = new ClassReader(classfileBuffer);
        ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);
        ClassVisitor finalVisitor = classWriter;
        for (Class<? extends ClassVisitor> visitor : visitors) {
          finalVisitor =
              visitor
                  .getConstructor(VisitContext.class, ClassVisitor.class)
                  .newInstance(context, finalVisitor);
        }
        statistics.visited.incrementAndGet();
        classReader.accept(finalVisitor, 0);
        context.wrapUp();
        return classWriter.toByteArray();
      } else {
        statistics.ignored.incrementAndGet();
        return classfileBuffer;
      }
    } catch (Throwable t) {
      markTransformationUnsafe(className, t.toString());
      statistics.failures.incrementAndGet();
      return classfileBuffer;
    }
  }

  public static synchronized void clearAllUsageData() {
    usedClasses.clear();
    internalEvents.clear();
  }

  // Only to peek at internal structure
  public static synchronized Map<String, Set<String>> getClassDependencies() {
    return classDependencies.getSafeCopy();
  }

  // Only to peek at internal structure
  public static synchronized Map<String, Set<ResourceDependency>> getResourceDependencies() {
    return resourceDependencies.getSafeCopy();
  }

  // [DependenciesCollector] Used exclusively by the collector: refer to it instead
  public static CmiCollectedDependencies collectLocalRawDependenciesAndReset() {
    return getUsedRawDependencies(true);
  }

  public static void logClassUsage(String className) {
    if (showTopN > NOT_COUNTING_HITS) {
      usedClasses.compute(className, (key, v) -> v != null ? v + 1 : 1);
    } else {
      usedClasses.putIfAbsent(className, NOT_COUNTING_HITS); // Not counting + optimization
    }
  }

  public static void logClassUsage(Class<?> clazz) {
    logClassUsage(constructDependencyName(clazz.getName()));
  }

  public static void logClassAndResourceUsage(
      Object dependency, boolean isClassDependency, String className) {
    logClassUsage(className);
    if (dependency != null) {
      if (isClassDependency) {
        Class<?> dependencyClass =
            (dependency instanceof Class) ? (Class<?>) dependency : dependency.getClass();
        computeAndAddClassDependency(
            dependencyClass.getClassLoader(),
            className,
            constructDependencyName(dependencyClass.getName()));
      } else {
        if (dependency instanceof ResourceDependency) {
          computeAndAddResourceDependency(className, (ResourceDependency) dependency);
        } else if (!dependency.toString().endsWith(classExtension)) {
          computeAndAddResourceDependency(
              className, new ResourceDependency(dependency.toString(), ResourceAccessType.input));
        } else {
          recordInternalEvent(
              String.format(
                  "%s Unmanaged resource type %s from %s: add code to ignore or handle",
                  CMI_WARN, dependency, className));
        }
      }
    }
  }

  public static void logFileResourceUsage(File resource, String accessMode, String className) {
    logFileResourceUsage(resource.getAbsolutePath(), accessMode, className);
  }

  public static void logFileResourceUsage(
      String resourcePath, String accessMode, String className) {
    // Ignore jars: they are covered by class path
    if (!resourcePath.endsWith(".jar")) {
      logClassAndResourceUsage(
          new ResourceDependency(resourcePath, ResourceAccessType.fromFileAccessMode(accessMode)),
          false,
          className);
    }
  }

  public static void logUriResourceUsage(URI resource, String className) {
    logNetworkResourceUsage(resource.toString(), className);
  }

  private static boolean isClassOrJarResource(URL resource) {
    return (resource.getProtocol().equals("jar") || resource.getProtocol().equals("file"))
        && (resource.getPath().endsWith(classExtension) || resource.getPath().endsWith(".jar"));
  }

  public static void logUrlResourceUsage(URL resource, String className) {
    if (resource == null || isClassOrJarResource(resource)) {
      // Not interested in these cases
      return;
    }
    if (resource.getProtocol().equals("jar")) {
      logClassAndResourceUsage(
          new ResourceDependency(resource.toString(), ResourceAccessType.input), false, className);
    } else if (resource.getProtocol().equals("file")) {
      try {
        // This occurs because windows local absolute paths (e.g. D:\foo\bar) get encoded as
        // file:////D:/foo/bar,
        // so the "path" component of the URL starts with ////D:/, not D:. We need to remove the
        // ////
        Path path =
            resource.getPath().startsWith("////")
                ? Paths.get(resource.getPath().substring(4))
                : Paths.get(resource.toURI());
        logFileResourceUsage(path.toFile(), "r", className);
      } catch (Exception e) {
        // This time, try URL instead before throwing the towel
        try {
          Path path = Paths.get(resource.getPath());
          logFileResourceUsage(path.toFile(), "r", className);
        } catch (Exception e2) {
          recordInternalEvent(
              String.format(
                  "%s Cannot parse URI %s from %s: ignored due to '%s' and '%s'",
                  CMI_WARN, resource, className, e, e2));
          logFileResourceUsage(resource.getPath(), "r", className);
        }
      }
    } else {
      logNetworkResourceUsage(resource.toString(), className);
    }
  }

  public static void logClassAndStreamResourceByNameUsage(
      ClassLoader classLoader, String name, String className) {
    if (name.endsWith(classExtension)) {
      logClassUsage(name);
    } else {
      URL url = classLoader.getResource(name);
      if (url != null) { // If null, no consequence as not found in scope
        logUrlResourceUsage(url, className);
      }
    }
  }

  public static void logClassAndStreamResourceByNameUsage(
      Class<?> clazz, String name, String className) {
    URL url = clazz.getResource(name);
    if (url != null) { // If null, no consequence as not found in scope
      logUrlResourceUsage(url, className);
    }
  }

  public static void logClassAndResourcesByNameUsage(
      ClassLoader classLoader, String name, String className) {
    try {
      Enumeration<URL> e = classLoader.getResources(name);
      while (e.hasMoreElements()) {
        logUrlResourceUsage(e.nextElement(), className);
      }
    } catch (IOException e) {
      // No consequence as not found in scope
    }
  }

  public static void logIpResourceUsage(String resource, String className) {
    logNetworkResourceUsage(resource, className);
  }

  public static void logIpPortResourceUsage(String resource, int port, String className) {
    logNetworkResourceUsage(String.format("%s:%d", resource, port), className);
  }

  public static void logIpPortResourceUsage(InetAddress resource, int port, String className) {
    logNetworkResourceUsage(String.format("%s:%d", resource.toString(), port), className);
  }

  private static void logNetworkResourceUsage(String networkAddress, String className) {
    logClassAndResourceUsage(
        new ResourceDependency(networkAddress, ResourceAccessType.network), false, className);
  }

  // ONLY meant for testing; use #collectLocalRawDependenciesAndReset() otherwise
  public static synchronized CmiCollectedDependencies getUsedRawDependencies(
      boolean clearInternalState) {
    Set<String> usedClassesCopy =
        instrumentationExemptedClasses.stream()
            .map(ClassMonitorInjector::constructDependencyName)
            .collect(Collectors.toSet());

    classesFromExemptedPackages.forEach(
        usedClassesCopy::add); // collection addAll has a bug, please to do not use
    usedClasses
        .keySet()
        .forEach(usedClassesCopy::add); // collection addAll has a bug, please to do not use
    if (showTopN > NOT_COUNTING_HITS) {
      List<Map.Entry<String, Integer>> sortedUsage =
          usedClasses.entrySet().stream()
              .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
              .collect(Collectors.toList());
      String topList =
          sortedUsage.subList(0, Math.min(showTopN, sortedUsage.size())).stream()
              .map((entry) -> String.format("%s (%d)", entry.getKey(), entry.getValue()))
              .collect(Collectors.joining("\n - "));
      recordInternalEvent(
          String.format(
              "%s Internal CMI state: top %d used classes: \n - %s.", CMI_INFO, showTopN, topList));
    }
    if (clearInternalState) {
      usedClasses.clear();
    }

    Set<String> allClassDependencies = new HashSet<>();
    Set<ResourceDependency> allResourceDependencies = new HashSet<>();
    for (String className : usedClassesCopy) {
      collectUsedClassesAndResources(className, allClassDependencies, allResourceDependencies);
    }

    // Delayed capture and reset -- MUST BE LAST THING TO DO BEFORE RETURNING!!!
    List<String> internalEventsCopy = new ArrayList<>();
    internalEvents.forEach(
        internalEventsCopy::add); // collection addAll has a bug, please to do not use
    if (clearInternalState) {
      internalEvents.clear();
    }

    return new CmiCollectedDependencies(
        usedClassesCopy,
        allClassDependencies,
        allResourceDependencies,
        internalEventsCopy,
        new CmiCollectionStateSnapshot(
            classDependencies.size(),
            classDependencies.getDependenciesCount(),
            resourceDependencies.size(),
            resourceDependencies.getDependenciesCount(),
            allTransformedOptimusClassesAndTheirClassloader.size(),
            cachedIsOptimusJar.values().stream().mapToInt(b -> b ? 1 : 0).sum(),
            cachedIsOptimusJar.size(),
            cachedIsOptimusClass.values().stream().mapToInt(b -> b ? 1 : 0).sum(),
            cachedIsOptimusClass.size()));
  }

  // If not found, the result set is empty
  public static Set<String> peekAtClassStaticDependencies(String className) {
    Set<String> allClassDependencies = new HashSet<>();
    Set<ResourceDependency> allResourceDependencies = new HashSet<>();
    collectUsedClassesAndResources(
        constructDependencyName(className), allClassDependencies, allResourceDependencies);
    allClassDependencies.removeIf(
        d -> !isLoadedOptimusClass(d)); // Remove stuff that are not class loaded yet
    return allClassDependencies;
  }

  // Strictly static dependencies as we discovered types that are referred to
  public static void computeAndAddClassDependency(
      ClassLoader loader, String className, String classDependency) {
    if (isOptimusClass(loader, classDependency)) {
      classDependencies.add(className, classDependency);
    }
  }

  // Runtime dependencies as we discover through API access
  private static void computeAndAddResourceDependency(
      String className, ResourceDependency resource) {
    resourceDependencies.add(className, resource);
  }

  private static synchronized void collectUsedClassesAndResources(
      String className,
      Set<String> allClassDependencies,
      Set<ResourceDependency> allResourceDependencies) {
    if (allClassDependencies.add(className)) {
      allResourceDependencies.addAll(resourceDependencies.getSafeCopy(className));
      for (String dependency : classDependencies.getSafeCopy(className)) {
        collectUsedClassesAndResources(dependency, allClassDependencies, allResourceDependencies);
      }
    }
  }

  // Only invoke if obtained from Class<?>.getName()
  public static String constructDependencyName(String dependencyName) {
    assert (!dependencyName.contains("[]"));
    return dependencyName.replace('.', '/') + classExtension;
  }

  // Optimized classes to minimize `replaceAll` overhead with the most efficient paths
  public static String constructDependencyNameFromSimpleType(Type dependencyType) {
    final String dependencyName;
    switch (dependencyType.getSort()) {
      case Type.ARRAY:
        dependencyName = dependencyType.getElementType().getInternalName();
        break;
      case Type.OBJECT:
        dependencyName = dependencyType.getInternalName();
        break;
      default:
        dependencyName = dependencyType.getClassName(); // Beautified version for native types
    }
    assert (!dependencyName.contains("."));
    return dependencyName + classExtension;
  }

  private static boolean isSyntheticDynamicClass(String resourcePath) {
    return resourcePath.contains("$MockitoMock$")
        || resourcePath.contains("$$FastClassByGuice$$")
        || resourcePath.contains("$$EnhancerBySpringCGLIB$$");
  }

  private static boolean rememberAsOptimusClass(String resourcePath, boolean isOptimusClass) {
    // Memory footprint reduction
    if (!isSyntheticDynamicClass(resourcePath)) {
      cachedIsOptimusClass.put(resourcePath, isOptimusClass);
    }
    return isOptimusClass;
  }

  private static final Set<String> nativeTypes =
      new HashSet<>(
          Arrays.asList(
              "int.class",
              "boolean.class",
              "long.class",
              "float.class",
              "double.class",
              "byte.class",
              "char.class",
              "short.class",
              "void.class"));

  // NOTE: This is on a hot critical path, so its performance is important
  public static boolean isOptimusClass(ClassLoader classLoader, String resourcePath) {

    if (cachedIsOptimusClass.containsKey(resourcePath)) {
      return cachedIsOptimusClass.get(resourcePath);
    }

    // Memory footprint reduction
    if (isSyntheticDynamicClass(resourcePath)) {
      return false;
    }

    if (nativeTypes.contains(resourcePath)) {
      return rememberAsOptimusClass(resourcePath, false);
    }

    // Costly lookup
    URL resourceUrl = classLoader.getResource(resourcePath);
    if (resourceUrl == null) {
      return rememberAsOptimusClass(resourcePath, false);
    }
    if (resourceUrl.getProtocol().equals("jrt")) {
      // jigsaw modular run-time images
      return rememberAsOptimusClass(resourcePath, false);
    }

    String jarfileName = resourceUrl.getPath().split("!")[0];
    if (cachedIsOptimusJar.containsKey(jarfileName)) {
      return rememberAsOptimusClass(resourcePath, cachedIsOptimusJar.get(jarfileName));
    }
    if (jarfileName.endsWith(".jar")) {
      try {
        boolean result = isOptimusJar(new URL(jarfileName));
        cachedIsOptimusJar.put(jarfileName, result);
        return rememberAsOptimusClass(resourcePath, result);
      } catch (IOException ignored) {
        return false;
      }
    } else {
      // If not a JAR, then it could be an unsupported protocol (as we discovered with jrt)
      // However, the Optimus Test Runner (OTR) does unpack some jars given some suites expect to
      // find files from filesystem
      // (i.e. do not use Class.getResource...), so they happen to be effectively from Optimus
      boolean looksUnpacked = resourceUrl.toString().contains("/classes/");
      if (!looksUnpacked) {
        // It does not look like it is related to Optimus Test Runner (OTR)
        recordInternalEvent(
            String.format(
                "%s Resource %s in %s is assumed from Optimus",
                CMI_INFO, resourcePath, resourceUrl));
      }
      return rememberAsOptimusClass(resourcePath, true);
    }
  }

  public static boolean isOptimusJar(URL jarfileURL) {

    if (localJarPathPrefix == null) { // not local obt, we can rely on presence of manifest
      try (JarInputStream jarstream = new JarInputStream(jarfileURL.openStream())) {
        Manifest manifest = jarstream.getManifest();
        if (manifest != null) {
          String obtVersion = manifest.getMainAttributes().getValue("Buildtool-Version");
          return obtVersion != null;
        }
      } catch (IOException e) {
        // Best effort check
      }
      return false;
    } else {
      try {
        Path jarPath = Paths.get(jarfileURL.toURI());
        return jarPath.startsWith(localJarPathPrefix)
            && !jarPath.startsWith(rejectedLocalJarPathPrefix);
      } catch (URISyntaxException e) {
        return false;
      }
    }
  }

  public static ClassFileTransformer instance(ClassLoader host) {
    try {
      Class<?> cmiLoadedByAppClassloader =
          Class.forName(ClassMonitorInjector.class.getName(), true, host);
      return (ClassFileTransformer)
          cmiLoadedByAppClassloader.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException ex) {
      // boot classloader, or another classloader not owned by us; skip
      return null;
    }
  }
}
