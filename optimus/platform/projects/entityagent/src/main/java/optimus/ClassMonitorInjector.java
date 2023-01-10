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
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import optimus.deps.CollectedDependencies;
import optimus.deps.DynamicDependencyDiscoveryClassVisitor;
import optimus.deps.InterProcessFile;
import optimus.deps.OptimusDependencyDiscoveryClassVisitor;
import optimus.deps.OptimusMethodEntryDependencyDiscoveryClassVisitor;
import optimus.deps.TransformationStatistics;
import optimus.deps.VisitContext;

import optimus.graph.DiagnosticSettings;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Type;

/**
 * This class is used to monitor all class usage across any program that is run with it enabled.
 * Thanks to it we can discover which classes were used for any particular test run,
 * which enables features like caching tests with a much finer granularity.
 * <p>
 * It is realized by injecting logging methods inside existing code every time:
 * - new class is created
 * - getResource, getResourcesAsStream, loadClass or forName methods are called
 * - class name is used in annotation
 * - static class method is called
 * - static class field is accessed
 * - classOf or instanceof is used
 * <p>
 * When that happens we are logging which class depends on what (together with their sha1s and URLs).
 * Those results are stored in global map, which is persistent across multiple test runs.
 * For each test run we are also storing classes which were accessed during that particular run.
 * At the end of any test run we can find all classes on which given test depends by gathering:
 * - all classes which were accessed directly
 * - all dependencies of that classes, all classDependencies of classDependencies, and so on, recursively
 * <p>
 * Because of that map of dependencies we do not need to reload all classes for every single test run,
 * thus we can archive as small granularity as we want with a constant overhead (caused by additional logging methods).
 * <p>
 * This solution still has some known deficiencies which can be fixed later on. Especially:
 * - we cannot discover shadowing without using classloaders on the cached test side
 * - with multiple class loaders we need to have way to uniquely store logging calls results
 * (or a way to accumulate results from multiple class loaders)
 * <p>
 * See optimus.dtc.DTC for more details.
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
  private static final Map<String, Integer> usedClasses = new ConcurrentHashMap<>(estimatedNumberOfClasses);

  // cache for optimus class check
  // -- only populated during transformation
  private static final Map<String, ClassLoader> allTransformedOptimusClassesAndTheirClassloader = new ConcurrentHashMap<>();
  // mapping of a class and its dependencies (other classes)
  // -- populated during transformation, except for some resources when it happens during execution
  private static final ConcurrentDependencyMap<String> classDependencies = new ConcurrentDependencyMap<>();
  // mapping of a class and its dependencies (resources)
  // -- only populated during execution
  private static final ConcurrentDependencyMap<ResourceDependency> resourceDependencies = new ConcurrentDependencyMap<>();

  // cache for optimus jar check
  // -- populated during transformation and dependencies grinding (after cache lookup)
  private static final Map<String, Boolean> cachedIsOptimusJar = new ConcurrentHashMap<>();
  private static final Map<String, Boolean> cachedIsOptimusClass = new ConcurrentHashMap<>();

  public static final String javaLangClass = "java/lang/Class";
  public static final String javaLangClassLoader = "java/lang/ClassLoader";
  public static final String classExtension = ".class";

  private static String contextTag = "";

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
  public static final List<String> instrumentationExemptedClasses = Arrays.asList("optimus/ClassMonitorInjector",
                                                                                  "optimus/ResourceAccessType",
                                                                                  "optimus/ResourceDependency",
                                                                                  "optimus/comparison/Explanation",
                                                                                  "optimus/comparison/Explanation$",
                                                                                  "optimus/comparison/ExplanationState",
                                                                                  "optimus/comparison/ExplanationState$",
                                                                                  "optimus/comparison/StringDiffExplainer",
                                                                                  "optimus/comparison/StringDiffExplainer$",
                                                                                  "optimus/comparison/Tabulator",
                                                                                  "optimus/comparison/Tabulator$");
  public static final List<ExemptedPackage> instrumentationExemptedPackages = Arrays.asList(new ExemptedPackage("optimus/graph/",
                                                                                                                "optimus.platform.core.main",
                                                                                                                "core.jar"),
                                                                                            new ExemptedPackage(
                                                                                                "optimus/dtc/",
                                                                                                "optimus.platform.distributed_test_cache.",
                                                                                                "distributed_test_cache.jar"));
  // CAUTION: This list is dynamic and cannot be used to analyze cached dependencies!
  //          Used only by DTC and CMI internals
  public static final List<String> classesFromExemptedPackages = new CopyOnWriteArrayList<>();

  // internal error/warning tracking--can't use logback or console
  // -- populated during transformation and execution
  private static final List<String> internalEvents = new CopyOnWriteArrayList<>();

  // capturing the system property and environment variables on startup of JVM
  public static volatile Properties systemProperties = getSystemProperties();
  public static volatile Map<String, String> environmentVariableMap = getEnvironmentVariableMap();

  public static final String FILE_PREFIX = "file|";
  public static final String NETWORK_PREFIX = "network|";

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
      rejectedPrefix = prefix != null
                       ? prefix.resolve("ide_config")
                       : prefix;
    } catch (Throwable t) {
      prefix = null;
      rejectedPrefix = null;
    }
    localJarPathPrefix = prefix;
    rejectedLocalJarPathPrefix = rejectedPrefix;
  }

  public static void setContextTag(String tag) {
    contextTag = String.format("[%s] ", tag); // extra space is on purpose
  }

  private void markTransformationUnsafe(String transformedClassName, String throwableText) {
    recordInternalEvent(String.format("%s error during transformation of class %s: %s",
                                      CMI_ERROR,
                                      transformedClassName,
                                      throwableText));
  }

  public static final TransformationStatistics statistics = new TransformationStatistics();

  // For testing only
  public static final AtomicBoolean rejectResourcesThatCannotBeFound = new AtomicBoolean(true);

  private static final List<CollectedDependencies> remoteDependenciesList = new CopyOnWriteArrayList<>();

  private static Path getCodetreeArtifactPrefix() throws MalformedURLException, URISyntaxException {
    URL resourceURL = ClassMonitorInjector.class.getClassLoader().getResource(constructDependencyName(
        ClassMonitorInjector.class.getName()));
    if (resourceURL != null) {
      Path thisJarPath = Paths.get(new URL(resourceURL.getPath().split("!")[0]).toURI());
      boolean isBuildDirJar = thisJarPath.getFileName().toString().contains("HASH");
      if (isBuildDirJar) {
        return thisJarPath.getParent().getParent().getParent().getParent();
      }
    }
    //null otherwise as we can do the check using manifest values
    return null;
  }

  public static Properties getSystemProperties() {
    Properties p = new Properties();
    p.putAll(System.getProperties());
    return p;
  }

  public static Map<String, String> getEnvironmentVariableMap() {
    return new HashMap<>(System.getenv());
  }

  // When a class is loaded, it means it was truly needed.
  // `cachedIsOptimusClass` will contain more entries than `allTransformedOptimusClassesAndTheirClassloader` as
  // we discover more static dependencies than are dynamically exercised. We care only about the actual dependency
  // with the least overhead possible: we do end up erring on the side of caution with more dependencies than truly are.
  private static boolean isLoadedOptimusClass(String className) {
    return allTransformedOptimusClassesAndTheirClassloader.containsKey(className);
  }

  public static void recordInternalEvent(String event) {
    internalEvents.add(contextTag + event);
  }

  private static String transformToClassResourceName(String className) {
    return className.replace("[]", "") + classExtension;
  }

  @Override
  public byte[] transform(ClassLoader loader,
                          String className,
                          Class<?> classBeingRedefined,
                          ProtectionDomain protectionDomain,
                          byte[] classfileBuffer) {

    // Deal with arrays of some type by removing the square brackets
    String classResourceName = transformToClassResourceName(className);
    // Lack of protection domain suggest jdk classes
    // Exclude jdk internals (hiding being official APIs) and a few frameworks
    if (protectionDomain == null ||
        className.startsWith("jdk/") ||
        className.startsWith("junit/") ||
        className.startsWith("org/junit/") ||
        className.startsWith("org/scalatest/") ||
        className.startsWith("scala/collection/") ||
        (isIJ && className.startsWith("com/intellij/"))) {
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

        // Instrumenting (i.e. adding instructions) is optional, but we still need to collect static dependencies!
        boolean exemptedPackage = instrumentationExemptedPackages.stream().anyMatch(iep -> iep.isExempted(loader,
                                                                                                          classResourceName));
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
      }
      if (visitors.size() > 0) {
        VisitContext context = new VisitContext(loader, className, classResourceName);
        ClassReader classReader = new ClassReader(classfileBuffer);
        ClassWriter classWriter = new ClassWriter(classReader, ClassWriter.COMPUTE_MAXS);
        ClassVisitor finalVisitor = classWriter;
        for (Class<? extends ClassVisitor> visitor : visitors) {
          finalVisitor = visitor.getConstructor(VisitContext.class, ClassVisitor.class).newInstance(context,
                                                                                                    finalVisitor);
        }
        statistics.visited.incrementAndGet();
        classReader.accept(finalVisitor, 0);
        context.wrapUp();
        return classWriter.toByteArray();
      } else {
        statistics.unchanged.incrementAndGet();
        return classfileBuffer;
      }
    } catch (Throwable t) {
      markTransformationUnsafe(className, t.toString());
      statistics.failures.incrementAndGet();
      return classfileBuffer;
    }
  }

  public synchronized static void clearLocalUsageData() {
    usedClasses.clear();
    internalEvents.clear();
  }

  public synchronized static void clearAllUsageData() {
    usedClasses.clear();
    internalEvents.clear();
    remoteDependenciesList.clear();
  }

  // Only to peek at internal structure
  public synchronized static Map<String, Set<String>> getClassDependencies() {
    return classDependencies.getSafeCopy();
  }

  // Only to peek at internal structure
  public synchronized static Map<String, Set<ResourceDependency>> getResourceDependencies() {
    return resourceDependencies.getSafeCopy();
  }

  // [GSF] Used by the OptimusCalculationServiceHandler on the grid engine to get the information for the client
  // [UI] Used by ClassMonitorServlet inside the UI backend
  public static CollectedDependencies collectLocallyUsedDependenciesAndReset() {
    return getUsedRawDependencies(true, true);
  }

  // [GSF] Used in GSFGridDistributionClient to track the remote execution dependencies
  public static void logRemoteClassAndResourceUsage(CollectedDependencies dependencies) {
    remoteDependenciesList.add(dependencies);
  }

  public static List<CollectedDependencies> getRemotelyUsedDependenciesAndReset() {
    List<CollectedDependencies> remoteDependenciesListCopy = List.copyOf(remoteDependenciesList);
    remoteDependenciesList.clear();
    return remoteDependenciesListCopy;
  }

  public static void logClassUsage(String className) {
    if (showTopN > NOT_COUNTING_HITS) {
      usedClasses.compute(className,
                          (key, v) -> v != null
                                      ? v + 1
                                      : 1);
    } else {
      usedClasses.putIfAbsent(className, NOT_COUNTING_HITS); // Not counting + optimization
    }
  }

  public static void logClassUsage(Class<?> clazz) {
    logClassUsage(constructDependencyName(clazz.getName()));
  }

  public static void logClassAndResourceUsage(Object dependency, boolean isClassDependency, String className) {
    logClassUsage(className);
    if (dependency != null) {
      if (isClassDependency) {
        Class<?> dependencyClass = (dependency instanceof Class)
                                   ? (Class) dependency
                                   : dependency.getClass();
        computeAndAddClassDependency(dependencyClass.getClassLoader(),
                                     className,
                                     constructDependencyName(dependencyClass.getName()));
      } else {
        if (dependency instanceof ResourceDependency) {
          computeAndAddResourceDependency(className, (ResourceDependency) dependency);
        } else if (!dependency.toString().endsWith(classExtension)) {
          computeAndAddResourceDependency(className,
                                          new ResourceDependency(dependency.toString(), ResourceAccessType.input));
        } else {
          recordInternalEvent(String.format("%s Unmanaged resource type %s from %s: add code to ignore or handle",
                                            CMI_WARN,
                                            dependency.toString(),
                                            className));
        }
      }
    }
  }

  public static void logFileResourceUsage(File resource, String accessMode, String className) {
    logFileResourceUsage(resource.getAbsolutePath(), accessMode, className);
  }

  public static void logFileResourceUsage(String resourcePath, String accessMode, String className) {
    // Ignore jars: they are covered by class path
    if (!resourcePath.endsWith(".jar")) {
      logClassAndResourceUsage(new ResourceDependency(resourcePath, ResourceAccessType.fromFileAccessMode(accessMode)),
                               false,
                               className);
    }
  }

  public static void logUriResourceUsage(URI resource, String className) {
    logNetworkResourceUsage(resource.toString(), className);
  }

  private static boolean isClassOrJarResource(URL resource) {
    return (resource.getProtocol().equals("jar") || resource.getProtocol().equals("file")) &&
           (resource.getPath().endsWith(classExtension) || resource.getPath().endsWith(".jar"));
  }

  public static void logUrlResourceUsage(URL resource, String className) {
    if (resource == null || isClassOrJarResource(resource)) {
      // Not interested in these cases
      return;
    }
    if (resource.getProtocol().equals("jar")) {
      logClassAndResourceUsage(new ResourceDependency(resource.toString(), ResourceAccessType.input), false, className);
    } else if (resource.getProtocol().equals("file")) {
      try {
        // This occurs because windows local absolute paths (e.g. D:\foo\bar) get encoded as file:////D:/foo/bar,
        // so the "path" component of the URL starts with ////D:/, not D:. We need to remove the ////
        Path path = resource.getPath().startsWith("////")
                    ? Paths.get(resource.getPath().substring(4))
                    : Paths.get(resource.toURI());
        logFileResourceUsage(path.toFile(), "r", className);
      } catch (URISyntaxException | FileSystemNotFoundException | UnsupportedOperationException | InvalidPathException e) {
        recordInternalEvent(String.format("%s Cannot parse URI %s from %s: ignored",
                                          CMI_WARN,
                                          resource.toString(),
                                          className));
      }
    } else {
      logNetworkResourceUsage(resource.toString(), className);
    }
  }

  public static void logClassAndStreamResourceByNameUsage(ClassLoader classLoader, String name, String className) {
    if (name.endsWith(classExtension)) {
      logClassUsage(name);
    } else {
      URL url = classLoader.getResource(name);
      if (url != null) { // If null, no consequence as not found in scope
        logUrlResourceUsage(url, className);
      }
    }
  }

  public static void logClassAndStreamResourceByNameUsage(Class<?> clazz, String name, String className) {
    URL url = clazz.getResource(name);
    if (url != null) { // If null, no consequence as not found in scope
      logUrlResourceUsage(url, className);
    }
  }

  public static void logClassAndResourcesByNameUsage(ClassLoader classLoader, String name, String className) {
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
    logClassAndResourceUsage(new ResourceDependency(networkAddress, ResourceAccessType.network), false, className);
  }

  // NEVER mix in remote dependencies: this is meant only to capture local ones.
  public synchronized static CollectedDependencies getUsedRawDependencies(boolean clearInternalState,
                                                                          boolean sanitize) {
    Set<String> usedClassesCopy = instrumentationExemptedClasses.stream()
                                                                .map(ClassMonitorInjector::constructDependencyName)
                                                                .collect(Collectors.toSet());

    classesFromExemptedPackages.forEach(usedClassesCopy::add);
    usedClasses.keySet().forEach(usedClassesCopy::add); // collection addAll has a bug, please to do not use
    if (showTopN > NOT_COUNTING_HITS) {
      List<Map.Entry<String, Integer>> sortedUsage = usedClasses.entrySet()
                                                                .stream()
                                                                .sorted((o1, o2) -> o2.getValue()
                                                                                      .compareTo(o1.getValue()))
                                                                .collect(Collectors.toList());
      String topList = sortedUsage.subList(0, Math.min(showTopN, sortedUsage.size()))
                                  .stream()
                                  .map((entry) -> String.format("%s (%d)", entry.getKey(), entry.getValue()))
                                  .collect(Collectors.joining("\n - "));
      recordInternalEvent(String.format("%s Internal CMI state: top %d used classes: \n - %s.",
                                        CMI_INFO,
                                        showTopN,
                                        topList));
    }
    if (clearInternalState) {
      usedClasses.clear();
    }

    Set<String> allClassDependencies = new HashSet<>();
    Set<ResourceDependency> allResourceDependencies = new HashSet<>();
    for (String className : usedClassesCopy) {
      collectUsedClassesAndResources(className, allClassDependencies, allResourceDependencies);
    }

    if (sanitize) {
      recordInternalEvent(String.format("%s Sanitizing %s classes and %s resources.",
                                        CMI_INFO,
                                        allClassDependencies.size(),
                                        allResourceDependencies.size()));
      // Remove any class that is not from Optimus
      int unrefinedClassDependencySize = allClassDependencies.size();
      allClassDependencies.removeIf(d -> !isLoadedOptimusClass(d));
      recordInternalEvent(String.format(
          "%s We removed %s classes to retain only those that were truly needed (i.e. class loaded).",
          CMI_INFO,
          (unrefinedClassDependencySize - allClassDependencies.size())));

      int unrefinedResourceDependencySize = allResourceDependencies.size();
      // Remove anything that is read and written to
      allResourceDependencies.removeIf(d -> allResourceDependencies.stream()
                                                                   .anyMatch(d2 -> d2.getResourceId()
                                                                                     .equals(d.getResourceId()) &&
                                                                                   !d2.getAccessType()
                                                                                      .equals(d.getAccessType()) &&
                                                                                   d2.getAccessType().created));
      // Remove any dependency that is also written to (without reading back)
      allResourceDependencies.removeIf(d -> d.getAccessType().created);
      // Remove read-only paths and missing files
      allResourceDependencies.removeIf(d -> {
        if (d.getAccessType() == ResourceAccessType.input) {
          try {
            if (d.getResourceId().startsWith("jar:file")) {
              InputStream input = null;
              try {
                input = new URL(d.getResourceId()).openStream();
              } finally {
                if (input != null) { input.close(); }
              }
            } else {
              Path path = Paths.get(d.getResourceId());
              return (!path.toFile().exists() && rejectResourcesThatCannotBeFound.get()) || path.toFile().isDirectory();
            }
          } catch (InvalidPathException e) {
            return false; // Let it be visible
          } catch (IOException e) {
            return rejectResourcesThatCannotBeFound.get(); // Can't be accessed
          } catch (IllegalArgumentException e) {
            recordInternalEvent(String.format("%s We could not open %s (deemed inaccessible): %s", CMI_WARN, d.getResourceId(), e.getMessage()));
            return rejectResourcesThatCannotBeFound.get(); // Can't be accessed
          } catch (Throwable t) {
            // Do not rethrow, but let's poison the dependencies to prevent caching
            recordInternalEvent(String.format("%s We could not open %s: %s", CMI_ERROR, d.getResourceId(), t.getMessage()));
            t.printStackTrace(); // Best observability; notice that we don't have any logger in CMI since we are in a JVM agent
            return false; // Let it be visible
          }
        }
        return false; // Retain
      });
      // Remove loop back addresses
      allResourceDependencies.removeIf(d -> d.getAccessType().equals(ResourceAccessType.network) &&
                                            (d.getResourceId().contains("localhost") || d.getResourceId().contains(
                                                "127.0.0.1")));
      recordInternalEvent(String.format(
          "%s We removed %s resources to exclude those generated by the test, using loopback interface or that are not found",
          CMI_INFO,
          (unrefinedResourceDependencySize - allResourceDependencies.size())));
      // Remove DTC internal files passed between processes, determined by system properties.
      int resourceCountBeforeTrim = allResourceDependencies.size();
      String resultFileProp = systemProperties.getProperty(InterProcessFile.lookupResult.sysProp, "None");
      String runContextProp = systemProperties.getProperty(InterProcessFile.runContext.sysProp, "None");
      allResourceDependencies.removeIf(d -> d.getResourceId().equals(resultFileProp) ||
                                            d.getResourceId().equals(runContextProp));
      recordInternalEvent(String.format(
          "%s We removed %s resource(s) to exclude those generated by the DTC infra for inter-process communication.",
          CMI_INFO,
          (resourceCountBeforeTrim - allResourceDependencies.size())));
      recordInternalEvent(String.format("%s Sanitizing complete: left with %s class dependencies and %s resources.",
                                        CMI_INFO,
                                        allClassDependencies.size(),
                                        allResourceDependencies.size()));
    }

    recordInternalEvent(String.format("%s Internal CMI state: collected thus far %s class dependencies for %s classes.",
                                      CMI_INFO,
                                      classDependencies.getDependenciesCount(),
                                      classDependencies.size()));
    recordInternalEvent(String.format(
        "%s Internal CMI state: collected thus far %s resources dependencies for %s classes.",
        CMI_INFO,
        resourceDependencies.getDependenciesCount(),
        resourceDependencies.size()));
    recordInternalEvent(String.format(
        "%s Internal CMI state: transformation internals [transformed classes = %s, optimus jars = %s out of %s, optimus classes = %s out of %s].",
        CMI_INFO,
        allTransformedOptimusClassesAndTheirClassloader.size(),
        cachedIsOptimusJar.values()
                          .stream()
                          .mapToInt(b -> b
                                         ? 1
                                         : 0)
                          .sum(),
        cachedIsOptimusJar.size(),
        cachedIsOptimusClass.values()
                            .stream()
                            .mapToInt(b -> b
                                           ? 1
                                           : 0)
                            .sum(),
        cachedIsOptimusClass.size()));

    Set<String> usedResourcesCopy = allResourceDependencies.stream()
                                                           .map(ResourceDependency::getQualifiedResourceId)
                                                           .collect(Collectors.toSet());

    recordInternalEvent(String.format("%s Internal CMI state: %d class dependencies were collected",
                                      CMI_INFO,
                                      allClassDependencies.size()));

    recordInternalEvent(String.format("%s Internal CMI state: %d used classes were collected",
                                      CMI_INFO,
                                      usedClassesCopy.size()));

    recordInternalEvent(String.format("%s Internal CMI state: %d used resources were collected",
                                      CMI_INFO,
                                      usedResourcesCopy.size()));

    // Delayed capture and reset -- MUST BE LAST THING TO DO BEFORE RETURNING!!!
    List<String> internalEventsCopy = new ArrayList<>();
    internalEvents.forEach(internalEventsCopy::add); // collection addAll has a bug, please to do not use
    if (clearInternalState) { internalEvents.clear(); }

    return new CollectedDependencies(usedClassesCopy,
                                     allClassDependencies,
                                     usedResourcesCopy,
                                     internalEventsCopy,
                                     systemProperties,
                                     environmentVariableMap,
                                     new HashSet<>(),
                                     new ArrayList<>());
  }

  // If not found, the result set is empty
  public static Set<String> peekAtClassStaticDependencies(String className) {
    Set<String> allClassDependencies = new HashSet<>();
    Set<ResourceDependency> allResourceDependencies = new HashSet<>();
    collectUsedClassesAndResources(constructDependencyName(className), allClassDependencies, allResourceDependencies);
    allClassDependencies.removeIf(d -> !isLoadedOptimusClass(d)); // Remove stuff that are not class loaded yet
    return allClassDependencies;
  }

  // Strictly static dependencies as we discovered types that are referred to
  public static void computeAndAddClassDependency(ClassLoader loader, String className, String classDependency) {
    if (isOptimusClass(loader, classDependency)) {
      classDependencies.add(className, classDependency);
    }
  }

  // Runtime dependencies as we discover through API access
  private static void computeAndAddResourceDependency(String className, ResourceDependency resource) {
    resourceDependencies.add(className, resource);
  }

  private synchronized static void collectUsedClassesAndResources(String className,
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
    return resourcePath.contains("$MockitoMock$") ||
           resourcePath.contains("$$FastClassByGuice$$") ||
           resourcePath.contains("$$EnhancerBySpringCGLIB$$");
  }

  private static boolean rememberAsOptimusClass(String resourcePath, boolean isOptimusClass) {
    // Memory footprint reduction
    if (!isSyntheticDynamicClass(resourcePath)) {
      cachedIsOptimusClass.put(resourcePath, isOptimusClass);
    }
    return isOptimusClass;
  }

  private static final Set<String> nativeTypes = new HashSet<>(Arrays.asList("int.class",
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

    if (cachedIsOptimusClass.containsKey(resourcePath)) { return cachedIsOptimusClass.get(resourcePath); }

    // Memory footprint reduction
    if (isSyntheticDynamicClass(resourcePath)) { return false; }

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
      // However, the OBT test runner does unpack some jars given some suites expect to find files from filesystem
      // (i.e. do not use Class.getResource...), so they happen to be effectively from Optimus
      recordInternalEvent(String.format("%s Resource %s in %s is assumed from Optimus",
                                        CMI_INFO,
                                        resourcePath,
                                        resourceUrl));
      return rememberAsOptimusClass(resourcePath, true);
    }
  }

  public static boolean isOptimusJar(URL jarfileURL) {

    if (localJarPathPrefix == null) { //not local obt, we can rely on presence of manifest
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
        return jarPath.startsWith(localJarPathPrefix) && !jarPath.startsWith(rejectedLocalJarPathPrefix);
      } catch (URISyntaxException e) {
        return false;
      }
    }
  }

  public static ClassFileTransformer instance(ClassLoader host) {
    try {
      Class<?> cmiLoadedByAppClassloader = Class.forName(ClassMonitorInjector.class.getName(), true, host);
      return (ClassFileTransformer) cmiLoadedByAppClassloader.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException ex) {
      // boot classloader, or another classloader not owned by us; skip
      return null;
    }
  }
}

enum ResourceAccessType {
  input(false), output(true), both(true), network(false);

  public final boolean created;

  ResourceAccessType(boolean created) {
    this.created = created;
  }

  public static ResourceAccessType fromFileAccessMode(String accessMode) {
    if (accessMode.contains("rw") || accessMode.contains("wr")) {
      return both;
    } else if (accessMode.contains("w")) {
      return output;
    }
    return input;
  }
}





