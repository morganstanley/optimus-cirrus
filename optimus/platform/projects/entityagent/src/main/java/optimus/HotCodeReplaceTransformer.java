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

import static optimus.debug.CommonAdapter.asJavaName;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.Instrumentation;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;

import optimus.graph.DiagnosticSettings;

public class HotCodeReplaceTransformer implements ClassFileTransformer {

  static void log(String message) {
    log(message, false);
  }

  private static void log(String message, boolean always) {
    if (always || DiagnosticSettings.enableHotCodeReplaceLogging) {
      System.out.println("HCR>> " + message);
    }
  }

  private final Instrumentation instrumentation;

  // cache conversions from String to Path, since these can be comparitively slow
  private static final ConcurrentHashMap<String, Path> paths = new ConcurrentHashMap<>();

  // original jar is the one (indirectly) on the real java.class.path. All of the classes coming to
  // transform will have
  // been loaded from these jars
  private final ConcurrentHashMap<String, ObtJarMetadata> originalClassJarToMetadata =
      new ConcurrentHashMap<>();

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  HotCodeReplaceTransformer(Instrumentation instrumentation) {
    this.instrumentation = instrumentation;
  }

  /**
   * Intercepts loading from build_obt jars to a) record the classloading and b) replace bytes with
   * those loaded from the most version of the jar (if the current jar is not the original jar)
   */
  @Override
  public byte[] transform(
      ClassLoader loader,
      String className,
      Class<?> classBeingRedefined,
      ProtectionDomain protectionDomain,
      byte[] classfileBuffer) {
    if (protectionDomain == null || protectionDomain.getCodeSource().getLocation() == null) {
      return classfileBuffer;
    }
    String jarPath = protectionDomain.getCodeSource().getLocation().getPath();

    if (isObtClassJar(jarPath) || isInstalledClassJar(jarPath)) {
      Lock readLock = lock.readLock();
      readLock.lock();
      try {
        ObtJarMetadata metadata =
            originalClassJarToMetadata.computeIfAbsent(
                jarPath,
                p -> new ObtJarMetadata(p, DiagnosticSettings.enableHotCodeReplaceAutoClose));
        metadata.recordClassLoad(className, loader);

        log("Loading requested for " + className + " from OBT jar " + jarPath);
        return metadata.loadReplacementClassBytes(className, classfileBuffer);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        readLock.unlock();
      }
    } else {
      return classfileBuffer;
    }
  }

  private static final String HASHED_JAR = "([^\\\\/]+?)(?:\\.INCR)?\\.HASH[0-9a-f]+\\.jar";
  // Group 1 is scope
  private static final Pattern OBT_HASHED_PATHING_JAR =
      Pattern.compile(".*[\\\\/]pathing[\\\\/]" + HASHED_JAR);
  // Group 1 is jar directory path, group 2 is scope
  static final Pattern OBT_HASHED_CLASS_JAR = Pattern.compile("(.*)[\\\\/]" + HASHED_JAR);

  private static final Pattern OBT_INSTALLED_PATHING_JAR =
      Pattern.compile(
          "[\\\\/](\\w+)[\\\\/](\\w+)[\\\\/]\\w+[\\\\/]install[\\\\/]common[\\\\/](?:bin[\\\\/]..[\\\\/])?lib/(\\w+)(?:\\.(\\w+))?-runtimeAppPathing\\.jar");

  // Example:
  // /my/network/path/install/optimus/platform/local/install/common/lib/entityagent.jar
  static final Pattern OBT_INSTALLED_CLASS_JAR =
      Pattern.compile(
          ".*[\\\\/](\\w+)[\\\\/](\\w+)[\\\\/]\\w+[\\\\/]install[\\\\/]common[\\\\/]lib[\\\\/](\\w+)(?:\\.(\\w+))?\\.jar");

  private static String pathingScopeForHashedJar(String jarPath) {
    Matcher matcher = OBT_HASHED_PATHING_JAR.matcher(jarPath);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      return null;
    }
  }

  private static ScopeId pathingScopeForInstalledJar(String jarPath) {
    Matcher matcher = OBT_INSTALLED_PATHING_JAR.matcher(jarPath);
    if (matcher.find()) {
      String scopeType = matcher.group(4) != null ? matcher.group(4) : "main";
      return new ScopeId(matcher.group(1), matcher.group(2), matcher.group(3), scopeType);
    } else {
      return null;
    }
  }

  static Path getPath(String jarPath) {
    return paths.computeIfAbsent(jarPath, Paths::get);
  }

  static boolean isObtClassJar(String jarPath) {
    return OBT_HASHED_CLASS_JAR.matcher(jarPath).matches();
  }

  private static boolean isInstalledClassJar(String jarPath) {
    return OBT_INSTALLED_CLASS_JAR.matcher(jarPath).matches();
  }

  static FileTime lastModified(Path path) {
    try {
      if (Files.exists(path)) {
        return Files.getLastModifiedTime(path);
      } else {
        return FileTime.fromMillis(0);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // Convert leading "/", "///" etc. to "//"
  static String standardize(String path) {
    int length = path.length();
    if ((length == 1 && path.charAt(0) == '/')
        || (length > 1 && path.charAt(0) == '/' && path.charAt(1) != '/')) {
      path = '/' + path;
    }

    int count = 2;
    while (path.length() > count && path.charAt(count) == '/') {
      count++;
    }
    if (count > 2) {
      path = path.substring(count - 2);
    }

    return path;
  }

  void startPolling() {
    /*
     * Watches for changes to mapping to current class jars (for the class jars we actually loaded stuff from) and
     * kicks off retransformation on any loaded classes which changed hash.
     */
    final Thread t =
        new Thread("ObtHotCodeReplaceWatcher") {
          @Override
          public void run() {
            try {
              final String classpath = System.getProperty("java.class.path");
              ClasspathState classpathState = null;
              for (String jar : classpath.split(File.pathSeparator)) {
                final String hashedPathingScope = pathingScopeForHashedJar(jar);
                if (hashedPathingScope != null) {
                  final Path hashedPathingJar = getPath(jar);
                  log("Scope: " + hashedPathingScope + " (jar: " + hashedPathingJar + ")");

                  // eg. C:/path/to/workspace/build_obt/1.23/pathing/a.b.c.d.HASH123.jar =>
                  // C:/path/to/workspace/build_obt/classpath-mapping.txt
                  Path classPathMappingFile =
                      hashedPathingJar
                          .getParent()
                          .getParent()
                          .getParent()
                          .resolve("classpath-mapping.txt");

                  classpathState = new ObtCurrentMapping(hashedPathingScope, classPathMappingFile);
                  break;
                }
                ScopeId installedPathingScope = pathingScopeForInstalledJar(jar);
                if (installedPathingScope != null) {
                  final Path installedPathingJar = getPath(jar);
                  log("Scope: " + installedPathingScope + " (jar: " + installedPathingJar + ")");
                  classpathState =
                      new InstalledPathingJar(installedPathingScope, installedPathingJar);
                  break;
                }
              }

              if (classpathState != null) {
                scanForClassJarUpdates(classpathState);
              } else {

                log("No pathing jar found on classpath: " + classpath);
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        };
    t.setName("HotCodeReplace");
    t.setDaemon(true);
    t.start();
  }

  private void scanForClassJarUpdates(ClasspathState classpathState) throws Exception {
    //noinspection InfiniteLoopStatement
    while (true) {
      long startTime = System.currentTimeMillis();
      Set<String> changedJars = classpathState.pollForChangedJars();
      if (!changedJars.isEmpty()) {
        Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
          // build map from stable part of name to full name (for faster lookup below)
          Map<String, String> newJarsByStableName = new HashMap<>();
          for (String jar : changedJars) {
            final String stableName = classpathState.stableName(jar);
            if (stableName != null) {
              newJarsByStableName.put(stableName, jar);
            }
          }

          int nUpdatedJars = 0;
          int nUpdatedClasses = 0;

          // for each loaded jar that has changed, find changed loaded classes and reload them
          for (Map.Entry<String, ObtJarMetadata> nameToMetadata :
              originalClassJarToMetadata.entrySet()) {
            String originalJar = nameToMetadata.getKey();
            String stableName = classpathState.stableName(originalJar);
            if (stableName != null) {
              String newJar = newJarsByStableName.get(stableName);
              if (newJar != null) {
                String oldJar = nameToMetadata.getValue().getCurrentJarPath();
                String updatedJar = classpathState.classpathIsMutable() ? newJar : oldJar;
                log("Updating " + oldJar + " to " + updatedJar);
                Collection<Class<?>> updatedClasses =
                    nameToMetadata.getValue().updateJarPath(updatedJar);
                if (!updatedClasses.isEmpty()) {
                  instrumentation.retransformClasses(updatedClasses.toArray(new Class[0]));
                }
                String oldJarName = jarName(oldJar);
                String newJarName = jarName(updatedJar);
                log(
                    "Updated "
                        + updatedClasses.size()
                        + " classes ("
                        + oldJarName
                        + " -> "
                        + newJarName
                        + ")");
                nUpdatedJars++;
                nUpdatedClasses += updatedClasses.size();
              }
            }
          }

          log("Updated " + nUpdatedClasses + " classes in " + nUpdatedJars + " jars", true);
        } finally {
          writeLock.unlock();
        }
      }
      long endTime = System.currentTimeMillis();
      log(
          "Detected "
              + changedJars.size()
              + " class jar updates in "
              + (endTime - startTime)
              + "ms");

      Thread.sleep(1000);
    }
  }

  private String jarName(String jarPath) {
    return jarPath.replaceFirst(".*/", "");
  }
}

interface ClasspathState {
  String stableName(String jarPath);

  // Do the names of the jars change when rebuilding?
  boolean classpathIsMutable();

  Set<String> pollForChangedJars() throws IOException;
}

/**
 * Keeps track of the current OBT class jars (expanded from the pathing jar manifest) for a
 * particular scope of interest
 */
class ObtCurrentMapping implements ClasspathState {
  private final Path classPathMappingFile;
  private FileTime classPathMappingLastModified;

  private final String scope;
  private Path currentPathingJar;
  private Collection<String> currentJars;

  ObtCurrentMapping(String scope, Path classPathMappingFile) throws IOException {
    this.scope = scope;
    this.classPathMappingFile = classPathMappingFile;
    this.classPathMappingLastModified =
        HotCodeReplaceTransformer.lastModified(classPathMappingFile);
    this.currentPathingJar = resolvePathingJar();
    this.currentJars = loadObtJarsFromCurrentPathingJar();
  }

  // eg. C:/path/to/workspace/build_obt/1.23/java/a.b.c.d.HASH123.jar
  // returns C:/path/to/workspace/build_obt/1.23/java/a.b.c.d
  public String stableName(String jarPath) {
    Matcher m = HotCodeReplaceTransformer.OBT_HASHED_CLASS_JAR.matcher(jarPath);
    if (m.matches()) {
      return HotCodeReplaceTransformer.standardize(m.group(1) + "/" + m.group(2));
    } else {
      return null;
    }
  }

  @Override
  public boolean classpathIsMutable() {
    return true;
  }

  /**
   * returns new names for jars from the manifest of the scope of interest which have changed since
   * the last call to this method
   */
  public Set<String> pollForChangedJars() throws IOException {
    // lots of short circuits for efficiency reasons
    FileTime newLastModified = HotCodeReplaceTransformer.lastModified(classPathMappingFile);
    if (!newLastModified.equals(classPathMappingLastModified)) {
      classPathMappingLastModified = newLastModified;
      Path newPathingJar = resolvePathingJar();
      if (newPathingJar != null && !newPathingJar.equals(currentPathingJar)) {
        currentPathingJar = newPathingJar;
        Collection<String> newJars = loadObtJarsFromCurrentPathingJar();
        if (!newJars.equals(currentJars)) {
          Set<String> difference = new HashSet<>(newJars);
          difference.removeAll(currentJars);
          currentJars = newJars;
          return difference;
        }
      }
    }

    return Collections.emptySet();
  }

  /** Loads the current pathing jar for specific scope from the classpath-mapping.txt file */
  private Path resolvePathingJar() throws IOException {
    if (Files.exists(classPathMappingFile)) {
      for (String line : Files.readAllLines(classPathMappingFile)) {
        if (line.startsWith(scope + '\t')) {
          return HotCodeReplaceTransformer.getPath(line.substring(line.indexOf('\t') + 1));
        }
      }
      throw new IllegalArgumentException(
          "Couldn't find scope " + scope + " in " + classPathMappingFile);
    } else {
      HotCodeReplaceTransformer.log("No mapping file found at " + classPathMappingFile);
      return null;
    }
  }

  /** Returns all build_obt jars from the classpath manifest of the specified pathing jar */
  private Collection<String> loadObtJarsFromCurrentPathingJar() throws IOException {
    if (currentPathingJar != null) {
      try (JarFile jar = new JarFile(currentPathingJar.toFile())) {
        String[] classpath =
            jar.getManifest().getMainAttributes().getValue("Class-Path").split(" ");
        Stream<String> obtJars =
            Arrays.stream(classpath).filter(HotCodeReplaceTransformer::isObtClassJar);
        return obtJars.map(f -> f.replace("file://", "")).collect(Collectors.toList());
      }
    } else {
      return Collections.emptyList();
    }
  }
}

/** Keeps track of jars within an installed pathing jar */
class InstalledPathingJar implements ClasspathState {
  private final ScopeId scopeId;
  private Map<Path, FileTime> currentFingerprints;
  private Map<Path, FileTime> currentClassJars;

  InstalledPathingJar(ScopeId scopeId, Path pathingJar) throws IOException {
    this.scopeId = scopeId;
    try (JarFile jar = new JarFile(pathingJar.toFile())) {
      String[] cp = jar.getManifest().getMainAttributes().getValue("Class-Path").split(" ");
      // Assume all relative paths are potentially-updatable jars
      Stream<String> obtJars = Arrays.stream(cp).filter(f -> !f.startsWith("file://"));
      Set<Path> classJars =
          obtJars
              .map(
                  p ->
                      pathingJar
                          .getParent()
                          .resolve(HotCodeReplaceTransformer.getPath(p))
                          .normalize())
              .collect(Collectors.toSet());
      this.currentClassJars = loadFileTimes(classJars);
    }
    Path installRoot =
        pathingJar
            .normalize()
            .getParent()
            .getParent()
            .getParent()
            .getParent()
            .getParent()
            .getParent()
            .getParent();
    Set<Path> fingerprints =
        Files.walk(installRoot)
            .filter(p -> p.getFileName().toString().equals("fingerprints.txt"))
            .collect(Collectors.toSet());
    this.currentFingerprints = loadFileTimes(fingerprints);
  }

  @Override
  public String stableName(String jarPath) {
    // for installed jars, differences in path resolution may mean the paths don't match between
    // pathing-jar-based paths and classloader paths (even when both are absolute)
    Matcher m = HotCodeReplaceTransformer.OBT_INSTALLED_CLASS_JAR.matcher(jarPath);
    if (m.matches()) {
      String meta = m.group(1) != null ? m.group(1) : scopeId.getMeta();
      String bundle = m.group(2) != null ? m.group(2) : scopeId.getBundle();
      String module = m.group(3);
      String tpe = m.group(4) != null ? m.group(4) : "main";
      return meta + "." + bundle + "." + module + "." + tpe;
    } else {
      return null;
    }
  }

  @Override
  public boolean classpathIsMutable() {
    return false;
  }

  private Map<Path, FileTime> loadFileTimes(Set<Path> paths) {
    Stream<Path> stream = paths.stream();
    return stream.collect(
        Collectors.toMap(Function.identity(), HotCodeReplaceTransformer::lastModified));
  }

  public Set<String> pollForChangedJars() {
    Map<Path, FileTime> newFingerprints = loadFileTimes(currentFingerprints.keySet());
    // short-circuit - if the fingerprints are unchanged then the class jars must be too
    if (currentFingerprints.isEmpty() || !newFingerprints.equals(currentFingerprints)) {
      currentFingerprints = newFingerprints;
      Map<Path, FileTime> newClassJars = loadFileTimes(currentClassJars.keySet());
      if (!newClassJars.equals(currentClassJars)) {
        Map<Path, FileTime> difference = new HashMap<>(newClassJars);
        difference.entrySet().removeAll(currentClassJars.entrySet());
        currentClassJars = newClassJars;
        return difference.keySet().stream().map(Path::toString).collect(Collectors.toSet());
      } else {
        return Collections.emptySet();
      }
    } else {
      return Collections.emptySet();
    }
  }
}

final class Pair<T, U> {
  private final T first;
  private final U second;

  Pair(T first, U second) {
    this.first = first;
    this.second = second;
  }

  public T getFirst() {
    return first;
  }

  public U getSecond() {
    return second;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Pair<?, ?> pair = (Pair<?, ?>) o;
    return first.equals(pair.first) && second.equals(pair.second);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  @Override
  public String toString() {
    return "Pair(" + first + ", " + second + ')';
  }
}

final class ScopeId {
  private final String meta;
  private final String bundle;
  private final String module;
  private final String tpe;

  ScopeId(String meta, String bundle, String module, String tpe) {
    this.meta = meta;
    this.bundle = bundle;
    this.module = module;
    this.tpe = tpe;
  }

  public String getMeta() {
    return meta;
  }

  public String getBundle() {
    return bundle;
  }

  public String getModule() {
    return module;
  }

  public String getTpe() {
    return tpe;
  }

  @Override
  public String toString() {
    return meta + "." + bundle + "." + module + "." + tpe;
  }
}

class ObtJarMetadata {
  private String currentJarPath;
  private boolean autoCloseJar;
  private JarFile currentJar;
  private final Map<String, String> originalClassesToHashes;
  private Map<String, String> currentClassesToHashes;
  private final Set<Pair<String, ClassLoader>> loadedClasses = ConcurrentHashMap.newKeySet();

  class JarSession implements Closeable {

    private JarFile jar;

    JarSession() throws IOException {
      if (autoCloseJar) {
        jar = new JarFile(currentJarPath);
      } else {
        jar = currentJar;
      }
    }

    public JarFile getJar() {
      return jar;
    }

    @Override
    public void close() throws IOException {
      if (autoCloseJar) {
        jar.close();
      }
    }
  }

  /** Initializes current and original class -> hash mappings */
  ObtJarMetadata(String originalJarPath, boolean autoCloseJar) {
    this.currentJarPath = originalJarPath;
    this.autoCloseJar = autoCloseJar;
    try {
      if (!autoCloseJar) {
        this.currentJar = new JarFile(currentJarPath);
      }
      try (JarSession session = new JarSession()) {
        this.originalClassesToHashes = loadClassHashes(session.getJar());
        this.currentClassesToHashes = originalClassesToHashes;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  String getCurrentJarPath() {
    return currentJarPath;
  }

  /**
   * if the current jar jas a different hash for className, returns the bytes from that class in
   * that jar, else returns originalClassBytes
   */
  byte[] loadReplacementClassBytes(String className, byte[] originalClassBytes) throws IOException {
    // optimization - don't reload class if it's the same as the original
    String originalHash = originalClassesToHashes.get(className);
    if (currentClassesToHashes == originalClassesToHashes
        || (originalHash != null && originalHash.equals(currentClassesToHashes.get(className)))) {
      HotCodeReplaceTransformer.log("Skipping reload for " + className + " since it's unchanged");
      return originalClassBytes;
    } else {
      try (JarSession session = new JarSession()) {
        JarFile jar = session.getJar();
        ZipEntry entry = jar.getEntry(className + ".class");
        try (InputStream inStream = jar.getInputStream(entry)) {
          byte[] bytes = new byte[(int) entry.getSize()];
          int pos = 0;
          while (pos < bytes.length) {
            pos += inStream.read(bytes, pos, bytes.length - pos);
          }

          HotCodeReplaceTransformer.log(
              "Reloaded " + className + " from " + currentJarPath + " since it's changed");
          return bytes;
        }
      }
    }
  }

  /**
   * Loads class hashes from newJarPath, updates internal state, and returns any loaded classes for
   * which the hash has changed.
   */
  Collection<Class<?>> updateJarPath(String newJarPath) {
    ArrayList<Class<?>> changedClasses = new ArrayList<>();
    JarFile newJar;
    try {
      newJar = new JarFile(newJarPath);

      Map<String, String> newHashes = loadClassHashes(newJar);
      for (Pair<String, ClassLoader> loadedClassAndLoader : loadedClasses) {
        String loadedClassName = loadedClassAndLoader.getFirst();
        String newHash = newHashes.get(loadedClassName);
        if (newHash != null && !newHash.equals(currentClassesToHashes.get(loadedClassName))) {
          var secondClassLoader = loadedClassAndLoader.getSecond();
          Class<?> loadedClass = secondClassLoader.loadClass(asJavaName(loadedClassName));
          changedClasses.add(loadedClass);
        } else if (newHash == null) {
          HotCodeReplaceTransformer.log(
              "No new hash found for " + loadedClassName + "; not reloading");
        }
      }

      if (autoCloseJar) {
        newJar.close();
      } else {
        currentJar.close();
        currentJar = newJar;
      }
      currentJarPath = newJarPath;
      currentClassesToHashes = newHashes;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    HotCodeReplaceTransformer.log(
        changedClasses.size() + " loaded class(es) are changed in " + newJarPath);
    return changedClasses;
  }

  private static Map<String, String> loadClassHashes(JarFile jar) throws IOException {
    HashMap<String, String> classToHash = new HashMap<>();
    ZipEntry hashFile = jar.getEntry("META-INF/optimus/entryHashes.sha256");
    if (hashFile != null) {
      try (BufferedReader entryHashes =
          new BufferedReader(new InputStreamReader(jar.getInputStream(hashFile)))) {

        String line;
        while ((line = entryHashes.readLine()) != null) {
          int tabIdx = line.indexOf("\t");
          if (tabIdx > 0) {
            String className =
                line.substring(0, tabIdx - 6)
                    .replace(
                        '\\',
                        '/'); // strip off .class; also obt has a bug on Windows where \\ is used
            // instead of / in the
            // entryHashes file
            String hash =
                line.substring(tabIdx + 5); // skip the constant "HASH" part to save a few bytes
            classToHash.put(className, hash);
          }
        }
      }

      HotCodeReplaceTransformer.log(
          "Loaded " + classToHash.size() + " class hashes from " + jar.getName());
    } else {
      HotCodeReplaceTransformer.log("No class hashes found in " + jar.getName());
    }

    return classToHash;
  }

  void recordClassLoad(String className, ClassLoader loader) {
    loadedClasses.add(new Pair<>(className, loader));
  }
}
