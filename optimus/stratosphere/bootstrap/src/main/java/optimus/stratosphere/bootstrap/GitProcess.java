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
package optimus.stratosphere.bootstrap;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.typesafe.config.Config;

/**
 * This class was carefully crafted with a lot of trial and error - be careful when modifying it.
 * Few things to keep in mind:
 *
 * <ul>
 *   <li>ProcessBuilder do not pass PATH to child processes unless you run parent process in the
 *       shell (<code>cmd /c</code> or <code>bash -c</code>). It was working in previous JVM
 *       versions (1.8 and before), but it was undocumented behaviour.
 *   <li>Depending on the environment, PATH might have different text case (e.g. Path on Windows vs
 *       PATH on Linux). Do not make assumptions about it, if possible check existing env to verify
 *       how it looks.
 *   <li>On Windows not all git commands ran through full exe path works properly. For example
 *       <code>path_to_the_git\git.exe fetch</code> will fail due to missing git-upload-pack.exe.
 *       That is because Windows version of the git relies heavily on spawning various
 *       sub-processes. Due to that all git commands on Windows need to be ran through <code>cmd /c
 *       </code> with git on the Path.
 *   <li>When running git on Windows using <code>cmd /c</code> commands which contains <code>^
 *       </code> needs to be escaped using <code>^^</code>.
 *   <li>On Linux running git through <code>bash -c</code> is much more troublesome due to
 *       complicated escaping. That is because on Windows all <code>cmd /c git</code> args can be
 *       passed as subsequent ProcessBuilder parameters, while on Linux they need to be enclosed in
 *       a properly escaped string.
 *   <li>Since this class uses workspace Config, be aware when using it for setting-up test
 *       workspaces. If workspace is re-used between tests, like in the case of integration tests,
 *       previous test may intentionally break the config. It need to be cleaned before first git
 *       command is issued in the next test or test setup.
 * </ul>
 */
public class GitProcess {

  private Supplier<Config> config;

  public GitProcess(Config config) {
    this.config = () -> config;
  }

  public List<String> gitCmd(String... args) {
    List<String> exec = config.get().getStringList("git.exec");
    List<String> command = new ArrayList<>(exec);
    if (OsSpecific.isWindows) {
      command.addAll(
          Arrays.stream(args)
              .map(this::escapeWindows)
              .filter(Predicate.not(String::isEmpty))
              .collect(Collectors.toList()));
    } else {
      command.addAll(Arrays.asList(args));
    }
    return command;
  }

  public ProcessBuilder createProcessBuilder(
      Path srcDir, Map<String, String> additionalEnv, String... args) {
    List<String> cmd = gitCmd(args);
    ProcessBuilder pb = new ProcessBuilder(cmd);
    if (srcDir != null) {
      pb.directory(srcDir.toFile());
    }
    Map<String, String> environment = pb.environment();
    addGitToPath(environment);
    environment.putAll(additionalEnv);
    pb.redirectErrorStream(true);
    return pb;
  }

  public static boolean isSparseReady(Config conf) {
    if (OsSpecific.isWindows) {
      return isUsingGitFromTools(conf);
    } else {
      // sparse on linux works fine out of the box
      return true;
    }
  }

  public static boolean isUsingGitFromTools(Config conf) {
    String path = "tools.git.isDefault";
    return OsSpecific.isWindows && conf.hasPath(path) && conf.getBoolean(path);
  }

  public Path getGitExecPath() {
    List<String> execCmd = config.get().getStringList("git.exec");
    Path execName = Paths.get(execCmd.get(execCmd.size() - 1)).getFileName();
    return Paths.get(getGitPath()).resolve(execName);
  }

  public Map.Entry<String, String> addGitToPath(Map<String, String> env) {
    String mainKey = OsSpecific.isLinux ? "PATH" : "Path";
    String secondaryKey = OsSpecific.isLinux ? "Path" : "PATH";
    String mainPath = env.getOrDefault(mainKey, "");
    String secondaryPath = env.getOrDefault(secondaryKey, "");

    env.remove(mainKey);
    env.remove(secondaryKey);

    String gitPath = getGitPath();
    String strippedPath = (mainPath + File.pathSeparator + secondaryPath).replace(gitPath, "");
    String newPath = gitPath + File.pathSeparator + strippedPath;
    env.put(mainKey, newPath);

    return Map.entry(mainKey, newPath);
  }

  public String getGitPath() {
    return isUsingGitFromTools(config.get()) ? toolsGitPath() : config.get().getString("git.path");
  }

  private String toolsGitPath() {
    final String pathSuffix = config.get().getString("artifactory-tools.available.git.path");
    return Paths.get(config.get().getString("stratosphereHome"))
        .resolve(".stratosphere")
        .resolve("tools")
        .resolve(pathSuffix)
        .toString();
  }

  private String escapeWindows(String str) {
    return str.replaceAll("\\^", "\\^\\^");
  }
}
