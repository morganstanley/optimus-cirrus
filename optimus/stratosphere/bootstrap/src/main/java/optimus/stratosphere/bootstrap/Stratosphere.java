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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import com.typesafe.config.Config;
import optimus.stratosphere.bootstrap.config.StratosphereConfig;
import optimus.stratosphere.bootstrap.config.migration.MigrationManager;

public class Stratosphere {

  public static int SUCCESS = 0;
  public static int FAILURE = 1;

  private static int runInfra(Path infraPath, String[] args, Config config) {
    Path javaInstall = Paths.get(config.getString("internal.java.install"));
    Path javaPath =
        OsSpecific.isWindows
            ? javaInstall.resolve("bin").resolve("java.exe")
            : javaInstall.resolve("bin").resolve("java");

    String extraOpts = System.getenv("STRATO_EXTRA_JVM_OPTS");

    List<String> baseCommand =
        Arrays.asList(
            javaPath.toString(),
            extraOpts,
            // On Java 17 we get exceptions if we don't export the required packages (previously it
            // was just a warning)
            "--add-exports=java.security.jgss/sun.security.jgss.spi=ALL-UNNAMED",
            "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
            "--add-exports=java.security.jgss/sun.security.krb5.internal=ALL-UNNAMED",
            "--add-exports=java.security.jgss/sun.security.krb5.internal.ccache=ALL-UNNAMED",
            // JPE Kerberos: Prevent harmless ">>> Kerberos logger:" noise from its initialization
            // to stderr
            "-Dcom.ms.infra.kerberos.debug.quiet_init=true",
            "-classpath",
            desktopClasspath(infraPath).toString(),
            "com.ms.stratosphere.desktop.StratosphereLauncher");

    List<String> command = new ArrayList<>();

    command.addAll(baseCommand);
    command.addAll(Arrays.asList(args));
    command.removeIf(Objects::isNull);

    ProcessBuilder pb = new ProcessBuilder(command).inheritIO();
    pb.environment().put("STRATOSPHERE_INFRA", infraPath.toString());
    if (config.hasPath("internal.java.tool-options")) {
      pb.environment().put("JAVA_TOOL_OPTIONS", config.getString("internal.java.tool-options"));
    }

    System.out.println("Starting Stratosphere...");

    try {
      return pb.start().waitFor();
    } catch (Exception e) {
      e.printStackTrace();
      return FAILURE;
    }
  }

  private static Path desktopClasspath(Path infraPath) {
    Path pathingPath = infraPath.resolve("lib").resolve("desktop-runtimeAppPathing.jar");
    Path desktopPath = infraPath.resolve("lib").resolve("stratosphere-desktop.jar");

    // old strato versions don't have pathing jar
    if (Files.exists(pathingPath)) {
      return pathingPath;
    } else if (Files.exists(desktopPath)) {
      return desktopPath;
    } else {
      String msg = Messages.noLongerExistsMessage(infraPath);
      throw new RuntimeException(msg);
    }
  }

  private static int runSnapshotInfra(String[] args, Config config) {
    String stratosphereInfraOverride = System.getenv("STRATOSPHERE_INFRA_OVERRIDE");
    Path infraPath =
        Paths.get(
            stratosphereInfraOverride != null
                ? stratosphereInfraOverride
                : System.getenv("STRATOSPHERE_INFRA"));
    return runInfra(infraPath, args, config);
  }

  private static int runNewInfra(final String[] args, final Config config) throws Exception {
    Path infraPath = Paths.get(config.getString("stratosphereInfra"));
    return runInfra(infraPath, args, config);
  }

  public static void main(String[] args) {
    System.exit(run(args));
  }

  private static int run(String[] args) {
    String baseFailureMessage = Messages.baseFailureMessage();

    try {
      String command = (args.length > 0) ? args[0] : "";
      return runCommand(command, args);
    } catch (RecoverableStratosphereException e) {
      System.err.println("[ERROR] " + e.getMessage());
      System.err.println(System.lineSeparator() + Messages.recoverableErrorMessage());
      return FAILURE;
    } catch (StratosphereException se) {
      System.err.println(baseFailureMessage);
      System.err.println(se.getMessage());
      if (se.getCause() != null) {
        System.err.println("");
        se.getCause().printStackTrace();
      }
      return FAILURE;
    } catch (Exception e) {
      String msg = "[ERROR] " + e.toString() + "\n" + baseFailureMessage;
      System.err.println(msg);
      e.printStackTrace();
      return FAILURE;
    }
  }

  private static int runCommand(String command, String[] args) throws Exception {
    Path workspace = WorkspaceRoot.find();
    final Config config = StratosphereConfig.loadFromCurrentDir();
    final List<String> commandsToRunWithSnapshotInfra =
        config.getStringList("internal.commands-to-run-with-snapshot-infra");

    if (workspace != null) {
      int migrationStatus =
          MigrationManager.runIfNeeded(
              workspace, command, config, /* showTrayNotification = */ true);
      if (migrationStatus == FAILURE) {
        return migrationStatus;
      }
    }
    return (workspace == null || commandsToRunWithSnapshotInfra.contains(command.toLowerCase()))
        ? runSnapshotInfra(args, config)
        : runNewInfra(args, config);
  }
}
