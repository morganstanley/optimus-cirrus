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
package optimus.stratosphere.bootstrap.config.migration.p4;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import com.typesafe.config.Config;
import optimus.stratosphere.bootstrap.OsSpecific;
import optimus.stratosphere.bootstrap.config.migration.MigrationFiles;

public class NewToOldLayoutMigration extends MigrationFiles {
  private final Config config;

  public NewToOldLayoutMigration(Path workspaceRoot, Config config) {
    super(workspaceRoot);
    this.config = config;
  }

  public void runIfNeeded() throws IOException {
    if (!Files.exists(workspaceScriptFile) && Files.exists(customConfFile)) {
      Files.createDirectories(stratosphereWsUserHome);
      migrateConfig();
      migrateScalaVersion();
      migrateBigHeapSettings();
    }
  }

  private void migrateBigHeapSettings() throws IOException {
    if (config.getBoolean("intellij.compile-server.vmoptions.heap.useBig")) {
      useBigHeapFile.toFile().createNewFile();
    }
  }

  private void migrateScalaVersion() throws FileNotFoundException {
    try (PrintWriter pw = new PrintWriter(scalaVersionFile.toFile())) {
      pw.print(config.getString("scalaVersion").substring(0, 4));
    }
  }

  private String setEnvString(String envName, String propertyName) {
    String value = (config.hasPath(propertyName) ? config.getString(propertyName) : "");
    return OsSpecific.setEnvString(envName, value);
  }

  private void migrateConfig() throws FileNotFoundException {
    try (PrintWriter pw = new PrintWriter(workspaceScriptFile.toFile())) {

      pw.println(setEnvString("STRATOSPHERE_HOME", "stratosphereHome"));
      pw.println(setEnvString("STRATOSPHERE_WORKSPACE", "stratosphereWorkspace"));
      pw.println(setEnvString("STRATOSPHERE_WS_DIR", "stratosphereWsDir"));
      pw.println(setEnvString("STRATOSPHERE_INSTALL", "stratoInstall"));
      pw.println();

      if (OsSpecific.isWindows) {
        pw.println("call module load msde/git");
        pw.println(
            "for /f \"delims=\" %%x in ('git rev-parse --abbrev-ref HEAD') do @set STRATOSPHERE_BRANCH=%%x");
        pw.println("set TERM=msys");
      } else {
        pw.println("module load msde/git");
        pw.println("export STRATOSPHERE_BRANCH=`git rev-parse --abbrev-ref HEAD`");
      }
    }
  }
}
