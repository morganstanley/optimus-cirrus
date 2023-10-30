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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import optimus.stratosphere.bootstrap.OsSpecific;
import optimus.stratosphere.bootstrap.config.migration.MigrationFiles;

public class OldToNewLayoutMigration extends MigrationFiles {

  public OldToNewLayoutMigration(Path workspaceRoot) {
    super(workspaceRoot);
  }

  public void runIfNeeded() throws IOException {
    if (Files.exists(workspaceScriptFile) && !Files.exists(customConfFile)) {
      migrateConfig();
    }
  }

  private void migrateConfig() throws IOException {
    Files.createDirectories(customConfFile.getParent());
    try (PrintWriter pw = new PrintWriter(customConfFile.toString())) {
      Config customConf = ConfigFactory.empty();
      for (String workspaceCmdLine :
          Files.readAllLines(workspaceScriptFile, Charset.forName("UTF-8"))) {
        if (workspaceCmdLine.startsWith(OsSpecific.envSetter)) {
          String[] property = workspaceCmdLine.replace(OsSpecific.envSetter, "").split("=");
          if (property.length == 2) {
            String propertyName = null;
            if (property[0].equals("STRATOSPHERE_INSTALL")) {
              propertyName = "stratosphereInstallDir";
            }

            if (propertyName != null) {
              String propertyValue = property[1].replace("\"", "");
              customConf =
                  customConf.withValue(propertyName, ConfigValueFactory.fromAnyRef(propertyValue));
            }
          }
        }
      }

      if (Files.exists(useBigHeapFile)) {
        String bigHeapProperty = "intellij.compile-server.vmoptions.heap.useBig";
        customConf = customConf.withValue(bigHeapProperty, ConfigValueFactory.fromAnyRef(true));
      }

      pw.println(customConf.root().render(ConfigRenderOptions.concise().setFormatted(true)));
    }
  }
}
