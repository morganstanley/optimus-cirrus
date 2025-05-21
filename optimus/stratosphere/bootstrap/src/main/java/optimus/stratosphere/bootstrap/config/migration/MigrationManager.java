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
package optimus.stratosphere.bootstrap.config.migration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import com.typesafe.config.Config;
import optimus.stratosphere.bootstrap.Stratosphere;
import optimus.stratosphere.bootstrap.WorkspaceRoot;
import optimus.stratosphere.bootstrap.config.migration.p4.NewToOldLayoutMigration;
import optimus.stratosphere.bootstrap.config.migration.p4.OldToNewLayoutMigration;
import optimus.stratosphere.bootstrap.config.migration.truncation.HistoryTruncationMigration;

public class MigrationManager {

  private static final String CODETREE_ARCHIVE_URL_PATH =
      "internal.history-truncation.codetree-archive-url";

  public static boolean requiresForkMigration(Config config, String command) {
    return config.hasPath(CODETREE_ARCHIVE_URL_PATH)
        && Stream.of("catchup", "forkSync").anyMatch(command::equalsIgnoreCase);
  }

  public static int runIfNeeded(
      Path workspaceRoot, String command, Config config, boolean showTrayNotification) {
    try {
      boolean isNewLayout =
          Files.exists(workspaceRoot.resolve("src/" + WorkspaceRoot.STRATOSPHERE_CONFIG_FILE));
      if (isNewLayout) {
        new OldToNewLayoutMigration(workspaceRoot).runIfNeeded();
      } else {
        new NewToOldLayoutMigration(workspaceRoot, config).runIfNeeded();
      }
    } catch (IOException e) {
      System.err.println("[ERROR] Problem occurred during migration of configuration:");
      System.err.println(e.getMessage());
      return Stratosphere.FAILURE;
    }

    try {
      new HistoryTruncationMigration(workspaceRoot, config)
          .printWarningIfNeeded(command, showTrayNotification);
    } catch (Exception e) {
      System.err.println(
          "[ERROR] Problem occurred while checking if history truncation migration is needed:");
      e.printStackTrace();
      return Stratosphere.FAILURE;
    }

    return Stratosphere.SUCCESS;
  }
}
