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
package optimus.stratosphere.bootstrap.config.migration.truncation;

import java.awt.AWTException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import optimus.stratosphere.bootstrap.IniFile;
import optimus.stratosphere.bootstrap.OsSpecific;
import optimus.stratosphere.bootstrap.TrayNotification;
import optimus.stratosphere.bootstrap.config.StratosphereConfig;
import optimus.stratosphere.bootstrap.config.migration.MigrationFiles;
import com.typesafe.config.Config;

import static optimus.stratosphere.bootstrap.config.migration.truncation.HistoryTruncationConfigKeys.*;

public class HistoryTruncationMigration extends MigrationFiles {
  private final Pattern remotePattern = Pattern.compile("remote \"(.*)\"");
  private final String dividingLine =
      "----------------------------------------------------------------------";
  private final String innerDividingLine = "\n" + dividingLine + "\n";

  private final Config config;

  public HistoryTruncationMigration(Path workspaceRoot) {
    super(workspaceRoot);
    this.config = StratosphereConfig.get(workspaceRoot);
  }

  public boolean isMigrationNeeded() {
    // No side effect please!
    return isMigrationNeeded(new StringBuffer());
  }

  /*
   * Keep operations here as lightweight was possible.
   */
  public boolean isMigrationNeeded(StringBuffer msg) {
    List<HistoryTruncationMigratedRemote> migratedRemotes = getMigratedRepositories();
    if (migratedRemotes.size() == 0) {
      return false;
    }

    List<HistoryTruncationMigratedRemote> remotesRequiringAction =
        migratedRemotes.stream()
            .filter(
                remote -> {
                  if (remote.isForkPhaseOneMigrationNeeded()
                      || remote.isForkPhaseTwoMigrationNeeded()) {
                    return true;
                  }

                  // If remote branches were not locally migrated
                  //    --otherwise assumed that all local branches were updated as well
                  return remote.repository.branches.stream()
                      .anyMatch(branch -> !remote.repository.migratedBranches.contains(branch));
                })
            .collect(Collectors.toList());

    boolean phaseOneMigrationRequired =
        remotesRequiringAction.stream()
            .anyMatch(HistoryTruncationMigratedRemote::isPhaseOneMigrationNeeded);
    boolean phaseTwoMigrationRequired =
        remotesRequiringAction.stream()
            .anyMatch(HistoryTruncationMigratedRemote::isPhaseTwoMigrationNeeded);

    // Display "origin" messages first
    remotesRequiringAction.forEach(
        remote -> {
          if (remote.isOrigin()) {
            if (remote.isPhaseTwoMigrationNeeded()) {
              // We need to create new fork and use "strato setup"
              msg.append(remote.repository.leanifiedMessage);
              msg.append(innerDividingLine);
            } else if (remote.isPhaseOneMigrationNeeded()) {
              // We need to use "strato migrate"
              msg.append(remote.repository.migratedMessage);
              msg.append(innerDividingLine);
            }
          }
        });
    // Display other remote messages next
    remotesRequiringAction.forEach(
        remote -> {
          if (!remote.isOrigin()) {
            msg.append(remote.nonOriginRemoteMessage);
            msg.append(innerDividingLine);
          }
        });

    if (phaseOneMigrationRequired) {
      msg.append(config.getString(pleaseMigrateMessageKey));
    }
    if (phaseOneMigrationRequired && phaseTwoMigrationRequired) {
      msg.append(innerDividingLine);
    }
    if (phaseTwoMigrationRequired) {
      msg.append(config.getString(pleaseLeanUpMessageKey));
    }
    return !remotesRequiringAction.isEmpty();
  }

  public List<HistoryTruncationMigratedRemote> getMigratedRepositories() {
    if (!config.hasPath(migratedRepositoriesKey)) {
      return Collections.emptyList();
    }

    List<String> migratedRepositoryNames = config.getStringList(migratedRepositoriesKey);
    List<HistoryTruncationMigratedRepository> migratedRepositories =
        migratedRepositoryNames.stream()
            .map(repoName -> HistoryTruncationMigratedRepository.create(config, repoName))
            .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
            .collect(Collectors.toList());

    Function<Map.Entry<String, Properties>, Optional<HistoryTruncationMigratedRemote>>
        findMigratedRemote =
            section -> {
              if (section.getKey().startsWith("remote ") && section.getValue().containsKey("url")) {
                Matcher matcher = remotePattern.matcher(section.getKey());
                final String remoteName;
                if (matcher.find()) {
                  remoteName = matcher.group(1);
                  String url = section.getValue().get("url").toString();
                  return migratedRepositories.stream()
                      .filter(
                          repo ->
                              repo.fromUrlRegexes.stream()
                                      .anyMatch(regex -> regex.matcher(url).matches())
                                  && !LocalDate.now().isBefore(repo.since))
                      .findFirst()
                      .map(
                          repo ->
                              new HistoryTruncationMigratedRemote(config, remoteName, url, repo));
                }
                return Optional.empty();
              }
              return Optional.empty();
            };

    // No git operations here please! But be as swift as possible.
    if (gitConfig.toFile().isFile()) {
      try (FileReader fileReader = new FileReader(gitConfig.toFile())) {
        Map<String, Properties> gitConfigContent = IniFile.parse(fileReader);
        return gitConfigContent.entrySet().stream()
            .map(findMigratedRemote)
            .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
            .collect(Collectors.toList());
      } catch (IOException e) {
        String msg =
            "[ERROR] Stratosphere could not properly determine if migration is required: '"
                + e.toString()
                + "'.\n";
        System.err.println(msg);
        e.printStackTrace();
      }
    }
    return Collections.emptyList();
  }

  private String addHeader(String msg) {
    List<String> lines = new LinkedList<>();
    for (String line : msg.split("\n")) {
      lines.add(line.trim().replace("|", ""));
    }

    return String.join(
        "\n",
        "|" + dividingLine,
        "|",
        "| WARNING: Repository migration recommended",
        "|",
        "|" + dividingLine,
        "| " + String.join("\n|", lines),
        "|" + dividingLine);
  }

  private List<String> exemptedCommands =
      Arrays.asList("deleteWorkspace", "diag", "migrate", "setup", "switchToPrivateFork");

  public void printWarningIfNeeded(String command, boolean showTrayNotification)
      throws AWTException {
    if (exemptedCommands.contains(command) || OsSpecific.isCi()) {
      // we should not display warnings if the user is already calling
      // migration-enabled or workspace-independent commands
      return;
    }
    final StringBuffer msg = new StringBuffer();
    boolean migrationNeeded = isMigrationNeeded(msg);
    if (migrationNeeded) {
      if (msg.length() > 0) {
        System.out.println(addHeader(msg.toString()));
      }
      if (showTrayNotification) {
        TrayNotification.show(
            "Repository Migration Recommended",
            "Repository Migration Recommended",
            String.format(
                "Migration of the '%s' repository may be needed. "
                    + "Please run `stratosphere "
                    + "migrate` from command line to see more details.",
                config.getString("stratosphereWorkspace")));
      }
    }
  }
}
