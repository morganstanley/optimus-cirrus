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

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import optimus.stratosphere.bootstrap.config.migration.MigrationUtils;
import com.typesafe.config.Config;

public class HistoryTruncationMigratedRepository {
  public final HistoryTruncationConfigKeys keys;
  public final String name;
  public final String url;
  public final String archiveUrl;
  public final LocalDate since;
  public final List<Pattern> fromUrlRegexes;
  public final List<String> branches;
  public final boolean leanified;
  public final List<String> migratedBranches;
  public final boolean migratedFork;
  public final int migratedPrivateForkFromArchiveVersion;
  public final int migratedPrivateForkFromLeanVersion;
  public final String migratedMessage;
  public final String leanifiedMessage;

  private HistoryTruncationMigratedRepository(Config config, HistoryTruncationConfigKeys keys) {
    this.keys = keys;
    this.name = config.getString(keys.repoNameKey);
    this.url = config.getString(keys.repoUrlKey);
    this.archiveUrl = config.getString(keys.repoArchiveUrlKey);
    this.since = MigrationUtils.getDateFrom(config.getString(keys.repoSinceDateKey));
    this.fromUrlRegexes =
        config.getStringList(keys.repoFromOriginUrlRegexesKey).stream()
            .map(Pattern::compile)
            .collect(Collectors.toList());
    this.branches = config.getStringList(keys.repoBranchesKey);
    this.leanified = config.getBoolean(keys.repoLeanifiedKey);
    if (config.hasPath(keys.repoMigratedBranchesKey)) {
      this.migratedBranches = config.getStringList(keys.repoMigratedBranchesKey);
    } else {
      this.migratedBranches = Collections.emptyList();
    }
    if (config.hasPath(keys.repoMigratedForkKey)) {
      this.migratedFork = config.getBoolean(keys.repoMigratedForkKey);
    } else {
      this.migratedFork = false;
    }
    if (config.hasPath(keys.repoPrivateForkFromArchiveMigratedVersion)) {
      this.migratedPrivateForkFromArchiveVersion =
          config.getInt(keys.repoPrivateForkFromArchiveMigratedVersion);
    } else {
      this.migratedPrivateForkFromArchiveVersion = 0;
    }
    if (config.hasPath(keys.repoPrivateForkFromLeanMigratedVersion)) {
      this.migratedPrivateForkFromLeanVersion =
          config.getInt(keys.repoPrivateForkFromLeanMigratedVersion);
    } else {
      this.migratedPrivateForkFromLeanVersion = 0;
    }
    this.migratedMessage =
        formatMessage(config.getString(HistoryTruncationConfigKeys.migratedRepoMessageKey), this);
    this.leanifiedMessage =
        formatMessage(config.getString(HistoryTruncationConfigKeys.leanifiedRepoMessageKey), this);
  }

  public static String formatMessage(String message, HistoryTruncationMigratedRepository repo) {
    return message
        .replaceAll("@repo-name@", repo.name)
        .replaceAll("@repo-url@", repo.url)
        .replaceAll("@archived-repo-url@", repo.archiveUrl)
        .replaceAll("@branches@", String.join(", ", repo.branches));
  }

  public String getNamespace() {
    return keys.namespace;
  }

  boolean isBranchMigrationNeeded() {
    return !migratedBranches.containsAll(branches);
  }

  boolean isLeanRepoMigrationNeeded() {
    // All we want is that we hit either case once. We are unlikely to hit both from the onset.
    // We will see both in the process of moving stuff from the obsolete private to a new one, but
    // the new private
    // will be fixed by the 'stratosphere setup'.
    // If user runs older version, we don't want to take action.
    return leanified
        && migratedPrivateForkFromArchiveVersion
            < HistoryTruncationConfigKeys.phaseTwoForkMigrationVersion
        && migratedPrivateForkFromLeanVersion
            < HistoryTruncationConfigKeys.phaseTwoForkMigrationVersion;
  }

  public static Optional<HistoryTruncationMigratedRepository> create(
      Config config, String repoNamespace) {
    HistoryTruncationConfigKeys keys = new HistoryTruncationConfigKeys(repoNamespace);
    if (config.hasPath(keys.repoNameKey)) {
      return Optional.of(new HistoryTruncationMigratedRepository(config, keys));
    } else {
      return Optional.empty();
    }
  }
}
