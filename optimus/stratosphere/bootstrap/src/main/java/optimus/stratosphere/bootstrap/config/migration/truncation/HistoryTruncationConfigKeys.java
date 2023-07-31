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

public class HistoryTruncationConfigKeys {
  private static String makeGlobalKey(String subName) {
    return String.format("%s.%s", repositoryMigrationKeyRoot, subName);
  }

  private static String makeRepoKey(String repoName, String subName) {
    return makeGlobalKey(String.format("%s.%s", repoName, subName));
  }

  // We use versions in case we need to roll out a fix and make additional changes
  public static final int phaseTwoForkMigrationVersion = 1;

  private static final String repositoryMigrationKeyRoot = "internal.repository-migration";
  public static final String truncationNameKey = makeGlobalKey("name");
  public static final String truncationRepositoriesKey = makeGlobalKey("repositories");
  public static final String truncatedUpstreamRepoKey = makeGlobalKey("upstream-url");
  public static final String migratedRepositoriesKey = makeGlobalKey("repositories");
  public static final String migratedRepoMessageKey = makeGlobalKey("migrated-repo-message");
  public static final String leanifiedRepoMessageKey = makeGlobalKey("leanified-repo-message");
  public static final String nonOriginRemoteMessageKey = makeGlobalKey("non-origin-remote-message");
  public static final String pleaseMigrateMessageKey = makeGlobalKey("please-migrate-message");
  public static final String pleaseLeanUpMessageKey = makeGlobalKey("please-lean-up-message");
  public static final String privateForkToArchiveMessageWithNoPriorWarningKey =
      makeGlobalKey("private-fork-to-archive-no-prior-warning");
  public static final String privateForkToArchiveMessageWithPriorWarningKey =
      makeGlobalKey("private-fork-to-archive-with-prior-warning");
  public static final String selectedMigrationConfigLocation = makeGlobalKey("config-location");

  public final String namespace;
  public final String repoNamespaceKey;
  public final String repoNameKey;
  public final String repoUrlKey;
  public final String repoArchiveUrlKey;
  public final String repoSinceDateKey;
  public final String repoFromOriginUrlRegexesKey;
  public final String repoBranchesKey;
  public final String repoLeanifiedKey;
  public final String repoMigratedBranchesKey;
  public final String repoMigratedForkKey;
  public final String repoPrivateForkFromArchiveMigratedVersion;
  public final String repoPrivateForkFromLeanMigratedVersion;

  public HistoryTruncationConfigKeys(String namespace) {
    this.namespace = namespace;
    this.repoNamespaceKey = makeRepoKey(namespace, "namespace");
    this.repoNameKey = makeRepoKey(namespace, "repo-name");
    this.repoUrlKey = makeRepoKey(namespace, "repo-url");
    this.repoArchiveUrlKey = makeRepoKey(namespace, "archived-repo-url");
    this.repoSinceDateKey = makeRepoKey(namespace, "since-date");
    this.repoFromOriginUrlRegexesKey = makeRepoKey(namespace, "from-origin-url-regexes");
    this.repoBranchesKey = makeRepoKey(namespace, "branches");
    this.repoLeanifiedKey = makeRepoKey(namespace, "leanified");
    this.repoMigratedBranchesKey = makeRepoKey(namespace, "migrated-branches");
    this.repoMigratedForkKey = makeRepoKey(namespace, "migrated-fork");
    this.repoPrivateForkFromArchiveMigratedVersion =
        makeRepoKey(namespace, "migrated-private-fork-from-archive-version");
    this.repoPrivateForkFromLeanMigratedVersion =
        makeRepoKey(namespace, "migrated-private-fork-from-lean-version");
  }
}
