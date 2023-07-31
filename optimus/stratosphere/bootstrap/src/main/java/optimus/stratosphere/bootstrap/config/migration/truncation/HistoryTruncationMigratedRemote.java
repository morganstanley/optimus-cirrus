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

import com.typesafe.config.Config;

public class HistoryTruncationMigratedRemote {
  public final String remoteName;
  public final String url;
  public final HistoryTruncationMigratedRepository repository;
  public final String nonOriginRemoteMessage; // TODO (OPTIMUS-29807) Consider removing or renaming

  public HistoryTruncationMigratedRemote(
      Config config,
      String remoteName,
      String url,
      HistoryTruncationMigratedRepository repository) {
    this.remoteName = remoteName;
    this.url = url;
    this.repository = repository;

    this.nonOriginRemoteMessage =
        HistoryTruncationMigratedRepository.formatMessage(
                config.getString(HistoryTruncationConfigKeys.nonOriginRemoteMessageKey), repository)
            .replaceAll("@remote-name@", remoteName);
  }

  public boolean isOrigin() {
    return remoteName.equals("origin");
  }

  public boolean isPrivateFork() {
    return url.contains("scm/~");
  }

  public boolean isPhaseOneMigrationNeeded() {
    return repository.isBranchMigrationNeeded() || isForkPhaseOneMigrationNeeded();
  }

  public boolean isPhaseTwoMigrationNeeded() {
    return repository.isLeanRepoMigrationNeeded();
  }

  public boolean isForkPhaseOneMigrationNeeded() {
    return !repository.migratedFork
        && isPrivateFork(); // Fork migration only applies to private forks
  }

  public boolean isForkPhaseTwoMigrationNeeded() {
    return isPhaseTwoMigrationNeeded()
        && isPrivateFork(); // Fork migration only applies to private forks
  }
}
