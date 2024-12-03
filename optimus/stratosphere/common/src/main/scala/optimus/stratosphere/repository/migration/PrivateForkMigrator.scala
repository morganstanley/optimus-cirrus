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
package optimus.stratosphere.repository.migration

import optimus.stratosphere.bitbucket.BitbucketApiRestClient
import optimus.stratosphere.bitbucket.Project
import optimus.stratosphere.bitbucket.Repository
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.history.HistoryStitch
import optimus.stratosphere.repository.PrivateForkCreator
import optimus.stratosphere.utils.ConfigUtils
import optimus.stratosphere.utils.RemoteSpec
import optimus.stratosphere.utils.RemoteUrl

import java.time.Duration
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

/** Archive fork containing full history and switch to a new one */
final class PrivateForkMigrator(ws: StratoWorkspaceCommon, origin: RemoteSpec) {

  private val logger = ws.log
  private val userName = ws.userName
  private lazy val bitbucketClient = new BitbucketApiRestClient(ws)(ws.setup.timeouts.privateFork[Duration])

  def migrateIfNeeded(): Unit = {
    if (requiresMigration) {
      logger.info("Migrating workspace due to history truncation...")
      if (shouldCreateNewFork) {
        switchFork()
      }
      new HistoryStitch(ws).undoAll()
      saveWorkspaceMigrated()
    }
  }

  private def requiresMigration: Boolean = {
    val config = ws.internal.historyTruncation
    config.codetreeArchiveUrl.isDefined && !config.isWorkspaceMigrated.contains(true)
  }

  private def shouldCreateNewFork: Boolean = {
    val isOwner = BitbucketApiRestClient.repoOwner(origin.remoteUrl).contains(userName)
    lazy val codetreeArchive = ws.internal.historyTruncation.codetreeArchiveUrl.get
    lazy val Repository(Project(key), slug) = bitbucketClient.getPrivateRepoOrigin(userName, origin.remoteUrl.repoName)
    isOwner && key.equalsIgnoreCase(codetreeArchive.projectKey) && slug.equalsIgnoreCase(codetreeArchive.repoName)
  }

  private def switchFork(): Unit = {
    archiveCurrentOrigin()
    createNewFork()
    logger.info("Switched to a new fork")
  }

  private def archiveCurrentOrigin(): Unit = {
    ws.log.info("Archiving legacy fork...")
    val archivedForkName =
      s"${origin.remoteUrl.repoName}_archive_${DateTimeFormatter.ISO_LOCAL_DATE.format(ZonedDateTime.now())}"
    bitbucketClient.updatePrivateForkName(userName, origin.remoteUrl.repoName, archivedForkName)
  }

  private def createNewFork(): Unit = {
    val sourceRepo = RemoteUrl(ws.catchUp.remote.url)
    val forkCreator = new PrivateForkCreator(ws, bitbucketClient)
    forkCreator.create(origin.remoteUrl.repoName, sourceRepo, userName)
  }

  private def saveWorkspaceMigrated(): Unit = {
    val customConfFile = ws.directoryStructure.customConfFile
    val settingKey = "internal.history-truncation.is-workspace-migrated"
    ConfigUtils.updateConfigProperty(customConfFile)(settingKey, true)(ws)
  }

}
