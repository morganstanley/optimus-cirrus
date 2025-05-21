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
package optimus.stratosphere.repository

import optimus.stratosphere.bitbucket.BitbucketApiRestClient
import optimus.stratosphere.bitbucket.RepoPermission
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.utils.RemoteUrl

final class PrivateForkCreator(ws: StratoWorkspaceCommon, val bitbucketClient: BitbucketApiRestClient) {

  private val log = ws.log

  def create(forkName: String, sourceRepository: RemoteUrl, userName: String): RemoteUrl =
    create(forkName, sourceRepository.projectKey, sourceRepository.repoName, userName)

  def create(
      forkName: String,
      projectKey: String,
      repository: String,
      userName: String,
  ): RemoteUrl = {
    log.info(s"Creating new fork '$forkName' for $projectKey/$repository...")
    val forkRemoteUrl = bitbucketClient.createPrivateFork(projectKey, repository, forkName)

    val privateForkReadGroup = ws.internal.bitbucket.allUsersGroup

    log.info(s"Setting read access for '$privateForkReadGroup' group on fork '$forkName'")
    bitbucketClient.grantAccessToGroup(userName, forkName, privateForkReadGroup, RepoPermission.REPO_READ)

    log.info("Setting write access for self on fork (needed if your Access Designation is PROD)...")
    bitbucketClient.grantWriteAccessToSelf(userName, forkName)

    log.info("Making sure the fork is synced with upstream")
    bitbucketClient.setForkSyncing(userName, forkName, enabled = false)
    bitbucketClient.setForkSyncing(userName, forkName, enabled = true)

    forkRemoteUrl
  }

}
