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

import optimus.stratosphere.utils.RemoteUrl

sealed abstract class RepositoryLocation(val remoteUrl: RemoteUrl, val local: Boolean) {

  protected val codetreeRepo = "codetree"

  final def bitbucketProject: String = remoteUrl.projectKey

  final def repoName: String = remoteUrl.repoName
}

/** A link to a project on bitbucket, for example OPTIMUS_CODETREE/codetree. */
final case class ProjectRepository(meta: String, project: String, name: String, override val remoteUrl: RemoteUrl)
    extends RepositoryLocation(remoteUrl, local = false) {

  lazy val isCodetree: Boolean =
    meta == "optimus" && project == codetreeRepo && repoName == codetreeRepo
}

object ProjectRepository {
  def apply(remoteUrl: RemoteUrl): ProjectRepository = {
    require(!remoteUrl.isFork, s"RemoteUrl is a fork, cannot be used to infer the project!")
    // Retrieves appropriate parts from url like:
    // http://username@company.com/atlassian-stash/scm/PROJECT_NAME/REPO_NAME.git
    val Array(metaProjectWithUnderscore, repositoryDotGit, _*) = remoteUrl.url.split("/").takeRight(2)
    val repository = repositoryDotGit.takeWhile(_ != '.')
    val Array(meta, project) = metaProjectWithUnderscore.split("_", /* limit = */ 2)
    ProjectRepository(meta, project, repository, remoteUrl)
  }
}

/** A link to a private fork on Bitbucket. */
final case class PrivateFork(override val remoteUrl: RemoteUrl) extends RepositoryLocation(remoteUrl, local = false)

/** A link to a local directory. Used mostly in testing or for reference repositories. */
final case class LocalRepository(override val remoteUrl: RemoteUrl) extends RepositoryLocation(remoteUrl, local = true)
