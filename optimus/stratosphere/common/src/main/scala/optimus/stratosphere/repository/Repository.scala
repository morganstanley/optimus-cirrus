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

import optimus.stratosphere.utils.EnvironmentUtils
import optimus.stratosphere.utils.RemoteUrl

import java.util.regex.Pattern

/**
 * Matcher for short repository names.
 */
class Repository(userName: String, bitbucketHostname: String) {
  private[repository] def privateUrl(repo: String): String =
    s"http://$userName@$bitbucketHostname/atlassian-stash/scm/~$userName/$repo.git"

  private val defaultMeta = "optimus"

  private val bitbucketRepo =
    """https?\:\/\/.+@""" + Pattern.quote(bitbucketHostname) + """\:?[0-9]*\/atlassian-stash\/scm\/.*\.git"""
  val linuxLocalRepo = """(?:\/[^\/]+)+\/?"""
  val windowsLocalRepo = """(?:[A-Z]\:|\\)(?:\\[^\\]+)+\\?"""
  private val BitbucketUrl = s"^($bitbucketRepo)$$".r
  private val LocalRepo = s"^(|$windowsLocalRepo|$linuxLocalRepo)$$".r

  private def repoUrl(meta: String, project: String, repo: String): String =
    s"http://$userName@$bitbucketHostname/atlassian-stash/scm/${meta}_$project/$repo.git"

  // https does not work with Kerberos, we need http instead
  private def removeHttps(url: String): String = url.replace("https://", "http://")

  def unapply(remoteUrl: RemoteUrl): Option[RepositoryLocation] = unapply(remoteUrl.url)

  def unapply(repoPath: String): Option[RepositoryLocation] =
    repoPath match {
      case BitbucketUrl(url) if url != null =>
        Some(RawRepository(RemoteUrl(removeHttps(url)), local = false))
      case LocalRepo(path) if path != null =>
        Some(RawRepository(RemoteUrl(path), local = true))
      case _ =>
        val elements =
          // handles "msde/train_optimus_dal/train"
          if (repoPath.count(_ == '/') == 2) repoPath.split("/")
          // handles "MSDE_TRAIN_OPTIMUS_DAL/train"
          else repoPath.split("_", /* limit = */ 2).flatMap(_.split("/"))
        elements match {
          case Array("private", repo @ _*) =>
            Some(RawRepository(RemoteUrl(privateUrl(repo.mkString("_"))), local = false))
          case Array(project, repo) if project.nonEmpty =>
            Some(ProjectRepository(defaultMeta, project, repo, RemoteUrl(repoUrl(defaultMeta, project, repo))))
          case Array(meta, project, repo) if meta.nonEmpty =>
            Some(
              ProjectRepository(
                meta.toLowerCase,
                project.toLowerCase,
                repo,
                RemoteUrl(repoUrl(meta.toLowerCase, project.toLowerCase, repo))))
          case _ =>
            None
        }
    }
}

object Repository {

  def resolveRepositoryLocation(remote: String, bitbucketHostname: String): Option[RepositoryLocation] = {
    val Repo = new Repository(EnvironmentUtils.userName, bitbucketHostname)
    val repository = remote match {
      case Repo(location) => Some(location)
      case _              => None
    }
    repository
  }

}
