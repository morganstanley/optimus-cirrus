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
package optimus.rest.bitbucket

import optimus.rest.Urls

/**
 * For the API related URLs.
 *
 * For the non-API-related, see [[BitbucketUrls]].
 */
trait BitBucketApiUrls {
  protected def instance: String

  protected def apiUrl(apiName: String, apiResource: String) =
    s"http://$instance/atlassian-stash/rest/$apiName/1.0/$apiResource"

  protected def apiUsersUrl: String = apiUrl("api", "users")

  protected def apiRepoUrl(project: String, repo: String): String =
    apiUrl("api", s"projects/$project/repos/$repo")

  protected def apiPrUrl(project: String, repo: String, prNumber: Long): String =
    s"${apiRepoUrl(project, repo)}/pull-requests/$prNumber"

  protected def apiPrBlockerCommentsUrl(project: String, repo: String, prNumber: Long): String =
    s"${apiPrUrl(project, repo, prNumber)}/blocker-comments"

  protected def apiPrBlockerCommentUpdateUrl(project: String, repo: String, prNumber: Long, taskId: Int): String =
    s"${apiPrBlockerCommentsUrl(project, repo, prNumber)}/$taskId"

  protected def apiPrsUrl(project: String, repo: String): String =
    s"${apiRepoUrl(project, repo)}/pull-requests"

  protected def apiPrActivitiesUrl(project: String, repo: String, prNumber: Long): String =
    s"${apiPrsUrl(project, repo)}/$prNumber/activities"

  protected def apiPrCommentsUrl(project: String, repo: String, prNumber: Long): String =
    s"${apiRepoUrl(project, repo)}/pull-requests/$prNumber/comments"

  protected def apiPrCommentsByCommentIdUrl(project: String, repo: String, prNumber: Long, commentId: Int): String =
    s"${apiRepoUrl(project, repo)}/pull-requests/$prNumber/comments/$commentId"

  protected def apiTasksUrl(project: String, repo: String, prNumber: Long): String = apiUrl("api", "blocker-comments")

  protected def apiListFilesUrl(
      project: String,
      repo: String,
      dirToList: String,
      branchOption: Option[String]): String = {
    val base = s"${apiRepoUrl(project, repo)}/files/$dirToList"
    branchOption.fold(base)(branch => s"$base?at=$branch")
  }

  protected def apiBrowseUrl(
      project: String,
      repo: String,
      browsePath: String,
      branchOption: Option[String]): String = {
    // File path may contain some strange characters, we need to urlencode
    val base = s"${apiRepoUrl(project, repo)}/browse/${browsePath.split("/").map(Urls.encode).mkString("/")}"
    branchOption.fold(base)(branch => s"$base?at=$branch")
  }

  protected def apiChangedFilesByCommitUrl(project: String, repo: String, commitId: String): String =
    s"${apiRepoUrl(project, repo)}/commits/$commitId/changes"

  protected[bitbucket] def apiFileSizeByFilePathUrl(
      project: String,
      repo: String,
      filePath: String,
      commitId: String): String =
    s"${apiBrowseUrl(project, repo, filePath, branchOption = None)}?size=true&at=$commitId"

  protected def apiGetAllCommitsByPrUrl(project: String, repo: String, prNumber: Long): String =
    s"${apiPrUrl(project, repo, prNumber)}/commits"

  protected def apiGetBuildStatus(commitId: String): String =
    apiUrl("build-status", s"commits/$commitId")

  protected def apiPrChangeDetails(project: String, repo: String, prNumber: Long): String =
    s"${apiPrUrl(project, repo, prNumber)}/changes"
}
