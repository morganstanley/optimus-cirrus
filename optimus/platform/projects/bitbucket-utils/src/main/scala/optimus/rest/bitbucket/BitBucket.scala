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

import optimus.rest.RestApi
import spray.json._

import scala.collection.immutable.Seq
import scala.concurrent.duration._

abstract class BitBucket(protected val instance: String, val timeout: Duration = 15.seconds)
    extends BitBucketApiUrls
    with RestApi
    with RecursiveQuerier {

  def getPrData(project: String, repo: String, prNumber: Int): PrData =
    get[BasePrData](apiPrUrl(project, repo, prNumber)).withInstance(instance)

  def getBasePrData(project: String, repo: String, prNumber: Int): BasePrData =
    get[BasePrData](apiPrUrl(project, repo, prNumber))

  def getRecentPrData(project: String, repo: String, quantityWanted: Int = 500): Seq[BasePrData] = {
    val url = s"${apiPrActivitesUrl(project, repo)}?state=ALL"
    queryPaged[BasePrData, PagedPrDataRequest](url, Some(quantityWanted))
  }

  def getPrDataByBranch(
      project: String,
      repo: String,
      branch: String,
      state: String,
      max: Option[Int]): Seq[BasePrData] = {
    val refhead = s"refs/heads/release/$branch"
    val url = s"${apiPrActivitesUrl(project, repo)}?state=$state&at=$refhead"
    queryPaged[BasePrData, PagedPrDataRequest](url, max, 500)
  }

  def getPrActivities(project: String, repo: String, prNumber: Int): Seq[PrActivity] =
    queryPaged[PrActivity, ActivityRequest](apiPrActivitiesByPrUrl(project, repo, prNumber))

  def addComment(project: String, repo: String, prNumber: Int, text: String, parentId: Option[String] = None): Int = {
    val postUrl = apiPrCommentsUrl(project, repo, prNumber)
    val data = AddComment(text, parentId.map(Parent.apply))
    post[Id](postUrl, Some(data.toJson.toString())).id
  }

  def updateComment(
      project: String,
      repo: String,
      prNumber: Int,
      commentId: Int,
      version: Int,
      newText: String): Int = {
    val putUrl = apiPrCommentsByCommitIdUrl(project, repo, prNumber, commentId)
    val data = UpdateComment(newText, version)
    put[Id](putUrl, Some(data.toJson.toString())).id
  }

  def findCommentById(commentsOnPr: Seq[PrActivity], commentId: String): Option[PrComment] =
    commentsOnPr
      .filter(_.action == PRAction.Commented.toString)
      .find(activity => activity.comment.exists(_.text.contains(commentId)))
      .flatMap(_.comment)

  def findCommentById(project: String, repo: String, prNumber: Int, commentId: Int): Option[PrComment] = {
    val getUrl = apiPrCommentsByCommitIdUrl(project, repo, prNumber, commentId)
    Some(get[PrComment](getUrl))
  }

  def addOrUpdateComment(
      project: String,
      repo: String,
      prNumber: Int,
      newText: String,
      existingComment: Option[PrComment]
  ): CommentUpdate = {
    existingComment match {
      case Some(existingComment) =>
        if (existingComment.text != newText) {
          val version = existingComment.version
          val commentId = existingComment.id
          UpdatedComment(updateComment(project, repo, prNumber, commentId, version, newText))
        } else
          NotUpdatedComment(existingComment.id)
      case None =>
        NewComment(addComment(project, repo, prNumber, newText))
    }
  }

  def addTasks(tasks: Seq[String], commentId: Int): Seq[PrTask] =
    tasks.map(addTask(_, commentId))

  def addTask(text: String, commentId: Int): PrTask = {
    val postUrl = apiTasksUrl
    val data = AddTask(text, Anchor(commentId, "COMMENT"))
    post[PrTask](postUrl, Some(data.toJson.toString))
  }

  def updateTask(state: String, taskId: Int): Int = {
    val updateUrl = apiUrl("api", s"tasks/$taskId")
    val data = UpdateTaskState(state)
    put[Id](updateUrl, Some(data.toJson.toString)).id
  }

  private def projectUrl(project: String, repo: String): String =
    s"http://$instance/atlassian-stash/projects/$project/repos/$repo"

  def prWebpage(project: String, repo: String, prNumber: Int, subUrl: String = ""): String =
    s"${projectUrl(project, repo)}/pull-requests/$prNumber/$subUrl"

  def htmlPrLink(project: String, repo: String, prNumber: Int, subUrl: String = ""): String =
    s"""<a href="${prWebpage(project, repo, prNumber, subUrl)}" target="_blank">PR#$prNumber</a>"""

  def fileWebPage(
      project: String,
      repo: String,
      filePath: String,
      gitRef: Option[String] = None,
      lineNumberSelector: Option[Int] = None): String = {
    val refPart = gitRef.fold("")("?at=" + _)
    val linePart = lineNumberSelector.fold("")("#" + _)
    s"${projectUrl(project, repo)}/browse/$filePath$refPart$linePart"
  }

  def getBrowseData(
      project: String,
      repo: String,
      browsePath: String,
      branchOption: Option[String] = None): BrowseData =
    get[BrowseData](apiBrowseUrl(project, repo, browsePath, branchOption))

  def getAllCommitsByPrNumber(project: String, repo: String, prNumber: Int): Seq[CommitId] =
    queryPaged[CommitId, CommitActivityRequest](apiGetAllCommitsByPrUrl(project, repo, prNumber))

  def getChangedFilesByCommitId(project: String, repo: String, commitId: String): Seq[CommitFile] =
    queryPaged[CommitFile, CommitRequest](apiChangedFilesByCommitUrl(project, repo, commitId))

  def getFileSizeByFilePath(project: String, repo: String, filePath: String, commitId: String): Int = {
    val url = apiFileSizeByFilePathUrl(project, repo, filePath, commitId)
    get[CommitFileSize](url).size
  }

  def getAllFileSizes(project: String, repo: String, commitId: CommitId): Seq[CommitFilePathSize] = {
    val commitFiles = getChangedFilesByCommitId(project, repo, commitId.id).filter(_.`type` != "DELETE")
    commitFiles.flatMap { commitFile =>
      val path = commitFile.path
      val filePath = s"${path.parent}/${path.name}"
      val fileSize = getFileSizeByFilePath(project, repo, filePath, commitId.id)
      Seq(CommitFilePathSize(filePath, fileSize, commitId.id, commitId.displayId))
    }
  }

  def annotateCommit(commit: String, payload: String): Unit = {
    val url = apiGetBuildStatus(commit)
    postWithoutResponse(url, Some(payload))
  }
}
