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
import optimus.rest.UnsuccessfulRestCall
import spray.json._

import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.util.Try

abstract class BitBucket(protected val instance: String, val timeout: Duration = 15.seconds)
    extends BitBucketApiUrls
    with RestApi
    with RecursiveQuerier {

  def getPrData(project: String, repo: String, prNumber: Long): PrData =
    handleErrors { get[BasePrData](apiPrUrl(project, repo, prNumber)).withInstance(instance) }

  def getBasePrData(project: String, repo: String, prNumber: Long): BasePrData =
    handleErrors { get[BasePrData](apiPrUrl(project, repo, prNumber)) }

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

  def findCommentById(project: String, repo: String, prNumber: Int, commentId: Int): Option[PrComment] =
    Try(get[PrComment](apiPrCommentsByCommitIdUrl(project, repo, prNumber, commentId))).toOption

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

  def getBlockerComment(project: String, repo: String, prNumber: Int, commentId: Int): PrBlockerComment =
    get[PrBlockerComment](apiPrBlockerCommentUpdateUrl(project, repo, prNumber, commentId))

  def addTasks(project: String, repo: String, prNumber: Int, tasks: Seq[String], commentId: Int): Seq[PrTask] =
    tasks.map(t => addTask(project, repo, prNumber, t, commentId))

  def addTask(project: String, repo: String, prNumber: Int, text: String, commentId: Int): PrTask = {
    val postUrl = apiPrBlockerCommentsUrl(project, repo, prNumber)
    val data = AddTask(text, Anchor(commentId, "COMMENT"))
    post[PrTask](postUrl, Some(data.toJson.toString))
  }

  def updateTask(project: String, repo: String, prNumber: Int, state: String, taskId: Int): Int = {
    val version = getBlockerComment(project, repo, prNumber, taskId).version
    val updateUrl = apiPrBlockerCommentUpdateUrl(project, repo, prNumber, taskId)

    val data = UpdateTaskState(state, version)
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

  /** @return size of the given file in bytes */
  def getFileSizeByFilePath(project: String, repo: String, filePath: String, commitId: String): Int =
    handleErrors { get[CommitFileSize](apiFileSizeByFilePathUrl(project, repo, filePath, commitId)).size }

  def getAllFileSizes(project: String, repo: String, commitId: CommitId): Seq[CommitFilePathSize] = {
    val commitFiles = getChangedFilesByCommitId(project, repo, commitId.id).filter(_.`type` != "DELETE")
    commitFiles.flatMap { commitFile =>
      val path = commitFile.path
      val filePath = s"${path.parent}/${path.name}"
      val fileSize = getFileSizeByFilePath(project, repo, filePath, commitId.id)
      Seq(CommitFilePathSize(filePath, fileSize, commitId.id, commitId.displayId))
    }
  }

  def annotateCommit[A: JsonWriter](commit: String, payload: A): Unit = {
    val url = apiGetBuildStatus(commit)
    handleErrors { postWithoutResponse(url, Some(payload.toJson.toString)) }
  }

  def getAnnotations(commit: String): Seq[BuildStatus] =
    queryPaged[BuildStatus, BuildStatuses](apiGetBuildStatus(commit))

  /** Wraps generic UnsuccessfulRestCall responses in sub-classes of [[BitBucketException]]. */
  private def handleErrors[A](code: => A): A =
    try code
    catch {
      case e: UnsuccessfulRestCall =>
        import spray.json._
        e.rawPayload.flatMap(_.parseJson.convertTo[BitBucketErrors].errors.headOption) match {
          case Some(err) if err.exceptionName == "com.atlassian.bitbucket.pull.NoSuchPullRequestException" =>
            throw NoSuchPullRequestException(err, e)
          case Some(err) if err.exceptionName == "com.atlassian.bitbucket.project.NoSuchProjectException" =>
            throw NoSuchProjectException(err, e)
          case Some(err) if err.exceptionName == "com.atlassian.bitbucket.content.NoSuchPathException" =>
            throw NoSuchPathException(err, e)
          case Some(err) if err.exceptionName == "com.atlassian.bitbucket.AuthorisationException" =>
            throw AuthorisationException(err, e)
          case Some(other) =>
            throw UncategorizedBitBucketException(other, e)
          case None =>
            throw e
        }
    }

  def fetchFilesFromBitBucket(
      project: String,
      repo: String,
      dirToList: String,
      branchOption: Option[String], // if none, branch will be set to default branch
      maxExpectedData: Option[Int] = None,
      expectedPageSize: Int = 10000 // expected page size in a recursive call
  ): Seq[String] = {
    queryPaged[String, RequestingFilesFromBitBucket](
      apiListFilesUrl(project, repo, dirToList, branchOption),
      maxExpectedData,
      expectedPageSize)
  }

  def getFileContent(
      project: String,
      repo: String,
      browsePath: String,
      branchOption: Option[String], // if none, branch will be set to default branch
      maxExpectedData: Option[Int] = None,
      expectedPageSize: Int = 10000 // expected page size in a recursive call
  ): Seq[FileContentLine] = {
    queryPaged[FileContentLine, FileContent](
      apiBrowseUrl(project, repo, browsePath, branchOption),
      maxExpectedData,
      expectedPageSize)
  }

  def getPrChanges(project: String, repo: String, prNumber: Long): Seq[PrChangesValuesField] =
    queryPaged[PrChangesValuesField, PrChangesDetails](apiPrChangeDetails(project, repo, prNumber))
}

sealed trait BitBucketException {
  def bitBucketError: BitBucketError
}

final case class UncategorizedBitBucketException(bitBucketError: BitBucketError, e: Throwable)
    extends RuntimeException(bitBucketError.message, e)
    with BitBucketException

final case class NoSuchPullRequestException(bitBucketError: BitBucketError, e: Throwable)
    extends RuntimeException(bitBucketError.message, e)
    with BitBucketException

final case class NoSuchProjectException(bitBucketError: BitBucketError, e: Throwable)
    extends RuntimeException(bitBucketError.message, e)
    with BitBucketException

final case class NoSuchPathException(bitBucketError: BitBucketError, e: Throwable)
    extends RuntimeException(bitBucketError.message, e)
    with BitBucketException

final case class AuthorisationException(bitBucketError: BitBucketError, e: Throwable)
    extends RuntimeException(bitBucketError.message, e)
    with BitBucketException
