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

import java.time.Instant
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config.ConfigFactory
import optimus.rest.json.InstantJsonFormat
import spray.json._

import scala.collection.immutable.Seq
import scala.util.matching.Regex

final case class PagedPrDataRequest(isLastPage: Boolean, values: Seq[BasePrData], nextPageStart: Option[Int])
    extends Paged[BasePrData]
object PagedPrDataRequest extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[PagedPrDataRequest] = jsonFormat3(PagedPrDataRequest.apply)
}

final case class BasePrData(
    id: Int,
    version: Int,
    title: String,
    state: String,
    description: Option[String],
    fromRef: Ref,
    toRef: Ref,
    author: Author,
    links: Links,
    closed: Boolean,
    locked: Boolean,
    createdDate: Instant,
    updatedDate: Instant,
    reviewers: Seq[Reviewer]
) {
  private def prWIPComment: String = "[COMMENT]: # (WIP)"
  private def prLockedComment: String = """[\s\S]*\[COMMENT\]: # \(DO NOT MERGE #\d*\)[\s\S]*"""

  // These are general guidelines for what a WIP pr might be. The only official way is '[COMMENT]: # (WIP)' in the description
  private def descriptionLikelyWipComments: Seq[String] = Seq("@overlord ignore")
  private def titleLikelyWipComments: Seq[String] = Seq("WIP", "DO NOT MERGE")

  def withInstance(instance: String): PrData =
    PrData(
      id,
      version,
      title,
      state,
      description,
      fromRef,
      toRef,
      author,
      links,
      closed,
      locked,
      createdDate,
      updatedDate,
      reviewers,
      instance
    )

  def isWip: Boolean =
    description.exists { d =>
      d.toUpperCase.contains(prWIPComment) ||
      d.toUpperCase.matches(prLockedComment)
    }

  def isLikelyWip: Boolean = checkStringForAllowList(title, titleLikelyWipComments) || description.exists(d =>
    checkStringForAllowList(d, descriptionLikelyWipComments))

  private def checkStringForAllowList(str: String, allowList: Seq[String]): Boolean = {
    allowList.exists { flaggedString =>
      val startIndex = str.toLowerCase.indexOf(flaggedString.toLowerCase)
      val endIndex = startIndex + flaggedString.length
      if (startIndex >= 0) {
        val isBeginningSafe = (startIndex == 0) || !str.charAt(startIndex - 1).isLetter
        val isEndSafe = (endIndex <= str.length) && ((endIndex == str.length) || !str.charAt(endIndex).isLetter)
        isBeginningSafe && isEndSafe
      } else
        false
    }
  }
}
object BasePrData extends DefaultJsonProtocol with NullOptions with InstantJsonFormat {
  implicit lazy val format: RootJsonFormat[BasePrData] = jsonFormat14(BasePrData.apply)
}

// BasePrData with instance baked-in, so we can use it in URL replacement
final case class PrData(
    id: Int,
    version: Int,
    title: String,
    state: String,
    description: Option[String],
    fromRef: Ref,
    toRef: Ref,
    author: Author,
    links: Links,
    closed: Boolean,
    locked: Boolean,
    createdDate: Instant,
    updatedDate: Instant,
    reviewers: Seq[Reviewer],
    instance: String
) {
  def url: String = {
    // spnego API resolves the hostname, so this is a heavy-handed way to undo the resolution
    val domain = ConfigFactory.parseResources("internal.conf").getString("bitbucket-domain")
    val urlPattern = s"http://(.+).$domain"
    links.self.head.href.replaceFirst(urlPattern, s"http://$instance")
  }

  def sourceBranch: String = fromRef.displayId
  def targetBranch: String = toRef.displayId
}

/**
 * Base class for all the paged results. Allows usage of [[BitBucket.queryPaged()]].
 *
 * See paged APIs: https://docs.atlassian.com/bitbucket-server/rest/7.2.3/bitbucket-rest.html#paging-params
 */
trait Paged[A] {
  def isLastPage: Boolean
  def values: Seq[A]
  def nextPageStart: Option[Int]
}

final case class Ref(id: String, displayId: String, repository: Repository, latestCommit: String)
object Ref extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Ref] = jsonFormat4(Ref.apply)
}

final case class Repository(name: String, project: Project)
object Repository extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Repository] = jsonFormat2(Repository.apply)
}

final case class Project(key: String, name: String)
object Project extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Project] = jsonFormat2(Project.apply)
}

final case class Links(self: Seq[Self])
object Links extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Links] = jsonFormat1(Links.apply)
}

final case class Self(href: String)
object Self extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Self] = jsonFormat1(Self.apply)
}

final case class Author(user: User)
object Author extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Author] = jsonFormat1(Author.apply)
}

final case class User(name: String, emailAddress: Option[String], displayName: String)
object User extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[User] = jsonFormat3(User.apply)
}

final case class Reviewer(approved: Boolean, status: String, user: User) {
  def markedNeedsWork: Boolean = status == ReviewerStatus.NeedsWork.toString
}
object Reviewer extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Reviewer] = jsonFormat3(Reviewer.apply)
}

final case class ActivityRequest(isLastPage: Boolean, values: Seq[PrActivity], nextPageStart: Option[Int])
    extends Paged[PrActivity]
object ActivityRequest extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[ActivityRequest] = jsonFormat3(ActivityRequest.apply)
}

final case class PrActivity(action: String, comment: Option[PrComment], createdDate: Instant)
object PrActivity extends DefaultJsonProtocol with NullOptions with InstantJsonFormat {
  implicit lazy val format: RootJsonFormat[PrActivity] = jsonFormat3(PrActivity.apply)
}

trait PrTextActivity {
  val state: String
  def isOpen: Boolean = state == TaskState.Open.toString
}

final case class PrTask(id: Int, text: String, state: String, createdDate: Instant) extends PrTextActivity
object PrTask extends DefaultJsonProtocol with InstantJsonFormat {
  implicit lazy val format: RootJsonFormat[PrTask] = jsonFormat4(PrTask.apply)
}

final case class PrBlockerComment(id: Int, text: String, state: String, version: Int)
object PrBlockerComment extends DefaultJsonProtocol with InstantJsonFormat {
  implicit lazy val format: RootJsonFormat[PrBlockerComment] = jsonFormat4(PrBlockerComment.apply)
}

final case class CommitActivityRequest(isLastPage: Boolean, values: Seq[CommitId], nextPageStart: Option[Int])
    extends Paged[CommitId]
object CommitActivityRequest extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[CommitActivityRequest] = jsonFormat3(CommitActivityRequest.apply)
}

final case class CommitId(id: String, displayId: String)
object CommitId extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[CommitId] = jsonFormat2(CommitId.apply)
}

final case class CommitRequest(isLastPage: Boolean, values: Seq[CommitFile], nextPageStart: Option[Int])
    extends Paged[CommitFile]
object CommitRequest extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[CommitRequest] = jsonFormat3(CommitRequest.apply)
}

final case class CommitFilePath(parent: String, name: String)
object CommitFilePath extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[CommitFilePath] = jsonFormat2(CommitFilePath.apply)
}

final case class CommitFile(path: CommitFilePath, `type`: String)
object CommitFile extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[CommitFile] = jsonFormat2(CommitFile.apply)
}

final case class CommitFileSize(size: Int)
object CommitFileSize extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[CommitFileSize] = jsonFormat1(CommitFileSize.apply)
}

final case class PrComment(
    id: Int,
    text: String,
    version: Int,
    author: User,
    createdDate: Instant,
    updatedDate: Instant, // PrComment always includes updatedDate, if it has never been updated it is equal to the created date
    state: String,
    severity: String,
    comments: Seq[PrComment],
    resolvedDate: Option[Instant],
    resolver: Option[User],
    parentCommentId: Option[Int],
    threadResolved: Option[Boolean] = None
) extends PrTextActivity {
  import PrComment._

  def resolvedBy: Option[String] = resolver.map(_.name)

  def isTask: Boolean = severity == CommentSeverity.Blocker.toString
  def isRestricted: Boolean = text.startsWith(restrictedPrefix)
  def isThreadResolved: Boolean = threadResolved.getOrElse(false)

  def tasks: Seq[PrTask] = comments.collect {
    case taskComment: PrComment if taskComment.isTask =>
      PrTask(taskComment.id, taskComment.text, taskComment.state, taskComment.createdDate)
  }

  def textComments: Set[String] = text.split("\n").filter(_.startsWith("[comment]: # ")).toSet

  def getCommentByKeyPieces(keyPieces: Seq[String], seperator: String = ":"): Option[String] = {
    textComments
      .find(_.matches(s"\\[comment\\]: # \\(${keyPieces.mkString(seperator)}$seperator[$allowedCharStr]*\\)"))
      .flatMap(c => PrComment.allowedCharCommentRegex.findFirstIn(c))
      .map(_.replaceAll(s"[^$allowedCharStr]", ""))
  }
}

object PrComment extends DefaultJsonProtocol with NullOptions with InstantJsonFormat {
  implicit lazy val format: JsonFormat[PrComment] = lazyFormat(jsonFormat13(PrComment.apply))

  val restrictedPrefix: String = "[Restricted] "
  val mandatoryPrefix: String = "[Mandatory] "
  val optionalPrefix: String = "[Optional] "
  val allowedCharStr: String = "\\w._-"
  val allowedCharRegex: Regex = s"[$allowedCharStr]*".r
  val allowedCharCommentRegex: Regex = s":[$allowedCharStr]*\\)".r

  def appendParentId(comment: PrComment, commentId: Int): PrComment = {
    PrComment(
      comment.id,
      comment.text,
      comment.version,
      comment.author,
      comment.createdDate,
      comment.updatedDate,
      comment.state,
      comment.severity,
      comment.comments,
      comment.resolvedDate,
      comment.resolver,
      Some(commentId)
    )
  }

  def getChildrenCommentsAndTasks(comment: PrComment): Seq[PrComment] = {
    if (comment.comments.nonEmpty) {
      val childComments = comment.comments.flatMap(getChildrenCommentsAndTasks)
      Seq(comment) ++ childComments.map(c => appendParentId(c, comment.id))
    } else
      Seq(comment)
  }
}

sealed trait CommentUpdate { def id: Int }
final case class NewComment(id: Int) extends CommentUpdate
final case class UpdatedComment(id: Int) extends CommentUpdate
final case class NotUpdatedComment(id: Int) extends CommentUpdate

final case class Id(id: Int)
object Id extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Id] = jsonFormat1(Id.apply)
}

final case class AddComment(text: String, parent: Option[Parent])
object AddComment extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[AddComment] = jsonFormat2(AddComment.apply)
}

final case class Parent(id: String)
object Parent extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Parent] = jsonFormat1(Parent.apply)
}

final case class UpdateComment(text: String, version: Int)
object UpdateComment extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[UpdateComment] = jsonFormat2(UpdateComment.apply)
}

final case class UpdateTaskState(state: String, version: Int)
object UpdateTaskState extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[UpdateTaskState] = jsonFormat2(UpdateTaskState.apply)
}

final case class UpdateCommentThreadState(threadResolved: Boolean, version: Int)
object UpdateCommentThreadState extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[UpdateCommentThreadState] = jsonFormat2(UpdateCommentThreadState.apply)
}

final case class AddTask(text: String, parent: Anchor)
object AddTask extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[AddTask] = jsonFormat2(AddTask.apply)
}

final case class Anchor(id: Int, `type`: String)
object Anchor extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[Anchor] = jsonFormat2(Anchor.apply)
}

final case class CommitFilePathSize(filePath: String, sizeInBytes: Int, commitId: String, commitDisplayId: String)
object CommitFilePathSize extends DefaultJsonProtocol {}

final case class BrowsePath(components: Seq[String], parent: Option[String], name: String, extension: Option[String])
object BrowsePath extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[BrowsePath] = jsonFormat4(BrowsePath.apply)
}

final case class BrowseFile(
    path: BrowsePath,
    contentId: Option[String],
    node: Option[String],
    `type`: String,
    size: Option[Int])
object BrowseFile extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[BrowseFile] = jsonFormat5(BrowseFile.apply)
}

final case class BrowseChildren(values: Seq[BrowseFile])
object BrowseChildren extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[BrowseChildren] = jsonFormat1(BrowseChildren.apply)
}

final case class BrowseData(
    path: BrowsePath,
    revision: String,
    children: BrowseChildren
)
object BrowseData extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[BrowseData] = jsonFormat3(BrowseData.apply)
}

final case class BitBucketScmChangeRequest(eventKey: String, pullRequest: BasePrData, comment: Option[PrComment])
object BitBucketScmChangeRequest extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[BitBucketScmChangeRequest] = jsonFormat3(BitBucketScmChangeRequest.apply)
}

final case class BitBucketScmTestRequest(test: Boolean)
object BitBucketScmTestRequest extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[BitBucketScmTestRequest] = jsonFormat1(BitBucketScmTestRequest.apply)
}

final case class BitBucketErrors(errors: Seq[BitBucketError])
object BitBucketErrors extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[BitBucketErrors] = jsonFormat1(BitBucketErrors.apply)
}

final case class BitBucketError(context: Option[String], message: String, exceptionName: String)
object BitBucketError extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[BitBucketError] = jsonFormat3(BitBucketError.apply)
}

final case class BuildStatus(
    state: BitBucketBuildStates.StashBuildState,
    key: String,
    name: String,
    url: String,
    description: String,
    dateAdded: Long)
object BuildStatus extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[BuildStatus] = jsonFormat6(BuildStatus.apply)
}
final case class BuildStatuses(isLastPage: Boolean, values: Seq[BuildStatus], nextPageStart: Option[Int])
    extends Paged[BuildStatus]
object BuildStatuses extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[BuildStatuses] = jsonFormat3(BuildStatuses.apply)
}

object PRAction extends Enumeration {
  type PRAction = Value
  val Approved: PRAction = Value("APPROVED")
  val Commented: PRAction = Value("COMMENTED")
  val Declined: PRAction = Value("DECLINED")
  val Merged: PRAction = Value("MERGED")
  val Opened: PRAction = Value("OPENED")
  val Reopened: PRAction = Value("REOPENED")
  val Rescoped: PRAction = Value("RESCOPED")
  val Unapproved: PRAction = Value("UNAPPROVED")
  val Updated: PRAction = Value("UPDATED")
}

object TaskState extends Enumeration {
  type TaskState = Value
  val Open: TaskState = Value("OPEN")
  val Resolved: TaskState = Value("RESOLVED")
}

object CommentSeverity extends Enumeration {
  type CommentSeverity = Value
  val Normal: CommentSeverity = Value("NORMAL")
  val Blocker: CommentSeverity = Value("BLOCKER")
}

object EventKey extends Enumeration {
  type EventKey = Value
  val PrCommentCreated: EventKey = Value("pr:comment:added")
  val PrCommentUpdated: EventKey = Value("pr:comment:edited")
  val PrCommentDeleted: EventKey = Value("pr:comment:deleted")
  val PrDeclined: EventKey = Value("pr:declined")
  val PrDeleted: EventKey = Value("pr:deleted")
}

object ReviewerStatus extends Enumeration {
  type ReviewerStatus = Value
  val NeedsWork: ReviewerStatus = Value("NEEDS_WORK")
  val Approved: ReviewerStatus = Value("APPROVED")
  val Unapproved: ReviewerStatus = Value("UNAPPROVED")
}

final case class RequestingFilesFromBitBucket(isLastPage: Boolean, values: Seq[String], nextPageStart: Option[Int])
    extends Paged[String]
object RequestingFilesFromBitBucket extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[RequestingFilesFromBitBucket] =
    jsonFormat3(RequestingFilesFromBitBucket.apply)
}

final case class FileContent(isLastPage: Boolean, values: Seq[FileContentLine], nextPageStart: Option[Int])
    extends Paged[FileContentLine]
object FileContent extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[FileContent] =
    jsonFormat(FileContent.apply, "isLastPage", "lines", "nextPageStart")
}

final case class FileContentLine(text: String)
object FileContentLine extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[FileContentLine] =
    jsonFormat1(FileContentLine.apply)
}

final case class PrChangesDetails(isLastPage: Boolean, values: Seq[PrChangesValuesField], nextPageStart: Option[Int])
    extends Paged[PrChangesValuesField]
object PrChangesDetails extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[PrChangesDetails] = jsonFormat3(PrChangesDetails.apply)
}

final case class PrChangesValuesField(
    path: PathComponent,
    properties: GitChangeType,
    srcPath: Option[SourcePathComponent])
object PrChangesValuesField extends DefaultJsonProtocol with NullOptions {
  implicit lazy val format: RootJsonFormat[PrChangesValuesField] = jsonFormat3(PrChangesValuesField.apply)
}

final case class PathComponent(components: Seq[String])
object PathComponent extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[PathComponent] = jsonFormat1(PathComponent.apply)
}

final case class SourcePathComponent(components: Seq[String])
object SourcePathComponent extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[SourcePathComponent] = jsonFormat1(SourcePathComponent.apply)
}
final case class GitChangeType(gitChangeType: String)
object GitChangeType extends DefaultJsonProtocol {
  implicit lazy val format: RootJsonFormat[GitChangeType] = jsonFormat1(GitChangeType.apply)
}
