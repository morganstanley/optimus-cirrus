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
package optimus.stratosphere.bitbucket

import akka.http.scaladsl.model.Uri
import com.typesafe.config.Config
import com.typesafe.config.ConfigRenderOptions
import optimus.stratosphere.bitbucket.PulRequestJsonProtocol._
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.http.client.HttpClientFactory
import optimus.stratosphere.utils.RemoteSpec
import optimus.stratosphere.utils.RemoteUrl
import spray.json._

import java.time.Instant
import java.time.{Duration => JDuration}
import scala.annotation.tailrec
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex

object BitbucketApiRestClient {
  private val privateForkOwnerExtractor: Regex = s".+scm/~(\\w+)/.+".r
  private val remoteUrlExtractor: Regex = s"(\\w+://)(\\w+)@(.*)".r

  def repoOwner(url: RemoteUrl): Option[String] = url.url match {
    case privateForkOwnerExtractor(userName) => Some(userName)
    case _                                   => None
  }

  def stripUserInUrl(url: String): String = url match {
    case remoteUrlExtractor(protocol, _, domainAndPath) => s"$protocol$domainAndPath"
    case _                                              => url
  }

  def slug(remoteUrl: RemoteUrl): String = {
    remoteUrl.url.split("/").reverse.head.split("\\.").head
  }
}

final case class Comment(text: String)
final case class CreatePrivateFork(name: String)
final case class SetAccess(enabled: Boolean)
final case class SetForkSyncing(enabled: Boolean)
final case class SetName(name: String)
final case class SynchronizeFork(refId: String, action: String)

trait BitbucketApiRestClientProtocol extends DefaultJsonProtocol {
  implicit val commentFormat: JsonFormat[Comment] = jsonFormat1(Comment.apply)
  implicit val createPrivateForkFormat: JsonFormat[CreatePrivateFork] = jsonFormat1(CreatePrivateFork.apply)
  implicit val setAccessFormat: JsonFormat[SetAccess] = jsonFormat1(SetAccess.apply)
  implicit val setForkSyncingFormat: JsonFormat[SetForkSyncing] = jsonFormat1(SetForkSyncing.apply)
  implicit val setNameFormat: JsonFormat[SetName] = jsonFormat1(SetName.apply)
  implicit val synchronizeForkFormat: JsonFormat[SynchronizeFork] = jsonFormat2(SynchronizeFork.apply)
}

final class BitbucketApiRestClient(workspace: StratoWorkspaceCommon)(
    val timeout: JDuration,
    val bitbucketHost: String = workspace.internal.bitbucketHostname)
    extends BitbucketApiRestClientProtocol {

  private val version = "1.0"
  private val defaultApiName = "api"
  // BitBucket is set to a default of 25 when not specified
  // We raise it to 1,000 for forks in particular, as to minimize a large number of calls for BitBucket.
  private val defaultLimit = 1000

  private val httpClient = HttpClientFactory
    .factory(workspace)
    .createClient(Uri(s"http://$bitbucketHost"), "BitBucket", timeout.toMillis milliseconds, sendCrumbs = false)

  private def getValues(command: String, urlParameters: (String, Any)*): Seq[Config] = {
    pagedGet(command, _ => true, shouldReturnFirstFound = false, urlParameters: _*)
  }

  private def findValue(command: String)(predicate: Config => Boolean): Option[Config] = {
    pagedGet(command, predicate, shouldReturnFirstFound = true).headOption
  }

  def allForks(project: String, repository: String): Seq[Repository] = {
    getValues(s"projects/$project/repos/$repository/forks").map { config =>
      Repository(project = Project(config.getConfig("project").getString("key")), slug = config.getString("slug"))
    }
  }

  private[bitbucket] def allProjects(): Seq[String] = {
    getValues("projects/").map(_.getString("key"))
  }

  def allRepositories(project: String): Seq[String] = {
    getValues(s"projects/$project/repos/").map(_.getString("name"))
  }

  def allPrivateRepositoriesOfUser(userName: String): Seq[RemoteSpec] =
    getValues(s"projects/~$userName/repos/")
      .filter(config => config.getString("project.type") == "PERSONAL") // all should be personal but it's for our tests
      .map(configToRemote)

  def allPullRequestsIds(project: String, repository: String): Seq[Long] = {
    getValues(s"projects/$project/repos/$repository/pull-requests").map(_.getLong("id"))
  }

  def allPullRequests(project: String, repository: String): Seq[Config] = {
    getValues(s"projects/$project/repos/$repository/pull-requests")
  }

  def allOpenPullRequestsOfCurrentUser(): Seq[Config] =
    getValues("dashboard/pull-requests", "state" -> "OPEN", "role" -> "AUTHOR")

  def allDeclinedPullRequestsOfCurrentUser(): Seq[Config] =
    getValues("dashboard/pull-requests", "state" -> "DECLINED", "role" -> "AUTHOR")

  def lastPullRequestActivity(project: String, repository: String, pullRequestId: Long): Long = {
    val config = findValue(s"projects/$project/repos/$repository/pull-requests/$pullRequestId/activities")(_ => true)
    val rootDate = config.map(
      _.getLong("createdDate")
    ) // All activity has a createdDate but everything else we look for is optional.
    val children = config.filter(_.hasPath("comment")).map(_.getConfig("comment")).map(childActivity)
    children match {
      case Some(c) => (rootDate :: c).flatten.max
      case None    => rootDate.get
    }
  }

  private def childActivity(config: Config): List[Option[Long]] = {
    val created = Some(config.getLong("createdDate"))
    val updated = if (config.hasPath("updatedDate")) Some(config.getLong("updatedDate")) else None

    val hasComments = config.hasPath("comments")
    val hasTasks = config.hasPath("tasks")

    def traverseValidChild(t: => Boolean, key: String): List[Option[Long]] =
      if (t) config.getConfigList(key).asScala.toList.flatMap(childActivity) else Nil

    List(created, updated) ++ traverseValidChild(hasComments, "comments") ++ traverseValidChild(hasTasks, "tasks")
  }

  def pullRequestActivities(project: String, repository: String, pullRequestId: Long): Seq[Config] =
    getValues(s"projects/$project/repos/$repository/pull-requests/$pullRequestId/activities")

  def declinePullRequest(pullRequest: Config): Unit = {
    httpClient.post(
      prepareUrl(
        getRelativeLinkUrl(pullRequest) + "/decline",
        defaultApiName,
        "version" -> pullRequest.getString("version")
      )
    )
  }

  def addPullRequestComment(pullRequest: Config, comment: String): Unit =
    httpClient.post(
      prepareUrl(
        getRelativeLinkUrl(pullRequest) + "/comments",
        defaultApiName
      ),
      Comment(comment)
    )

  def getPrivateRepoOrigin(
      userName: String,
      repository: String
  ): Repository = {
    val requestUrl = prepareUrl(s"projects/~${userName.toUpperCase()}/repos/$repository", defaultApiName)
    val repoConfig = httpClient.get(requestUrl).getConfig("origin")
    val repoJson = repoConfig.root().render(ConfigRenderOptions.concise)
    repoJson.parseJson.convertTo[Repository]
  }

  def createPullRequest(
      title: String,
      description: String,
      fromRef: Ref,
      toRef: Ref,
      reviewers: Seq[String]
  ): Config = {
    val pullRequest = PullRequest(
      title = title,
      description = description,
      state = "OPEN",
      open = true,
      closed = false,
      fromRef = fromRef,
      toRef = toRef,
      reviewers = if (reviewers.nonEmpty) Some(reviewers.map(name => Reviewer(User(name)))) else None
    )

    val url = prepareUrl(
      s"projects/${toRef.repository.project.key}/repos/${toRef.repository.slug}/pull-requests",
      defaultApiName)

    httpClient.post(url, pullRequest)
  }

  def pullRequest(project: String, repository: String, pullRequestId: Long): Config = {
    httpClient.get(prepareUrl(s"projects/$project/repos/$repository/pull-requests/$pullRequestId", defaultApiName))
  }

  def pullRequestUrl(pullRequest: Config): String = {
    s"http://$bitbucketHost/atlassian-stash/" + getRelativeLinkUrl(pullRequest)
  }

  def selectedProjects(projectsRegexps: Seq[String]): Seq[String] = {
    for {
      project <- allProjects()
      regex <- projectsRegexps
      if regex.r.pattern.matcher(project).matches()
    } yield project
  }

  def getPrivateForksOfUser(userName: String): Seq[RemoteSpec] = allPrivateRepositoriesOfUser(userName).filter(_.isFork)

  def createPrivateFork(projectKey: String, repo: String, forkName: String): RemoteUrl = {
    val forkUrl = prepareUrl(s"projects/$projectKey/repos/$repo", defaultApiName)
    val cloneUrl = httpClient
      .post(forkUrl, CreatePrivateFork(forkName))
      .getConfigList("links.clone")
      .asScala
      .find(_.getString("name") == "http")
      .get
      .getString("href")

    convertRepoUrlToProxy(cloneUrl)
  }

  def getForkSyncStatus(projectSlug: String, repository: String, assumeUserName: Boolean = true): ForkSyncStatus = {
    val project = if (assumeUserName) s"~$projectSlug" else projectSlug
    val forkSyncingUrl = prepareUrl(s"projects/$project/repos/$repository", "sync")

    val config = httpClient.get(forkSyncingUrl)

    val syncAvailable = config.getBoolean("available")
    val syncEnabled = config.getBoolean("enabled")

    val maybeLastSync =
      if (syncEnabled) Some(Instant.ofEpochMilli(config.getLong("lastSync")))
      else None

    val divergedBranches = {
      if (config.hasPath("divergedRefs")) {
        config
          .getConfigList("divergedRefs")
          .asScala
          .to(Seq)
          .filter(entry => entry.hasPath("id"))
          .filter(_.getString("state").toUpperCase == "DIVERGED")
          .map(_.getString("displayId"))
      } else {
        Seq.empty
      }
    }

    ForkSyncStatus(
      Repository(Project(project), repository),
      available = syncAvailable,
      enabled = syncEnabled,
      lastSync = maybeLastSync,
      divergedBranches = divergedBranches
    )
  }

  def updatePrivateForkName(userName: String, repository: String, name: String): Config = {
    val url = prepareUrl(s"projects/~$userName/repos/$repository", defaultApiName)
    httpClient.put(url, SetName(name))
  }

  def setForkSyncing(userName: String, repository: String, enabled: Boolean = true): Config = {
    val enableForkSyncingUrl = prepareUrl(s"projects/~$userName/repos/$repository", "sync")
    httpClient.post(enableForkSyncingUrl, SetForkSyncing(enabled))
  }

  def setAccess(userName: String, repository: String, makePublic: Boolean): Unit = {
    val setPublicAccessUrl = prepareUrl(s"projects/~$userName/repos/$repository", defaultApiName)
    httpClient.put(setPublicAccessUrl, SetAccess(makePublic))
  }

  def grantWriteAccessToSelf(userName: String, repository: String): Unit = {
    val grantWriteAccessUrl = prepareUrl(
      s"projects/~$userName/repos/$repository/permissions/users",
      defaultApiName,
      "permission" -> "REPO_WRITE",
      "name" -> userName)
    httpClient.put(grantWriteAccessUrl)
  }

  def forceForkSyncWithDiscard(userName: String, repository: String, ref: String): Config = {
    val forkSyncUrl = prepareUrl(s"projects/~$userName/repos/$repository/synchronize", "sync")
    httpClient.post(forkSyncUrl, SynchronizeFork(ref, "DISCARD"))
  }

  def getRemoteBranchDetails(userName: String, repository: String, branch: String): Config = {
    val getBranchDetailsUrl =
      prepareUrl(s"projects/~$userName/repos/$repository/branches", defaultApiName, "filterText" -> branch)
    httpClient.get(getBranchDetailsUrl)
  }

  def getRepoHookStatus(userName: String, repository: String, hookKey: String): Config = {
    val url = prepareUrl(s"projects/~$userName/repos/$repository/settings/hooks/$hookKey", defaultApiName)
    httpClient.get(url)
  }

  def getRepoHookSettings(userName: String, repository: String, hookKey: String): Config = {
    val url = prepareUrl(s"projects/~$userName/repos/$repository/settings/hooks/$hookKey/settings", defaultApiName)
    httpClient.get(url)
  }

  def enableRepoHook(userName: String, repository: String, hookKey: String): Unit = {
    val url = prepareUrl(s"projects/~$userName/repos/$repository/settings/hooks/$hookKey/enabled", defaultApiName)
    httpClient.put(url)
  }

  private[bitbucket] def getRelativeLinkUrl(pullRequest: Config): String = {
    // example href:
    // http://stash.company.com/atlassian-stash/projects/PROJECT_NAME/repos/REPO_NAME/pull-requests/42
    pullRequest.getConfigList("links.self").get(0).getString("href").split('/').drop(4).mkString("/")
  }

  private def pagedGet(
      command: String,
      predicate: Config => Boolean,
      shouldReturnFirstFound: Boolean,
      urlParameters: (String, Any)*): Seq[Config] = {
    @tailrec
    def pagedGet(
        start: Int,
        command: String,
        predicate: Config => Boolean,
        shouldReturnFirstFound: Boolean,
        acc: Seq[Config]): Seq[Config] = {
      val url = prepareUrl(command, defaultApiName, urlParameters ++ Seq("limit" -> defaultLimit, "start" -> start): _*)
      val result = httpClient.get(url)
      val values = result.getObjectList("values").asScala.to(Seq).map(_.toConfig).filter(predicate)

      if (values.nonEmpty && shouldReturnFirstFound) return values.headOption.to(Seq)

      if (result.getBoolean("isLastPage")) values ++ acc
      else pagedGet(result.getInt("nextPageStart"), command, predicate, shouldReturnFirstFound, values ++ acc)
    }

    pagedGet(0, command, predicate, shouldReturnFirstFound, Seq())
  }

  private def convertRepoUrlToProxy(url: String) = {
    RemoteUrl(url.replaceAll("@.*/atlassian-stash/", s"@$bitbucketHost/atlassian-stash/"))
  }

  private def prepareUrl(command: String, api: String, urlParameters: (String, Any)*): String = {
    val preparedUrlParameters = urlParameters.map { case (name, value) => s"$name=$value" }.mkString("&")
    s"http://$bitbucketHost/atlassian-stash/rest/$api/$version/$command?$preparedUrlParameters"
  }

  private def configToRemote(config: Config): RemoteSpec =
    RemoteSpec(
      config.getString("name"),
      convertRepoUrlToProxy(config.getConfigList("links.clone").get(0).getString("href")),
      Try(convertRepoUrlToProxy(config.getConfigList("origin.links.clone").get(0).getString("href"))).toOption
    )
}
