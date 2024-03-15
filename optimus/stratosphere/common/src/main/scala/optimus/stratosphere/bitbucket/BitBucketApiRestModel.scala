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

import spray.json.DefaultJsonProtocol
import spray.json.JsonFormat

import java.time.Instant
import scala.collection.immutable.Seq

final case class ForkSyncStatus(
    repository: Repository,
    available: Boolean,
    enabled: Boolean,
    lastSync: Option[Instant],
    divergedBranches: Seq[String]
)

final case class Project(key: String)

final case class Repository(project: Project, slug: String)

final case class Ref(id: String, repository: Repository)

final case class User(name: String)

final case class Reviewer(user: User)

final case class PullRequest(
    title: String,
    description: String,
    state: String,
    open: Boolean,
    closed: Boolean,
    fromRef: Ref,
    toRef: Ref,
    reviewers: Option[Seq[Reviewer]]
)

object PulRequestJsonProtocol extends DefaultJsonProtocol {
  implicit lazy val projectFormat: JsonFormat[Project] = jsonFormat1(Project)
  implicit lazy val repositoryFormat: JsonFormat[Repository] = jsonFormat2(Repository)
  implicit lazy val refFormat: JsonFormat[Ref] = jsonFormat2(Ref)
  implicit lazy val userFormat: JsonFormat[User] = jsonFormat1(User)
  implicit lazy val reviewerFormat: JsonFormat[Reviewer] = jsonFormat1(Reviewer)
  implicit lazy val pullRequestFormat: JsonFormat[PullRequest] = jsonFormat8(PullRequest)
}
