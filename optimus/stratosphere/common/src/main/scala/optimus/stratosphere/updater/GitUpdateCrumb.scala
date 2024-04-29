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
package optimus.stratosphere.updater

import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.telemetry.Crumb
import optimus.stratosphere.utils.EnvironmentUtils
import spray.json.DefaultJsonProtocol._
import spray.json._

private[updater] final case class GitUpdateCrumb(
    info: String,
    username: String,
    hostname: String,
    stratosphereVersion: String,
    gitVersion: Option[String],
    channel: Option[String],
) extends Crumb {
  override def producerType: String = "strato.git.update"
  override def asJson: String = GitUpdateCrumb.write(this).toString()
}

private[updater] object GitUpdateCrumb extends JsonWriter[GitUpdateCrumb] {

  def apply(info: String, workspace: StratoWorkspaceCommon): GitUpdateCrumb =
    new GitUpdateCrumb(
      info,
      EnvironmentUtils.userName,
      EnvironmentUtils.hostName,
      workspace.stratosphereVersion,
      GitUpdater.gitVersion(workspace).map(_.toString),
      workspace.stratosphereChannel
    )

  override def write(crumb: GitUpdateCrumb): JsValue =
    JsObject(
      Map(
        "producerType" -> JsString(crumb.producerType),
        "properties" -> Map[String, JsValue](
          "info" -> JsString(crumb.info),
          "username" -> JsString(crumb.username),
          "hostname" -> JsString(crumb.hostname),
          "stratosphereVersion" -> JsString(crumb.stratosphereVersion),
          "gitVersion" -> crumb.gitVersion.toJson,
          "channel" -> crumb.channel.toJson
        ).toJson
      )
    )
}
