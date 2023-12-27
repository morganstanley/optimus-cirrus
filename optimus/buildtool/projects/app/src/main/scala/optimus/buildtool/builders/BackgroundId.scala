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
package optimus.buildtool.builders

import optimus.buildtool.app.OptimusBuildToolBootstrap
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset

sealed trait BackgroundId {
  def toPrettyString: String
  def description: String
  def logFile(logDir: Directory): FileAsset

  override def toString: String = toPrettyString
  protected def timestamp: String = OptimusBuildToolBootstrap.generateLogFilePrefix()
}

final case class BackgroundAppId(scopeId: ScopeId, appName: String) extends BackgroundId {
  override val toPrettyString: String = s"background:$scopeId:$appName"
  override val description: String = s"App $appName from $scopeId"

  override def logFile(logDir: Directory): FileAsset =
    logDir.resolveFile(s"$timestamp.$scopeId.$appName.log")
}

final case class BackgroundCmdId(id: String) extends BackgroundId {
  override val toPrettyString: String = id
  override val description: String = s"Background command $id"

  override def logFile(logDir: Directory): FileAsset =
    logDir.resolveFile(s"$timestamp.$id.log")
}

final case class OpenApiCmdId(scopeId: ScopeId, appName: String) extends BackgroundId {
  override val toPrettyString: String = s"$scopeId:openapi:$appName"
  override val description: String = s"OpenAPI integration for app $appName"

  override def logFile(logDir: Directory): FileAsset =
    logDir.resolveFile(s"$timestamp.$scopeId.openapi.$appName.log")
}
