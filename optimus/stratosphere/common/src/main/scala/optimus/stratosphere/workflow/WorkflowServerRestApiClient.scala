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

package optimus.stratosphere.workflow
import akka.http.scaladsl.model.Uri
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.http.client.HttpClientFactory
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

final class WorkflowServerRestApiClient(workspace: StratoWorkspaceCommon)(
    timeout: Duration = 5 seconds,
    host: String = workspace.internal.urls.workflowServer.host,
    port: Int = workspace.internal.urls.workflowServer.port
) {

  private val httpClient = HttpClientFactory
    .factory(workspace)
    .createClient(Uri(s"http://$host:$port"), "workflow-server", timeout, sendCrumbs = false)

  def jiraInstanceProjectMapping(): Map[String, String] = {
    httpClient
      .get("/api/jira/instanceProjectMapping")
      .entrySet()
      .asScala
      .map(entry => entry.getKey -> entry.getValue.render())
      .toMap
  }
}
