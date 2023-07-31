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
package optimus.stratosphere.telemetry

import akka.http.scaladsl.model.Uri
import com.typesafe.config.ConfigRenderOptions
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.http.client.HttpClientFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object CrumbSender {
  def sendCrumb(crumb: Crumb)(implicit stratoWorkspace: StratoWorkspaceCommon): Future[Unit] = {
    val destination = Uri(stratoWorkspace.internal.telemetry.destination)
    stratoWorkspace.log.debug(s"Attempting to post crumb to: $destination...")

    val httpClient = HttpClientFactory
      .factory(stratoWorkspace)
      .createClient(
        destination,
        "Breadcrumbs",
        timeout = stratoWorkspace.internal.telemetry.timeout,
        sendCrumbs = false)

    httpClient
      .asyncPost(destination.path.toString(), crumb.asJson)
      .map { response =>
        stratoWorkspace.log.debug(s"Crumb relay response: '${response.root().render(ConfigRenderOptions.concise())}'")
      }
      .recover { case e =>
        stratoWorkspace.log.warning(s"Unable to post telemetry crumb (does not fail your command): $e")
      }
  }

}
