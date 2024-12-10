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
package optimus.stratosphere.http.client

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.settings.ConnectionPoolSettings
/* import optimus.security.akka.http.client.SimpleNoAuthenticationHttpClient
import optimus.security.akka.http.common.AkkaConfigurations */
import optimus.stratosphere.config.StratoWorkspaceCommon

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object HttpClientFactory {
  // In order to speak with the outside world, we need an instance of StratoWorkspace
  // But we do not need to keep creating factories (and its associated actor system)
  private val factory = new AtomicReference[HttpClientFactoryApi]()

  def factory(stratoWorkspace: StratoWorkspaceCommon): HttpClientFactoryApi = synchronized {
    def createFactory(clzName: String): HttpClientFactoryApi =
      Try(
        Class
          .forName(clzName)
          .getDeclaredConstructor(classOf[StratoWorkspaceCommon])
          .newInstance(stratoWorkspace)
          .asInstanceOf[HttpClientFactoryApi]) match {
        case Failure(exception) =>
          stratoWorkspace.log.error(s"Could not instantiate the http client factory: $exception", exception)
          new DefaultHttpClientFactory(stratoWorkspace)
        case Success(factory) =>
          factory
      }

    Option(factory.get()).getOrElse {
      val newFactory =
        stratoWorkspace.internal.factories.restClient match {
          case Some(clzName) => createFactory(clzName)
          // No configuration defined
          case None => new DefaultHttpClientFactory(stratoWorkspace)
        }
      factory.set(newFactory)
      newFactory
    }
  }
}

trait HttpClientFactoryApi {
  def createClient(
      rootUri: Uri,
      // Describe target system (e.g. jira instance); will be in crumb; httpRequest contains resolved target host, which does not say "what" it is.
      targetSystemType: String,
      timeout: Duration,
      sendCrumbs: Boolean = false
  ): RestClient
}

class SimpleNoAuthenticationRestClient(
    protected val stratoWorkspace: StratoWorkspaceCommon,
    rootUri: Uri,
    jTimeout: Duration
)(protected implicit val actorSystem: ActorSystem)
    extends RestClient {
  // private val innerClient = new SimpleNoAuthenticationHttpClient(rootUri)
  protected val timeout: FiniteDuration = FiniteDuration.apply(jTimeout.toMillis, TimeUnit.MILLISECONDS)

  def hostName: String = rootUri.authority.host.toString()

  override def request(
      pathQueryFragmentOrUri: String,
      modifier: HttpRequest => HttpRequest,
      settingsOverride: Option[ConnectionPoolSettings]): Future[HttpResponse] = ???
    // innerClient.request(pathQueryFragmentOrUri, modifier, settingsOverride)
}

class DefaultHttpClientFactory(stratoWorkspace: StratoWorkspaceCommon) extends HttpClientFactoryApi {
  // The class loader matters because of the plugin architecture of IntelliJ!
  /*  implicit val actorSystem: ActorSystem =
    ActorSystem("http-clients", AkkaConfigurations.BasicClientConfiguration, getClass.getClassLoader) */

  stratoWorkspace.log.debug("Using DefaultHttpClientFactory")

  def createClient(rootUri: Uri, targetSystemType: String, timeout: Duration, sendCrumbs: Boolean = false): RestClient = ???
    // new SimpleNoAuthenticationRestClient(stratoWorkspace, rootUri, timeout)
}
