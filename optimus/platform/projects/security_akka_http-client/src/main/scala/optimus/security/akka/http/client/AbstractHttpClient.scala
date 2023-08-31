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
package optimus.security.akka.http.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.HttpExt
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.settings.ClientConnectionSettings

import scala.collection.immutable
import scala.concurrent.Future

// Should be extended to customize to your needs
abstract class AbstractHttpClient(rootUri: Uri)(implicit actorSystem: ActorSystem) {
  require(rootUri.isAbsolute, "Cannot be a relative URI")
  require(rootUri.authority.nonEmpty, "Requires authority: check if you forgot to specify the scheme (http or https)")

  // Should be extended to customize the connection contexts (e.g. transport layer security)
  protected def configure(client: HttpExt): Unit = {} // Plain vanilla by default
  private val client = {
    val newClient = Http()
    configure(newClient)
    newClient
  }

  protected def isLocalHost(hostAddress: String): Boolean =
    hostAddress.startsWith("127.0.0.1") || hostAddress.startsWith("localhost")

  protected def addAuthenticationHeaders(uri: Uri, httpHeaders: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader]

  lazy val defaultConnectionPoolSettings: ConnectionPoolSettings =
    ConnectionPoolSettings(actorSystem.settings.config)

  def withConnectionSettings(
      connectionSettingsFct: ClientConnectionSettings => ClientConnectionSettings): ConnectionPoolSettings =
    defaultConnectionPoolSettings.withConnectionSettings(
      connectionSettingsFct(defaultConnectionPoolSettings.connectionSettings))

  // Can be extended to instrument (e.g. telemetry)
  protected def singleRequest(
      httpRequest: HttpRequest,
      settingsOverride: Option[ConnectionPoolSettings]): Future[HttpResponse] = {
    val requestWithNewHeaders =
      httpRequest.copy(headers = addAuthenticationHeaders(httpRequest.uri, httpRequest.headers))
    val response =
      settingsOverride
        .map(settings => client.singleRequest(requestWithNewHeaders, settings = settings))
        .getOrElse(client.singleRequest(requestWithNewHeaders))

    client.superPool()
    response
  }

  // noop as default implementation; allows for logging
  protected def inspect(rootUri: Uri, pathQueryFragmentOrUri: String, effectiveUri: Uri): Unit = {}

  // Version that allows specifying headers, verb and more
  def request(
      pathQueryFragmentOrUri: String, // You do not need the specify the schema and authority as it is already provided in rootUri
      modifier: HttpRequest => HttpRequest = identity,
      settingsOverride: Option[ConnectionPoolSettings] = None): Future[HttpResponse] = {
    val uri: Uri = {
      val asUri = Uri(pathQueryFragmentOrUri)
      if (asUri.isRelative) asUri.withAuthority(rootUri.authority).withScheme(rootUri.scheme) else asUri
    }
    inspect(rootUri, pathQueryFragmentOrUri, uri)
    singleRequest(modifier(HttpRequest.apply(uri = uri)), settingsOverride)
  }
}
