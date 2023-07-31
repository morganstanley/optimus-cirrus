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
import akka.http.javadsl.model.ContentType
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpMethod
import akka.http.scaladsl.model.HttpMethods
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.settings.ConnectionPoolSettings
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import optimus.rest.RestApi
import optimus.stratosphere.config.StratoWorkspaceCommon
import optimus.stratosphere.stash.UnsuccessfulStashCall
import spray.json._

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.Future
import scala.xml._

trait RestClient extends RestApi {
  protected implicit def actorSystem: ActorSystem
  protected implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher
  protected def stratoWorkspace: StratoWorkspaceCommon
  protected def timeout: FiniteDuration

  def hostName: String

  private def debug(stmt: String): Unit = stratoWorkspace.log.debug(s"[${Thread.currentThread().getName}] $stmt")

  def request(
      pathQueryFragmentOrUri: String, // You do not need the specify the schema and authority as it is already provided in rootUri
      modifier: HttpRequest => HttpRequest = identity,
      settingsOverride: Option[ConnectionPoolSettings] = None): Future[HttpResponse]

  private def asyncSingleRequestWithConversion[T](
      url: String,
      method: HttpMethod,
      maybeRequestData: Option[String],
      convert: String => T,
      contentType: ContentType.NonBinary = ContentTypes.`application/json`): Future[T] = {
    debug(s"Request: $url")
    request(
      url,
      request => {
        val updatedRequest = request.withMethod(method)
        maybeRequestData
          .map { data => updatedRequest.withEntity(contentType, data) }
          .getOrElse(updatedRequest)
      }
    )
      .flatMap { response =>
        debug(s"Response toStrict: $url")
        response.mapEntity(_.withoutSizeLimit).toStrict(timeout, Long.MaxValue)
      }
      .flatMap { response =>
        debug(s"Response toUtf8String: $url")
        response.entity
          .toStrict(timeout)
          .map(_.data.utf8String)
          .map { content =>
            response.status.intValue() match {
              case 200 | 201 | 204 =>
                convert(content)
              case _ =>
                throw UnsuccessfulStashCall(url, content)
            }
          }
      }
  }

  private def singleRequestWithConversion[T](
      url: String,
      method: HttpMethod,
      maybeRequestData: Option[String],
      convert: String => T,
      contentType: ContentType.NonBinary = ContentTypes.`application/json`
  ): T = {
    Await.result(
      asyncSingleRequestWithConversion(url, method, maybeRequestData, convert, contentType),
      timeout
    )
  }

  def get(url: String): Config =
    singleRequestWithConversion(url, HttpMethods.GET, None, ConfigFactory.parseString)
  def post(url: String, data: String): Config =
    singleRequestWithConversion(url, HttpMethods.POST, Some(data), ConfigFactory.parseString)
  def put(url: String, data: String): Config =
    singleRequestWithConversion(url, HttpMethods.PUT, Some(data), ConfigFactory.parseString)

  def asyncPost(url: String, data: String): Future[Config] =
    asyncSingleRequestWithConversion(url, HttpMethods.POST, Some(data), ConfigFactory.parseString)

  override def get[A: JsonReader](url: String, headers: Map[String, String]): A =
    singleRequestWithConversion(url, HttpMethods.GET, None, _.parseJson.convertTo[A])
  override def post[A: JsonReader](url: String, payload: Option[String], headers: Map[String, String]): A =
    singleRequestWithConversion(url, HttpMethods.POST, payload, _.parseJson.convertTo[A])
  override def put[A: JsonReader](url: String, payload: Option[String]): A =
    singleRequestWithConversion(url, HttpMethods.PUT, payload, _.parseJson.convertTo[A])
  override def postWithoutResponse(url: String, payload: Option[String]): Boolean =
    singleRequestWithConversion(url, HttpMethods.PUT, payload, _ => true)

  def getXml(url: String): Elem =
    singleRequestWithConversion(url, HttpMethods.GET, None, XML.loadString)
  def postXml(url: String, xml: Option[Node]): String =
    singleRequestWithConversion(
      url,
      HttpMethods.POST,
      xml.map(_.toString()),
      identity,
      contentType = ContentTypes.`text/xml(UTF-8)`)
}
