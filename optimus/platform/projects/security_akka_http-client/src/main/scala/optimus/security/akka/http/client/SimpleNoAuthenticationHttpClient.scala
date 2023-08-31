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
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.Uri

import scala.collection.immutable

class SimpleNoAuthenticationHttpClient(rootUri: Uri)(implicit actorSystem: ActorSystem)
    extends AbstractHttpClient(rootUri) {
  override protected def addAuthenticationHeaders(
      uri: Uri,
      httpHeaders: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
    httpHeaders // No authentication
}
