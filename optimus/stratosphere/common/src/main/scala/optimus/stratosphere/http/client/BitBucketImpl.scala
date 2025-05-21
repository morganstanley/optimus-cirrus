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

import akka.http.scaladsl.model.Uri
import optimus.rest.RestApi
import optimus.rest.bitbucket.BitBucket
import optimus.stratosphere.config.StratoWorkspaceCommon
import spray.json._

final class BitBucketImpl(stratoWorkspace: StratoWorkspaceCommon)
    extends BitBucket(stratoWorkspace.internal.bitbucket.hostname)
    with RestApi {
  private val httpClient = HttpClientFactory
    .factory(stratoWorkspace)
    .createClient(Uri(s"http://$instance"), "BitBucket", timeout, sendCrumbs = false)

  override protected def get[A: JsonReader](url: String, headers: Map[String, String]): A = httpClient.get(url, headers)
  override protected def post[A: JsonReader](url: String, payload: Option[String], headers: Map[String, String]): A =
    httpClient.post(url, payload, headers)
  override protected def put[A: JsonReader](url: String, payload: Option[String]): A = httpClient.put(url, payload)
  override protected def postWithoutResponse(url: String, payload: Option[String]): Boolean =
    httpClient.postWithoutResponse(url, payload)
}
