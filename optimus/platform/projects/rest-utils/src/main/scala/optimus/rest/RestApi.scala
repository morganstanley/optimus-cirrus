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
package optimus.rest

import spray.json.JsonReader

final case class UnsuccessfulRestCall(url: String, status: Int, message: String, rawPayload: Option[String])
    extends Exception(s"$message at: $url with status: $status")

trait RestApi {
  protected def get[A: JsonReader](url: String, headers: Map[String, String] = Map.empty): A

  protected def post[A: JsonReader](url: String, payload: Option[String], headers: Map[String, String] = Map.empty): A

  protected def put[A: JsonReader](url: String, payload: Option[String]): A

  protected def postWithoutResponse(url: String, payload: Option[String]): Boolean
}
