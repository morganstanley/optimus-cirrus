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
package optimus.stratosphere.artifactory

import spray.json.DefaultJsonProtocol
import spray.json.JsonFormat

final case class ArtifactoryResponse(status: String, totals: ArtifactoryResponseDetailedStatus)

final case class ArtifactoryResponseDetailedStatus(success: Int, failure: Int)

object ArtifactoryResponseJsonProtocol extends DefaultJsonProtocol {
  implicit lazy val artifactoryResponseFormat: JsonFormat[ArtifactoryResponse] = jsonFormat2(ArtifactoryResponse)
  implicit lazy val artifactoryResponseDetailedStatusFormat: JsonFormat[ArtifactoryResponseDetailedStatus] =
    jsonFormat2(ArtifactoryResponseDetailedStatus)
}
