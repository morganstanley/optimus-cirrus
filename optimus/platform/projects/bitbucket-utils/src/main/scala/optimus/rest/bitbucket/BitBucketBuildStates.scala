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
package optimus.rest.bitbucket

import spray.json._

import scala.util.Try

object BitBucketBuildStates extends Enumeration {
  type StashBuildState = Value

  val successful: StashBuildState = Value("SUCCESSFUL")
  val inProgress: StashBuildState = Value("INPROGRESS")
  val failed: StashBuildState = Value("FAILED")
  val unknown: StashBuildState = Value("UNKNOWN")

  // Enriched Build States:
  val uploading: StashBuildState = Value("UPLOADING") // Jenkins pipeline is uploading artifacts to NFS

  implicit def stashBuildState2String(state: StashBuildState): String = state.toString
  implicit def string2StashBuildState(s: String): StashBuildState =
    Try(BitBucketBuildStates.withName(s.toUpperCase)).getOrElse(unknown)

  implicit lazy val stashBuildStateCommonFormat: RootJsonFormat[BitBucketBuildStates.Value] =
    new RootJsonFormat[BitBucketBuildStates.Value] {
      override def write(obj: BitBucketBuildStates.Value): JsValue = JsString(obj)
      override def read(json: JsValue): BitBucketBuildStates.Value = json match {
        case JsString(txt) => BitBucketBuildStates.string2StashBuildState(txt)
        case unknown =>
          throw DeserializationException(s"expected string of ${BitBucketBuildStates.toString}, but got $unknown")
      }
    }

}
