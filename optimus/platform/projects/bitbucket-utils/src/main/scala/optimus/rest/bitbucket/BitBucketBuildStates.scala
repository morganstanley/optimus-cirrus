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
  type BitbucketBuildState = Value

  val successful: BitbucketBuildState = Value("SUCCESSFUL")
  val inProgress: BitbucketBuildState = Value("INPROGRESS")
  val failed: BitbucketBuildState = Value("FAILED")
  val unknown: BitbucketBuildState = Value("UNKNOWN")

  // Enriched Build States:
  val uploading: BitbucketBuildState = Value("UPLOADING") // Jenkins pipeline is uploading artifacts to NFS

  implicit def bitbucketBuildState2String(state: BitbucketBuildState): String = state.toString
  implicit def string2StashBuildState(s: String): BitbucketBuildState =
    Try(BitBucketBuildStates.withName(s.toUpperCase)).getOrElse(unknown)

  implicit lazy val bitbucketBuildStateCommonFormat: RootJsonFormat[BitBucketBuildStates.Value] =
    new RootJsonFormat[BitBucketBuildStates.Value] {
      override def write(obj: BitBucketBuildStates.Value): JsValue = JsString(obj)
      override def read(json: JsValue): BitBucketBuildStates.Value = json match {
        case JsString(txt) => BitBucketBuildStates.string2StashBuildState(txt)
        case unknown =>
          throw DeserializationException(s"expected string of ${BitBucketBuildStates.toString}, but got $unknown")
      }
    }

}
