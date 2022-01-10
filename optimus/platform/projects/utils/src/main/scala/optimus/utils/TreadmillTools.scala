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
package optimus.utils

object TreadmillTools {
  def treadmillInstance(): Option[String] = {
    val unknown = None
    val env = sys.env
    env.get("TREADMILL_CELL") map { cell =>
      val app = env.getOrElse("TREADMILL_APP", unknown)
      val instanceId = env.getOrElse("TREADMILL_INSTANCEID", unknown)
      val uniqueId = env.getOrElse("TREADMILL_UNIQUEID", unknown)
      // TM3 instance descriptor format: <TREADMILL_CELL>/<TREADMILL_APP>#<TREADMILL_INSTANCEID>/<TREADMILL_UNIQUEID>
      s"$cell/$app#$instanceId/$uniqueId"
    }
  }
}
