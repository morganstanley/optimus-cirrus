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
package optimus.dsi.base.watchtower

trait DalWatchTowerEvent {

  def id: String

  final def payload(titlePrefix: String): Map[String, String] = {
    // At the time of this writing, maximum payload size allowed is 2K bytes..
    //
    // If event is alertable (e.g. SockEye alert) then it has to follow some standards regarding what keys to include
    // in payload as per discussion with OptimusPM..
    val data =
      if (alertable) Map("nc_title" -> s"$titlePrefix $title", "nc_message" -> message)
      else Map("title" -> title, "message" -> message)
    data ++ additionalPayload
  }

  protected def additionalPayload: Map[String, String] = Map.empty

  protected def alertable: Boolean = false
  protected def title: String
  protected def message: String

  override def toString: String = s"${this.getClass.getName}($message)"
}
