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
package optimus.graph.diagnostics.ap

import optimus.breadcrumbs.crumbs.Properties
import optimus.scalacompat.collection._
import optimus.platform.util.Log

import spray.json._

object CrumbStackMatcher extends Log {

  type CrumbMap = Map[String, JsValue]

  private val q = "\""
  private val ProfStacksMarker = s"$q${Properties.profStacks.name}$q:[{"
  def likelyStacksSample(s: String): Boolean = s.contains(ProfStacksMarker)
  private val ProfCollapsedMarker = s"$q${Properties.profCollapsed}$q:$q"
  def likelyStacksFrames(s: String): Boolean = s.contains(ProfCollapsedMarker)
  private val StackPathMarker = s"$q${Properties.profMS}$q:$q"
  def likelyStackPathFrames(s: String): Boolean = s.contains(StackPathMarker)
  private val OtherStacksMarker = s"$q${Properties.pulse.name}$q:{"
  def likelyTimeSample(s: String): Boolean = s.contains(OtherStacksMarker) && !likelyStacksSample(s)
  private val HotspotsMarker = s"$q${Properties.spPropertyName.name}$q:$q"
  def likelyHotspots(s: String): Boolean = s.contains(HotspotsMarker)
  private val AppArgsMarker = s"$q${Properties.argsType.name}$q:$q"
  def likelyAppArgs(s: String): Boolean = s.contains(AppArgsMarker)

  def utcMsToMin(utc: Long): Int = (utc / 60000).toInt

}
