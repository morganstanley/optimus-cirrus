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
package optimus.buildtool.utils

object OptimusBuildToolProperties {
  private val Prefix = "optimus.buildtool"

  def get(subKey: String): Option[String] = sys.props.get(s"$Prefix.$subKey")
  def getOrElse(subKey: String, default: String): String = sys.props.getOrElse(s"$Prefix.$subKey", default)

  def getOrFalse(subKey: String): Boolean = get(subKey).exists(_.toBoolean)
  def getOrTrue(subKey: String): Boolean = get(subKey).forall(_.toBoolean)

  def asInt(subKey: String): Option[Int] = get(subKey).map(_.toInt)
  def asLong(subKey: String): Option[Long] = get(subKey).map(_.toLong)
  def asSet(subKey: String): Set[String] = get(subKey).map(_.split(",").toSet).getOrElse(Set.empty)

  val minimalLogging: Boolean = getOrFalse("minimalLogging")

}
