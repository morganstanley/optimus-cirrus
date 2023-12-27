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
package optimus.buildtool.config

import optimus.buildtool.format.WarningsConfiguration
import spray.json._

import scala.collection.immutable.Seq

final case class ScalacConfiguration(
    options: Seq[String],
    ignoredPlugins: Seq[ScopeId],
    target: Option[String],
    warnings: WarningsConfiguration) {

  def withParent(parent: ScalacConfiguration): ScalacConfiguration = ScalacConfiguration(
    parent.options ++ options,
    parent.ignoredPlugins ++ ignoredPlugins,
    target.orElse(parent.target),
    warnings.withParent(parent.warnings)
  )

  def optionsArray: Array[String] = options.toArray
  def targetOption: Option[String] = target.map(t => s"-target:$t")
  def resolvedOptions: Seq[String] = options ++ targetOption

  def asJson =
    JsObject(
      Seq(
        "options" -> JsArray(options.map(JsString.apply): _*),
        "warnings" -> warnings.asJson,
        "ignoredPlugins" -> JsArray(ignoredPlugins.map(id => JsString.apply(id.properPath)): _*)
      ) ++
        target.map("target" -> JsString(_)): _*
    )
}

object ScalacConfiguration {
  def empty: ScalacConfiguration =
    ScalacConfiguration(options = Nil, ignoredPlugins = Nil, target = None, WarningsConfiguration.empty)

}
