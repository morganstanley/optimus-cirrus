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
package optimus.tools.scalacplugins.entity

class EntitySettings {
  var entityRelation = false
  // internal use only (for test)
  var checkCtorSI = false
  var warts: List[String] = Nil
  var enableStaging: Boolean = false
  var posValidate = false
  var disableExportInfo: Boolean = false
  var loom: Boolean = LoomDefaults.enabled
}

object EntitySettings {
  object OptionNames {
    val wartsName = "warts:"
    val enableStagingName = "enableStaging:"
    val loomName = "loom:"
    val posValidateName = "posValidate:"
    val disableExportInfoName = "disableExportInfoPleaseUseOnlyInTests:"
    val enableEntityRelationName = "entityRelation:"
    val autoparName = "autopar:"
  }
}
