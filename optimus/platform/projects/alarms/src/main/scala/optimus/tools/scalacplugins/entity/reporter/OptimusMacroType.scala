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
package optimus.tools.scalacplugins.entity.reporter

case class OptimusMacroType(base: Int, name: String, description: String)

object OptimusMacroType {

  // N.B. This base number is related to OptimusWarningsBase and OptimusErrorsBase in OptimusAlarms.scala
  // and may be used to control error/warning report based on phase/macro type
  lazy val PICKLE_MACRO =
    OptimusMacroType(30000, "pickling_macro", "used to generate pickle/unpickle method for @embeddable")
  lazy val DAL_MACRO = OptimusMacroType(40000, "DAL_macro", "used to handle ???")
  lazy val REACTIVE_MACRO = OptimusMacroType(50000, "reactive_macro", "used to define reactive dsl")
  lazy val UI_MACRO = OptimusMacroType(60000, "UI_macro", "used to defined UI dsl")
  lazy val PARTIAL_FUNC_MACRO =
    OptimusMacroType(70000, "partial_function_macro", "used to generated async partial function")
  lazy val VERSIONING_MACRO = OptimusMacroType(80000, "versioning_macro", "used to define versioning transformer")
  lazy val COPYMETHOD_MACRO = OptimusMacroType(90000, "copymethod_macro", "used to generate copy method")
  lazy val RELATIONAL_MACRO = OptimusMacroType(100000, "relational_macro", "used to validate PriQL")
  lazy val MACRO_UTILS = OptimusMacroType(110000, "macro_utils", "utilities used by other macros")
  lazy val PLUGIN_MACRO = OptimusMacroType(130000, "plugin_macros", "macros used by entityplugin")
}
