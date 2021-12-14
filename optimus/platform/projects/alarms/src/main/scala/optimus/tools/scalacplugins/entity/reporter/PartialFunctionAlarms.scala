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

import optimus.exceptions.RTListStatic

object PartialFunctionAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.PARTIAL_FUNC_MACRO

  // partial function macro (PartialFunctionMacro.scala)
  val NON_PARTIAL_FUNCTION = abort1(71000, "%s is not partial function definition")
  val ERROR_TRANSFORM = abort1(71001, "error expanding @node partial function when transforming %s")
  val BAD_EXCEPTION = abort1(71002, s"%s is outside of the list known to be RT: ${RTListStatic.members.mkString(", ")}")
  val UNSPECIFIED_EXCEPTION = abort0(71003, "May not catch unspecified exceptions")
  val BROAD_EXCEPTION = error0(71004, "Please use `case e @ RTException` to catch all known RT exceptions")
  val SCALA_BUG = error1(71005, "Triggers scala bug %s")
  val NESTED_UNAPPLY = error0(71006, "Unapply may not be nexted within RTException.unapply")
}
