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

object ReactiveAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.REACTIVE_MACRO

  // Reactive macro (ReactiveMacro.scala)
  val INTERNAL_ERROR = error1(51000, "Internal error in ReactiveMacro - details will be reported separately - %s")
  val ABORT_INTERNAL_ERROR =
    abort1(51001, "Internal error in ReactiveMacro - details are available in the scala-log [via stderr] - %s")

  // Reactive bind macro (ReactiveBindMacro.scala)
  val DUPLICATE_PROPERTY = error1(52002, "%s is defined more than once")

  val ILLEGAL_BIND_TYPE = abort1(52006, "internal error: %s is not a ReactiveBindingGenerator!")
  val NESTED_SCENARIO =
    error0(52012, "Nested scenario is not supported in reactive, please use ScenarioReference instead")

  val UNKNOWN_BIND_ELEMENT = abort1(52017, "Unknown bind block element: %s")

  // Reactive macro util (ReactiveMacroUtil.scala)
  val TARGET_MUST_TWEAKABLE = error0(53000, "The bind target must be tweakable node")
}
