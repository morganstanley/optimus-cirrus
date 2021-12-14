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
  val COMPLEX_OUTPUT_TARGET =
    error1(52001, "Output target can only bind from @nodes (with stable args, or no args) on @entities %s")
  val DUPLICATE_PROPERTY = error1(52002, "%s is defined more than once")
  val UNEXPECTED_BIND_TYPE = error1(52003, "Unexpected bind type: %s")

  val ILLEGAL_BIND_TYPE = abort1(52006, "internal error: %s is not a ReactiveBindingGenerator!")
  val FAILED_TO_FIND_CONTEXT = error0(52010, "Macro failed to find correct Context. This should not happen!")
  val NESTED_SCENARIO =
    error0(52012, "Nested scenario is not supported in reactive, please use ScenarioReference instead")

  val NON_ENTITY_NODE = error3(52013, "In the expression '%s', '%s' is a @node, but the owner %s is not an @entity")
  val TWEAKABLE = error1(52014, "output binding must contain only stable values. %s is tweakable")
  val TWEAKABLE_IN_OVERRIDE =
    error1(52015, "output binding must contain only stable values. %s is potentially tweakable in a subclass")
  val NO_DEF_IN_OUT_PATH = error2(
    52016,
    "In the expression '%s', '%s' is a def and so not stable. The path to an output binding must be stable")
  val UNKNOWN_BIND_ELEMENT = abort1(52017, "Unknown bind block element: %s")
  val UNSUPPORTED_OUTPUT_BIND =
    error2(52018, "Output binding must be an @entity @node, construct unsupported %s. (Info: %s)")
  val CONSTANT_OUTPUT_BIND =
    error1(52019, "Output binding must be an @entity @node, not a constant value %s")

  // Reactive macro util (ReactiveMacroUtil.scala)
  val TARGET_MUST_TWEAKABLE = error0(53000, "The bind target must be tweakable node")
  val HANDLER_MUST_DEF = error0(53001, "The handler method must be a def")
  val HANDLER_MUST_PUBLIC = error0(53002, "The handler method must be public")
  val HANDLER_IN_PUBLIC_ENTITY = error0(53003, "The enclosing @stored @entity of the handler method must be public")
  val HANDLER_CANNOT_OVERRIDE = error0(
    53004,
    "The handler method must not be overridable. Either the enclosing @entity should be final, the def should be final, or is should be a def on an object"
  )
  val HANDLER_MUST_TOP_LEVEL_METHOD =
    error0(53005, "The handler method must be a top level method of an @entity")
  val HANDLER_WITH_NODE_ANNO = error0(53007, "A handler method must not be an @node")
}
