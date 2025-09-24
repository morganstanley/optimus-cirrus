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

object UIAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.UI_MACRO

  // UI assignment macro (AssignmentMacro.scala)
  val BIND_TO_NONNODE_NONBINDABLE =
    error0(63000, "The binding argument must be a node expression or a BindableNodeKey[_]")
  val BIND_TO_NONTWEAK_NODE =
    error1(63001, "Invalid binding to a non tweakable node. %s binding argument must be @node(tweak = true).")
  val NO_IMPLICIT_CONVERSION = error3(
    63002,
    "Implicit conversion could not be found from %s to %s (%s). " +
      "Please ensure that an implicit conversion function is visible in a given context (import) or manually specify a converter for the binding."
  )
  val BIND_TO_NODE_BINDABLE_AMBIGUOUS =
    error1(
      63003,
      "Invalid binding to a node which returns BindableNodeKey[_]. %s binding argument must be either a node expression OR a BindableNodeKey[_].")
  val TOGRAPH_BIND_TO_FROMGRAPH_BINDABLE =
    error1(
      63004,
      "Invalid %s binding to a FromGraphBindableNodeKey[_]. If using BindNode.fromGraph, make sure to specify a custom handler. " +
        "Alternatively bind to a tweakable node with BindNode.apply. If specifying converter with graphToUi method, " +
        "make sure to specify the uiToGraph converter if you want a ToGraph component in the binding."
    )
  val FROMGRAPH_BIND_TO_TOGRAPH_BINDABLE =
    error1(
      63005,
      "Invalid %s binding to a ToGraphBindableNodeKey[_]. If specifying converter with uiToGraph method, " +
        "make sure to specify the graphToUi converter if you want a FromGraph component to the binding."
    )
  val TWO_WAY_BINDABLE_TO_NONTWEAK_NODE =
    error2(
      63006,
      "Invalid construction of a TwoWayBindableNodeKey[_] with a non tweakable node. When using BindNode.apply, %s must be @node(tweak = true). " +
        "If you want a binding to a non tweakable node, use BindNode.%s."
    )

  // UI gui block macro (GuiBlockMacro.scala)
  val INVALID_TYPE = abort1(
    63100,
    "Invalid type in the gui block: %s. Gui block can " +
      "only contain gui widgets, import statements or variables returning gui widgets / collection of gui widgets."
  )

  val INVALID_TYPE_MOBILE = abort1(
    63210,
    "Invalid type in the mgui block: %s. mgui block can " +
      "only contain mobile gui widgets, import statements or variables returning mobile gui widgets / collection of mobile gui widgets."
  )
  val MACRO_EXCEPTION = abort2(63101, "GuiBlockMacro exception %s while type checking %s")
  val INVALID_EXPR = abort1(63102, "Invalid form of transformed expression: %s")

  // UI handler macro (HandlerMacro.scala)
  val INVALID_RETURN_TYPE = abort1(
    63200,
    "Invalid return type of a handler function (%s). " +
      "Expected a Gesture, sequence of tweaks or sequence of GUI widgets.")
  val HANDLER_SUBTYPE_CHECK = warning2(
    63201,
    "The handler parameter type (%s) is a specific version " +
      "of the required parameter (%s). This may result in a runtime error.")
  val INVALID_HANDLER_PARAM = abort2(
    63202,
    "Invalid type of a handler function parameter (%s). " +
      "Expected a type sharing the type hierarchy with %s.")
  val HANDLER_WITH_TOO_MANY_PARAM = abort0(
    63203,
    "More than one parameter to a handler. A handler function can only take [no arguments] or a single argument of type (or subtype) of [EventArgs]."
  )
  val NEED_UNDERSCORE = abort0(
    63204,
    "Invalid type of a handler (Seq[optimus.platform.Tweak]). If this is a function call please follow it with an _ to make it a function object.")
  val INVALID_HANDLER_TYPE = abort1(
    63205,
    "Invalid type of a handler (%s). " +
      "A ui handler must be either a @handle function or a gesture.")
  val INVALID_ASSIGN_TYPE = abort2(
    63209,
    "Invalid assign type, expected: (%s), actual: (%s), that means we failed to find the implicit converters for those 2 types, and the UI's default converters is not enough."
  )
}
