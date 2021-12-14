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

object MacroUtilsAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.MACRO_UTILS

  private val internalError = "internal macro error:"

  val INCORRECT_SYMBOL_OWNER = error6(
    110000,
    s"$internalError symbol %s (in tree %s) should have been owned by " +
      "symbol %s (in tree %s) but was actually owned by symbol %s (in tree %s)")
  val MACRO_TYPECHECK_FAILURE = error2(110001, s"$internalError tree produced by macro failed to typecheck: %s (error: %s)")
  val UNTYPECHECKED_TREE = error1(110002, s"$internalError found untypechecked tree  %s inside typechecked tree")
  val NO_SYMBOL = error1(110003, s"$internalError found tree with no symbol %s")
  val CONFUSING_FUNCTION = error1(110004, s"$internalError Unable to extract function from %s")
}
