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

object HandlerAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.UI_MACRO

  val NEED_HANDLER_ANNO = error0(63206, "definition of method used as handler should be annotated @handle")
  val NO_SIGNAL_EVENT_HANDLER = warning0(63209, "no signal events type is found in handler")
  val NO_TRANSACTION_SIGNAL_EVENT_HANDLER =
    warning0(63211, "no upsertable transaction signal events type is found in handler")
}
