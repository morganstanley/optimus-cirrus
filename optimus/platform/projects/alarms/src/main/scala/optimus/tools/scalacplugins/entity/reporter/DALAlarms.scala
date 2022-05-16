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

object DALAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.DAL_MACRO

  // DAL macro (DALMacro.scala)
  val BACK_REFERENCE_TYPE_MISMATCH =
    error2(41000, "Function passed to backReference has wrong result type: expected %s, got %s")
  val MISMATCHED_ENTITY_INDEX_TYPE =
    error2(41003, "Expected type '%s' in @indexed property or def in Entity but found type '%s'")
  val MISMATCHED_EVENT_INDEX_TYPE =
    error2(41004, "Expected type '%s' in @indexed property or def in Event but found type '%s'")
  val MUST_INDEX_PROPERTY = error1(41002, "Property %s must be @indexed.")
  val NO_PROPERTY_IN_COMPANION = error2(41001, "Cannot find %s member of companion %s")
}
