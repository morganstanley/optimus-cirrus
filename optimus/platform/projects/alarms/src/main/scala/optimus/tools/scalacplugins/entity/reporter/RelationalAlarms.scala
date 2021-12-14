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

object RelationalAlarms extends OptimusMacroAlarmsBase with OptimusMacroAlarmHelper {
  protected val macroType = OptimusMacroType.RELATIONAL_MACRO

  val NOPRIQL_ERROR =
    abort2(101000, "%s is annotated as @noDalPriql so cannot be executed as a DAL query e.g. from(%s)")

  val CANNOT_REIFY_ERROR = abort1(101001, "Cannot reify the target code with error: %s")
  val NON_INDEX_FILTER = warning0(101234, "Non-index filter will be executed on client side instead of server side.")

}
