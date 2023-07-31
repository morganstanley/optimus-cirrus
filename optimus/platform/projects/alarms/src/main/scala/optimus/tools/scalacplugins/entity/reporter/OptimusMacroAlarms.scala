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

trait OptimusMacroAlarmsBase extends OptimusAlarms {
  protected val macroType: OptimusMacroType
  final protected def base = macroType.base
}

// macro error/warning handling
trait OptimusMacroAlarmHelper extends BuilderHelpers { self: OptimusMacroAlarmsBase =>
  import OptimusAlarmType._
  override type Info = OptimusMacroType
  private def builders(tpe: Tpe) =
    (
      newBuilder(OptimusMacroAlarmBuilder0)(tpe)(_, macroType, _),
      newBuilder(OptimusMacroAlarmBuilder1)(tpe)(_, macroType, _),
      newBuilder(OptimusMacroAlarmBuilder2)(tpe)(_, macroType, _),
      newBuilder(OptimusMacroAlarmBuilder3)(tpe)(_, macroType, _),
      newBuilder(OptimusMacroAlarmBuilder4)(tpe)(_, macroType, _),
      newBuilder(OptimusMacroAlarmBuilder5)(tpe)(_, macroType, _),
      newBuilder(OptimusMacroAlarmBuilder6)(tpe)(_, macroType, _)
    )

  final val (error0, error1, error2, error3, error4, error5, error6) = builders(ERROR)
  final val (warning0, warning1, warning2, warning3, warning4, _, _) = builders(WARNING)
  final val (abort0, abort1, abort2, _, _, _, _) = builders(ABORT)
}

final case class OptimusMacroAlarm(id: AlarmId, tpe: OptimusMacroType, message: String, template: String)
    extends OptimusAlarmBase

abstract class OptimusMacroAlarmBuilder(val id: AlarmId, val tpe: OptimusMacroType, val template: String)
    extends OptimusAlarmBuilder {
  final protected def buildImpl0(): OptimusMacroAlarm =
    OptimusMacroAlarm(id, tpe, template, template)
  final protected def buildImpl(args: String*): OptimusMacroAlarm =
    OptimusMacroAlarm(id, tpe, String.format(template, args: _*), template)
}

final case class OptimusMacroAlarmBuilder0(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply() = buildImpl0()
}

final case class OptimusMacroAlarmBuilder1(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg: Any) = buildImpl(arg.toString)
}

final case class OptimusMacroAlarmBuilder2(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any) = buildImpl(arg1.toString, arg2.toString)
}

final case class OptimusMacroAlarmBuilder3(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any) =
    buildImpl(arg1.toString, arg2.toString, arg3.toString)
}

final case class OptimusMacroAlarmBuilder4(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any) =
    buildImpl(arg1.toString, arg2.toString, arg3.toString, arg4.toString)
}

final case class OptimusMacroAlarmBuilder5(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any, arg5: Any) =
    buildImpl(arg1.toString, arg2.toString, arg3.toString, arg4.toString, arg5.toString)
}

final case class OptimusMacroAlarmBuilder6(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any, arg5: Any, arg6: Any) =
    buildImpl(arg1.toString, arg2.toString, arg3.toString, arg4.toString, arg5.toString, arg6.toString)
}
