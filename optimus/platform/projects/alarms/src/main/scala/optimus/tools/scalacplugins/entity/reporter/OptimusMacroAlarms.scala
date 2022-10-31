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
trait OptimusMacroAlarmHelper { self: OptimusMacroAlarmsBase =>
  def error0(sn: Int, template: String): OptimusMacroAlarmBuilder0 = {
    register(
      OptimusMacroAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.ERROR),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def error1(sn: Int, template: String): OptimusMacroAlarmBuilder1 = {
    register(
      OptimusMacroAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.ERROR),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def error2(sn: Int, template: String): OptimusMacroAlarmBuilder2 = {
    register(
      OptimusMacroAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.ERROR),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def error3(sn: Int, template: String): OptimusMacroAlarmBuilder3 = {
    register(
      OptimusMacroAlarmBuilder3(
        alarmId(sn, OptimusAlarmType.ERROR),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def error4(sn: Int, template: String): OptimusMacroAlarmBuilder4 = {
    register(
      OptimusMacroAlarmBuilder4(
        alarmId(sn, OptimusAlarmType.ERROR),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def error5(sn: Int, template: String): OptimusMacroAlarmBuilder5 = {
    register(
      OptimusMacroAlarmBuilder5(
        alarmId(sn, OptimusAlarmType.ERROR),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def error6(sn: Int, template: String): OptimusMacroAlarmBuilder6 = {
    register(
      OptimusMacroAlarmBuilder6(
        alarmId(sn, OptimusAlarmType.ERROR),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def warning0(sn: Int, template: String): OptimusMacroAlarmBuilder0 = {
    register(
      OptimusMacroAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.WARNING),
        macroType,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def warning1(sn: Int, template: String): OptimusMacroAlarmBuilder1 = {
    register(
      OptimusMacroAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.WARNING),
        macroType,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def warning2(sn: Int, template: String): OptimusMacroAlarmBuilder2 = {
    register(
      OptimusMacroAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.WARNING),
        macroType,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def abort0(sn: Int, template: String): OptimusMacroAlarmBuilder0 = {
    register(
      OptimusMacroAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.ABORT),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def abort1(sn: Int, template: String): OptimusMacroAlarmBuilder1 = {
    register(
      OptimusMacroAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.ABORT),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def abort2(sn: Int, template: String): OptimusMacroAlarmBuilder2 = {
    register(
      OptimusMacroAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.ABORT),
        macroType,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

}

case class OptimusMacroAlarm(id: AlarmId, tpe: OptimusMacroType, message: String, template: String)
    extends OptimusAlarmBase

abstract class OptimusMacroAlarmBuilder(val id: AlarmId, val tpe: OptimusMacroType, val template: String)
    extends OptimusAlarmBuilder {

  final protected def buildImpl0(obtMandatory: Boolean, scalaMandatory: Boolean): OptimusMacroAlarm =
    OptimusMacroAlarm(id, tpe, template, template)
  final protected def buildImpl(obtMandatory: Boolean, scalaMandatory: Boolean, args: String*): OptimusMacroAlarm =
    OptimusMacroAlarm(id, tpe, String.format(template, args: _*), template)
}

case class OptimusMacroAlarmBuilder0(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply() = buildImpl0(obtMandatory, scalaMandatory: Boolean)
}

case class OptimusMacroAlarmBuilder1(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg: Any) = buildImpl(obtMandatory, scalaMandatory: Boolean, arg.toString)
}

case class OptimusMacroAlarmBuilder2(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any) = buildImpl(obtMandatory, scalaMandatory: Boolean, arg1.toString, arg2.toString)
}

case class OptimusMacroAlarmBuilder3(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any) =
    buildImpl(obtMandatory, scalaMandatory: Boolean, arg1.toString, arg2.toString, arg3.toString)
}

case class OptimusMacroAlarmBuilder4(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any) =
    buildImpl(obtMandatory, scalaMandatory: Boolean, arg1.toString, arg2.toString, arg3.toString, arg4.toString)
}

case class OptimusMacroAlarmBuilder5(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any, arg5: Any) =
    buildImpl(
      obtMandatory,
      scalaMandatory: Boolean,
      arg1.toString,
      arg2.toString,
      arg3.toString,
      arg4.toString,
      arg5.toString)
}

case class OptimusMacroAlarmBuilder6(
    override val id: AlarmId,
    override val tpe: OptimusMacroType,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    override val template: String)
    extends OptimusMacroAlarmBuilder(id, tpe, template) {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any, arg5: Any, arg6: Any) =
    buildImpl(
      obtMandatory,
      scalaMandatory: Boolean,
      arg1.toString,
      arg2.toString,
      arg3.toString,
      arg4.toString,
      arg5.toString,
      arg6.toString)
}
