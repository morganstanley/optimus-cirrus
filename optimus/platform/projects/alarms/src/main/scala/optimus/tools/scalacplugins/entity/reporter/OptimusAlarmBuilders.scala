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

import optimus.tools.scalacplugins.entity.OptimusPhaseInfo

trait OptimusPluginAlarmHelper { self: OptimusAlarms =>
  def error0(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder0 = {
    register(
      OptimusAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def errorOptional0(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder0 = {
    register(
      OptimusAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def error1(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder1 = {
    register(
      OptimusAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def errorOptional1(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder1 = {
    register(
      OptimusAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def error2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def errorOptional2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def error3(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder3 = {
    register(
      OptimusAlarmBuilder3(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def errorOptional3(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder3 = {
    register(
      OptimusAlarmBuilder3(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def error4(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder4 = {
    register(
      OptimusAlarmBuilder4(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def error5(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder5 = {
    register(
      OptimusAlarmBuilder5(
        alarmId(sn, OptimusAlarmType.ERROR),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def warning0(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder0 = {
    register(
      OptimusAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def warningOptional0(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder0 = {
    register(
      OptimusAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def warning1(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder1 = {
    register(
      OptimusAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def warningOptional1(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder1 = {
    register(
      OptimusAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def warning2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def warningObtNonSuppressible2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = true,
        scalaMandatory = false,
        template))
  }

  def warningOptional2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def warning3(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder3 = {
    register(
      OptimusAlarmBuilder3(
        alarmId(sn, OptimusAlarmType.WARNING),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def info0(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder0 = {
    register(
      OptimusAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def infoOptional0(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder0 = {
    register(
      OptimusAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def info1(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder1 = {
    register(
      OptimusAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def infoOptional1(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder1 = {
    register(
      OptimusAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def info2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def infoOptional2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def info3(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder3 = {
    register(
      OptimusAlarmBuilder3(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def infoOptional3(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder3 = {
    register(
      OptimusAlarmBuilder3(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def info4(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder4 = {
    register(
      OptimusAlarmBuilder4(
        alarmId(sn, OptimusAlarmType.INFO),
        phase,
        obtMandatory = true,
        scalaMandatory = true,
        template))
  }

  def debug0(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder0 = {
    register(
      OptimusAlarmBuilder0(
        alarmId(sn, OptimusAlarmType.DEBUG),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def debug1(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder1 = {
    register(
      OptimusAlarmBuilder1(
        alarmId(sn, OptimusAlarmType.DEBUG),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

  def debug2(sn: Int, phase: OptimusPhaseInfo, template: String): OptimusAlarmBuilder2 = {
    register(
      OptimusAlarmBuilder2(
        alarmId(sn, OptimusAlarmType.DEBUG),
        phase,
        obtMandatory = false,
        scalaMandatory = false,
        template))
  }

}

abstract class OptimusNonErrorMessagesBase extends OptimusAlarms with OptimusPluginAlarmHelper {
  final protected val base = 10000
}

abstract class OptimusErrorsBase extends OptimusAlarms with OptimusPluginAlarmHelper {
  final protected val base = 20000
}

case class OptimusPluginAlarm(
    id: AlarmId,
    phase: OptimusPhaseInfo,
    message: String,
    template: String,
    obtMandatory: Boolean,
    scalaMandatory: Boolean)
    extends OptimusAlarmBase

trait OptimusAlarmBuilder {
  val id: AlarmId
  val template: String
  val obtMandatory: Boolean
  val scalaMandatory: Boolean
}

trait OptimusPluginAlarmBuilder extends OptimusAlarmBuilder {
  val phase: OptimusPhaseInfo
  final protected def buildImpl0(obtMandatory: Boolean, scalaMandatory: Boolean): OptimusPluginAlarm =
    OptimusPluginAlarm(id, phase, template, template, obtMandatory, scalaMandatory)
  final protected def buildImpl(obtMandatory: Boolean, scalaMandatory: Boolean, args: String*): OptimusPluginAlarm =
    OptimusPluginAlarm(id, phase, String.format(template, args: _*), template, obtMandatory, scalaMandatory)
}

case class OptimusAlarmBuilder0(
    id: AlarmId,
    phase: OptimusPhaseInfo,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    template: String)
    extends OptimusPluginAlarmBuilder {
  def apply() = buildImpl0(obtMandatory, scalaMandatory)
  def as(tpe: OptimusAlarmType.Tpe): OptimusAlarmBuilder0 = copy(id = id.copy(tpe = tpe))
}

case class OptimusAlarmBuilder1(
    id: AlarmId,
    phase: OptimusPhaseInfo,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg: Any) = buildImpl(obtMandatory, scalaMandatory, arg.toString)
  def as(tpe: OptimusAlarmType.Tpe) = copy(id = id.copy(tpe = tpe))
}

case class OptimusAlarmBuilder2(
    id: AlarmId,
    phase: OptimusPhaseInfo,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any) = buildImpl(obtMandatory, scalaMandatory, arg1.toString, arg2.toString)
  def as(tpe: OptimusAlarmType.Tpe) = copy(id = id.copy(tpe = tpe))
}

case class OptimusAlarmBuilder3(
    id: AlarmId,
    phase: OptimusPhaseInfo,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any, arg3: Any) =
    buildImpl(obtMandatory, scalaMandatory, arg1.toString, arg2.toString, arg3.toString)
  def as(tpe: OptimusAlarmType.Tpe) = copy(id = id.copy(tpe = tpe))
}

case class OptimusAlarmBuilder4(
    id: AlarmId,
    phase: OptimusPhaseInfo,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any) =
    buildImpl(obtMandatory, scalaMandatory, arg1.toString, arg2.toString, arg3.toString, arg4.toString)
  def as(tpe: OptimusAlarmType.Tpe) = copy(id = id.copy(tpe = tpe))
}

case class OptimusAlarmBuilder5(
    id: AlarmId,
    phase: OptimusPhaseInfo,
    obtMandatory: Boolean,
    scalaMandatory: Boolean,
    template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any, arg5: Any) =
    buildImpl(obtMandatory, scalaMandatory, arg1.toString, arg2.toString, arg3.toString, arg4.toString, arg5.toString)
  def as(tpe: OptimusAlarmType.Tpe) = copy(id = id.copy(tpe = tpe))
}
