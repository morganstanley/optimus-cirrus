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

trait OptimusPluginAlarmHelper extends BuilderHelpers { self: OptimusAlarms =>
  import OptimusAlarmType._
  override type Info = OptimusPhaseInfo
  private def builders(tpe: Tpe) =
    (
      newBuilder(OptimusAlarmBuilder0)(tpe),
      newBuilder(OptimusAlarmBuilder1)(tpe),
      newBuilder(OptimusAlarmBuilder2)(tpe),
      newBuilder(OptimusAlarmBuilder3)(tpe),
      newBuilder(OptimusAlarmBuilder4)(tpe),
      newBuilder(OptimusAlarmBuilder5)(tpe)
    )

  final val (error0, error1, error2, error3, error4, error5) = builders(ERROR)
  final val (warning0, warning1, warning2, warning3, warning4, _) = builders(WARNING)
  final val (info0, info1, info2, info3, info4, info5) = builders(INFO)
  final val (debug0, debug1, debug2, _, _, _) = builders(DEBUG)
}

abstract class OptimusNonErrorMessagesBase extends OptimusAlarms with OptimusPluginAlarmHelper {
  final protected val base = 10000
}

abstract class OptimusErrorsBase extends OptimusAlarms with OptimusPluginAlarmHelper {
  final protected val base = 20000
}

final case class OptimusPluginAlarm(id: AlarmId, phase: OptimusPhaseInfo, message: String, template: String)
    extends OptimusAlarmBase

trait OptimusPluginAlarmBuilder extends OptimusAlarmBuilder {
  val phase: OptimusPhaseInfo
  final protected def buildImpl0(): OptimusPluginAlarm =
    OptimusPluginAlarm(id, phase, template, template)
  final protected def buildImpl(args: String*): OptimusPluginAlarm =
    OptimusPluginAlarm(id, phase, String.format(template, args: _*), template)
}

final case class OptimusAlarmBuilder0(id: AlarmId, phase: OptimusPhaseInfo, template: String)
    extends OptimusPluginAlarmBuilder {
  def apply() = buildImpl0()
}

final case class OptimusAlarmBuilder1(id: AlarmId, phase: OptimusPhaseInfo, template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any) = buildImpl(arg1.toString)
}

final case class OptimusAlarmBuilder2(id: AlarmId, phase: OptimusPhaseInfo, template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any) = buildImpl(arg1.toString, arg2.toString)
}

final case class OptimusAlarmBuilder3(id: AlarmId, phase: OptimusPhaseInfo, template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any, arg3: Any) =
    buildImpl(arg1.toString, arg2.toString, arg3.toString)
}

final case class OptimusAlarmBuilder4(id: AlarmId, phase: OptimusPhaseInfo, template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any) =
    buildImpl(arg1.toString, arg2.toString, arg3.toString, arg4.toString)
}

final case class OptimusAlarmBuilder5(id: AlarmId, phase: OptimusPhaseInfo, template: String)
    extends OptimusPluginAlarmBuilder {
  def apply(arg1: Any, arg2: Any, arg3: Any, arg4: Any, arg5: Any) =
    buildImpl(arg1.toString, arg2.toString, arg3.toString, arg4.toString, arg5.toString)
}
