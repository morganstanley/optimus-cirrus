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
package optimus.tools.scalacplugins.entity
package reporter

import java.lang.{reflect => jlr}

import scala.collection.mutable
import scala.reflect.internal.util.Position
import scala.tools.nsc.Global
import scala.tools.nsc.Reporting

trait OptimusPluginReporter {
  val global: Global
  val phaseInfo: OptimusPhaseInfo
  val pluginData: PluginData

  import OptimusPluginReporter._

  // TODO (OPTIMUS-51339): Remove the flag and make alarmNew the only option. When that is done, we can also remove
  //  alarmConfig and hasNew.
  def alarm(alarm: OptimusPluginAlarm, pos: Position): Unit =
    if (pluginData.alarmConfig.obtWarnConf) alarmObt(alarm, pos) else alarmLegacy(alarm, pos)

  def alarmObt(alarm: OptimusPluginAlarm, pos: Position): Unit = {
    val level = alarm.id.tpe
    val locallySuppressed = isLocallySuppressed(alarm, pos)
    val msg = {
      val msg = alarm.toString()
      if (locallySuppressed) s"${OptimusAlarms.SuppressedTag} $msg" else msg
    }

    level match {
      case OptimusAlarmType.ERROR   => global.reporter.error(pos, msg)
      case OptimusAlarmType.WARNING => global.reporter.warning(pos, msg)
      case OptimusAlarmType.INFO    => global.reporter.echo(pos, msg)
      case OptimusAlarmType.DEBUG   => if (pluginData.alarmConfig.debug) global.reporter.echo(pos, msg)
      case OptimusAlarmType.SILENT  => // ignore it!
      case unexpected               => throw new IllegalStateException(s"unexpected level $unexpected")
    }
  }

  def alarmLegacy(alarm: OptimusPluginAlarm, pos: Position): Unit = {
    val level = alarm.id.tpe
    val cfgLevel = pluginData.alarmConfig.getConfiguredLevel(alarm.id.sn, level)
    val locallySuppressed = isLocallySuppressed(alarm, pos)
    val hasNew = isNew(alarm)
    val newLevel = if (locallySuppressed || hasNew) OptimusAlarmType.INFO else cfgLevel

    newLevel match {
      case OptimusAlarmType.ERROR   => global.reporter.error(pos, alarm.toString())
      case OptimusAlarmType.WARNING => global.reporter.warning(pos, alarm.toString())
      case OptimusAlarmType.INFO    =>
        // when a `[NEW]` message is also suppressed, drop the `[NEW]` tag to avoid the
        // message being re-promoted to Error in BSPTraceListener/StandardBuilder
        val isSuppressed = locallySuppressed || level != cfgLevel
        val msgNoNew =
          if (hasNew && isSuppressed) alarm.toString().replaceAllLiterally(OptimusAlarms.NewTag, "").trim()
          else alarm.toString()
        // the prefix [SUPPRESSED] is later removed by OBT and the compilation message lifted back to warning.
        // This allows us to suppress a warning at the compile level, but not at the user level.
        val msg: String = if (!alarm.isInfo) s"${OptimusAlarms.SuppressedTag} $msgNoNew" else msgNoNew
        global.reporter.echo(pos, msg)
      case OptimusAlarmType.SILENT => // ignore it!
      case _                       => throw new IllegalStateException(s"unexpected level $newLevel")
    }
  }

  private def dedup[A <: OptimusAlarmBuilder](already: mutable.Set[(Int, Position)], a: A, pos: Position): Boolean = {
    if (already.contains((a.id.sn, pos)))
      false
    else {
      already += ((a.id.sn, pos))
      true
    }
  }

  // override for forcing suppression when testing
  protected[reporter] def suppressOverrideInTesting(msg: AlarmId): Boolean = false

  // Check if `alarm` is suppressed at `pos` without actually issuing it (see the INFO case above)
  private def isLocallySuppressed(alarm: OptimusPluginAlarm, pos: Position): Boolean = {
    if (alarm.toString().contains("@nowarn"))
      false // Very silly to suppress an error that's already about illegal suppression
    else if (global.currentRun eq null) {
      /* In testing, there is no currentRun, and so therefore calling PerRunReporting_isSuppressed below causes a NPE.
       * We still want to be able to test that suppressing work as expected so we put a test hook in here.
       */
      suppressOverrideInTesting(alarm.id)
    } else {
      // using alarm.id.sn rather than just alarm.toString here because all we want to support is @nowarn("msg=17001")
      val asMessage = Reporting.Message.Plain(pos, alarm.id.sn.toString, Reporting.WarningCategory.Other, site = "")
      val suppressed = PerRunReporting_isSuppressed.invoke(global.runReporting, asMessage).asInstanceOf[Boolean]
      if (suppressed && alarm.scalaMandatory) {
        // If we report this as an error, we'll abort before showing the actual illegal suppression, so make it a warning.
        global.reporter.warning(pos, s"Optimus: cannot suppress mandatory alarm #${alarm.id.sn}")
        false
      } else suppressed
    }
  }

  // TODO (OPTIMUS-51339): Remove when OBT loads the new list by itself.
  // Allow for new alarms (signified by "[NEW]" in the message) to not immediately fail the build
  private def isNew(alarm: OptimusPluginAlarm): Boolean = alarm.toString().contains(OptimusAlarms.NewTag)

  def alarm(builder: OptimusAlarmBuilder0, pos: Position): Unit = alarm(builder(), pos)
  def alarm(builder: OptimusAlarmBuilder1, pos: Position, arg: Any): Unit = alarm(builder(arg), pos)
  def alarm(builder: OptimusAlarmBuilder2, pos: Position, arg1: Any, arg2: Any): Unit =
    alarm(builder(arg1, arg2), pos)
  def alarm(builder: OptimusAlarmBuilder3, pos: Position, arg1: Any, arg2: Any, arg3: Any): Unit =
    alarm(builder(arg1, arg2, arg3), pos)
  def alarm(builder: OptimusAlarmBuilder4, pos: Position, arg1: Any, arg2: Any, arg3: Any, arg4: Any): Unit =
    alarm(builder(arg1, arg2, arg3, arg4), pos)
  def alarm(builder: OptimusAlarmBuilder5, pos: Position, arg1: Any, arg2: Any, arg3: Any, arg4: Any, arg5: Any): Unit =
    alarm(builder(arg1, arg2, arg3, arg4, arg5), pos)

  def alarm(builder: OptimusAlarmBuilder0, pos: Position, alarmed: mutable.Set[(Int, Position)]): Unit =
    if (dedup(alarmed, builder, pos))
      alarm(builder(), pos)
  def alarm(builder: OptimusAlarmBuilder1, pos: Position, alarmed: mutable.Set[(Int, Position)], arg: Any): Unit =
    if (dedup(alarmed, builder, pos))
      alarm(builder(arg), pos)
  def alarm(
      builder: OptimusAlarmBuilder2,
      pos: Position,
      alarmed: mutable.Set[(Int, Position)],
      arg1: Any,
      arg2: Any): Unit =
    if (dedup(alarmed, builder, pos))
      alarm(builder(arg1, arg2), pos)
  def alarm(
      builder: OptimusAlarmBuilder3,
      pos: Position,
      alarmed: mutable.Set[(Int, Position)],
      arg1: Any,
      arg2: Any,
      arg3: Any): Unit =
    if (dedup(alarmed, builder, pos))
      alarm(builder(arg1, arg2, arg3), pos)
  def alarm(
      builder: OptimusAlarmBuilder4,
      pos: Position,
      alarmed: mutable.Set[(Int, Position)],
      arg1: Any,
      arg2: Any,
      arg3: Any,
      arg4: Any): Unit =
    if (dedup(alarmed, builder, pos))
      alarm(builder(arg1, arg2, arg3, arg4), pos)
  def alarm(
      builder: OptimusAlarmBuilder5,
      pos: Position,
      alarmed: mutable.Set[(Int, Position)],
      arg1: Any,
      arg2: Any,
      arg3: Any,
      arg4: Any,
      arg5: Any): Unit =
    if (dedup(alarmed, builder, pos))
      alarm(builder(arg1, arg2, arg3, arg4, arg5), pos)

  def internalErrorAbort(pos: Position, msg: String): Nothing = {
    global.reporter.error(pos, s"Optimus internal error. Please contact the graph team: $msg")
    global.abort(s"Optimus internal error. Please contact the graph team: $msg")
  }

  /**
   * Creates a passable approximation of an expression which will create this tree. Useful to use with
   * [[internalErrorAbort]] for when they ask for help, for some value of "they".
   */
  def printRaw(tree: global.Tree): String = {
    import java.io._
    val result = new StringWriter()
    global.newRawTreePrinter(new PrintWriter(result)).print(tree)
    result.toString
  }

  def debug(pos: Position, msg: => String): Unit =
    if (pluginData.alarmConfig.debug) global.reporter.echo(pos, s"$phaseInfo: $msg")
}

object OptimusPluginReporter {
  val PerRunReporting_isSuppressed: jlr.Method = reflect.ensureAccessible {
    classOf[Reporting#PerRunReporting].getDeclaredMethod("isSuppressed", classOf[Reporting.Message])
  }
}
