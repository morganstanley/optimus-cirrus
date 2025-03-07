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
import scala.reflect.internal.util.SourceFile
import scala.tools.nsc.Global
import scala.tools.nsc.Reporting

trait OptimusPluginReporter {
  val global: Global
  val phaseInfo: OptimusPhaseInfo
  val pluginData: PluginData

  import OptimusPluginReporter._

  def alarm(alarm: OptimusPluginAlarm, pos: Position): Unit = {
    if (pluginData.alarmConfig.silence(alarm.id.sn)) return
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

  private def dedup[A <: OptimusAlarmBuilder, E](elements: mutable.Set[E], elem: E): Boolean = {
    // REMINDER: Set.add(...) returns true if the element was not yet present in the set, false otherwise
    elements.add(elem)
  }

  // override for forcing suppression when testing
  protected[reporter] def suppressOverrideInTesting(msg: AlarmId): Boolean = false

  // Check if `alarm` is suppressed at `pos` without actually issuing it (see the INFO case above)
  private def isLocallySuppressed(alarm: OptimusPluginAlarm, pos: Position): Boolean = {
    if (alarm.toString().contains("@nowarn"))
      false // Very silly to suppress an error that's already about illegal suppression
    else if (global.currentRun eq null) {
      /* In testing, there is no currentRun, and so therefore calling nowarnAction below causes a NPE.
       * We still want to be able to test that suppressing work as expected so we put a test hook in here.
       */
      suppressOverrideInTesting(alarm.id)
    } else {
      // using alarm.id.sn rather than just alarm.toString here because all we want to support is @nowarn("msg=17001")
      val asMessage =
        OptimusReporterCompat.Message(pos, alarm.id.sn.toString, Reporting.WarningCategory.Other, site = "")
      val suppressed = isSuppressed(global, asMessage)
      if (suppressed && alarm.id.tpe != OptimusAlarmType.WARNING) {
        // If we report this as an error, we'll abort before showing the actual illegal suppression, so make it a warning.
        import optimus.tools.scalacplugins.entity.reporter.OptimusAlarmType._
        val article = alarm.id.tpe match {
          case SILENT | DEBUG | WARNING => "a"
          case INFO | ERROR | ABORT     => "an"
        }
        global.reporter.warning(
          pos,
          s"Invalid @nowarn! Optimus: (${alarm.id.sn}) is $article ${alarm.id.tpe} message but only WARNING messages can be suppressed.")
        false
      } else suppressed
    }
  }

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

  def alarmDedup(builder: OptimusAlarmBuilder0, pos: Position, alarmed: mutable.Set[(Int, Position)]): Unit = {
    if (dedup(alarmed, builder.id.sn -> pos)) alarm(builder(), pos)
  }

  def alarmDedup(
      builder: OptimusAlarmBuilder2,
      pos: Position,
      arg1: Any,
      arg2: Any,
      alarmed: mutable.Set[(Int, Position, Any, Any)]): Unit = {
    // using String for arg1 and arg2 as Any may not have a good equality definition!
    val elem = (builder.id.sn, pos, arg1.toString, arg2.toString)
    if (dedup(alarmed, elem)) alarm(builder(arg1, arg2), pos)
  }

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
  import Reporting._

  private val is212 = try {
    Class.forName("scala.collection.TraversableLike")
    true
  } catch {
    case _: ClassNotFoundException => false
  }

  private lazy val PerRunReporting_isSuppressed: jlr.Method = reflect.ensureAccessible {
    classOf[Reporting#PerRunReporting].getDeclaredMethod("isSuppressed", classOf[Reporting.Message])
  }

  private lazy val PerRunReporting_nowarnAction: jlr.Method = reflect.ensureAccessible {
    classOf[Reporting#PerRunReporting].getDeclaredMethod("nowarnAction", classOf[Reporting.Message])
  }

  def nowarnAction(global: Global, msg: Message): Action =
    PerRunReporting_nowarnAction.invoke(global.runReporting, msg).asInstanceOf[Action]

  def isSuppressed(global: Global, msg: Message): Boolean = {
    if (is212) PerRunReporting_isSuppressed.invoke(global.runReporting, msg).asInstanceOf[Boolean]
    else nowarnAction(global, msg) == Reporting.Action.Silent
  }

  private val PerRunReporting_suppressions: jlr.Method = reflect.ensureAccessible {
    classOf[Reporting#PerRunReporting].getDeclaredMethod("suppressions")
  }

  def suppressions(global: Global): mutable.LinkedHashMap[SourceFile, mutable.ListBuffer[Suppression]] =
    PerRunReporting_suppressions
      .invoke(global.runReporting)
      .asInstanceOf[mutable.LinkedHashMap[SourceFile, mutable.ListBuffer[Suppression]]]
}
