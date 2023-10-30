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
package optimus.platform.internal

import optimus.tools.scalacplugins.entity.PluginDataAccessFromReflection
import optimus.tools.scalacplugins.entity.StagingPhase
import optimus.tools.scalacplugins.entity.reporter._
import language.reflectiveCalls

import scala.reflect.macros.blackbox.Context
import scala.reflect.macros.contexts

trait WithOptimusMacroReporter {
  val c: Context

  import c.universe.Position

  // error report helper methods without position
  @inline final def abort(builder: OptimusMacroAlarmBuilder0) =
    OptimusReporter.abortImpl(c)(builder(), c.macroApplication.pos)
  @inline final def abort(builder: OptimusMacroAlarmBuilder1, arg: Any) =
    OptimusReporter.abortImpl(c)(builder(arg), c.macroApplication.pos)
  @inline final def abort(builder: OptimusMacroAlarmBuilder2, arg1: Any, arg2: Any) =
    OptimusReporter.abortImpl(c)(builder(arg1, arg2), c.macroApplication.pos)
  @inline final def abort(builder: OptimusMacroAlarmBuilder3, arg1: Any, arg2: Any, arg3: Any) =
    OptimusReporter.abortImpl(c)(builder(arg1, arg2, arg3), c.macroApplication.pos)

  @inline final def error(builder: OptimusMacroAlarmBuilder0) =
    OptimusReporter.errorImpl(c)(builder(), c.macroApplication.pos)
  @inline final def error(builder: OptimusMacroAlarmBuilder1, arg: Any) =
    OptimusReporter.errorImpl(c)(builder(arg), c.macroApplication.pos)
  @inline final def error(builder: OptimusMacroAlarmBuilder2, arg1: Any, arg2: Any) =
    OptimusReporter.errorImpl(c)(builder(arg1, arg2), c.macroApplication.pos)

  @inline final def alarm(builder: OptimusMacroAlarmBuilder0): Unit =
    OptimusReporter.alarmImpl(c)(builder(), c.macroApplication.pos, false)
  @inline final def alarm(builder: OptimusMacroAlarmBuilder1, arg: Any): Unit =
    OptimusReporter.alarmImpl(c)(builder(arg), c.macroApplication.pos, false)
  @inline final def alarm(builder: OptimusMacroAlarmBuilder2, arg1: Any, arg2: Any): Unit =
    OptimusReporter.alarmImpl(c)(builder(arg1, arg2), c.macroApplication.pos, false)

  // error report helper methods with position
  @inline final def abort(builder: OptimusMacroAlarmBuilder0, pos: Position) =
    OptimusReporter.abortImpl(c)(builder(), pos)
  @inline final def abort(builder: OptimusMacroAlarmBuilder1, pos: Position, arg: Any) =
    OptimusReporter.abortImpl(c)(builder(arg), pos)
  @inline final def abort(builder: OptimusMacroAlarmBuilder2, pos: Position, arg1: Any, arg2: Any) =
    OptimusReporter.abortImpl(c)(builder(arg1, arg2), pos)

  @inline final def error(builder: OptimusMacroAlarmBuilder0, pos: Position) =
    OptimusReporter.errorImpl(c)(builder(), pos)
  @inline final def error(builder: OptimusMacroAlarmBuilder1, pos: Position, arg: Any) =
    OptimusReporter.errorImpl(c)(builder(arg), pos)
  @inline final def error(builder: OptimusMacroAlarmBuilder2, pos: Position, arg1: Any, arg2: Any) =
    OptimusReporter.errorImpl(c)(builder(arg1, arg2), pos)

  @inline final def alarm(builder: OptimusMacroAlarmBuilder0, pos: Position): Unit =
    OptimusReporter.alarmImpl(c)(builder(), pos, false)
  @inline final def alarm(builder: OptimusMacroAlarmBuilder1, pos: Position, arg: Any): Unit =
    OptimusReporter.alarmImpl(c)(builder(arg), pos, false)
  @inline final def alarm(builder: OptimusMacroAlarmBuilder2, pos: Position, arg1: Any, arg2: Any): Unit =
    OptimusReporter.alarmImpl(c)(builder(arg1, arg2), pos, false)
  @inline final def alarm(builder: OptimusMacroAlarmBuilder3, pos: Position, arg1: Any, arg2: Any, arg3: Any): Unit =
    OptimusReporter.alarmImpl(c)(builder(arg1, arg2, arg3), pos, false)
  @inline final def alarm(
      builder: OptimusMacroAlarmBuilder4,
      pos: Position,
      arg1: Any,
      arg2: Any,
      arg3: Any,
      arg4: Any): Unit = OptimusReporter.alarmImpl(c)(builder(arg1, arg2, arg3, arg4), pos, false)
  @inline final def alarm(
      builder: OptimusMacroAlarmBuilder5,
      pos: Position,
      arg1: Any,
      arg2: Any,
      arg3: Any,
      arg4: Any,
      arg5: Any): Unit = OptimusReporter.alarmImpl(c)(builder(arg1, arg2, arg3, arg4, arg5), pos, false)
  @inline final def alarm(
      builder: OptimusMacroAlarmBuilder6,
      pos: Position,
      arg1: Any,
      arg2: Any,
      arg3: Any,
      arg4: Any,
      arg5: Any,
      arg6: Any): Unit = OptimusReporter.alarmImpl(c)(builder(arg1, arg2, arg3, arg4, arg5, arg6), pos, false)
}

object OptimusReporter {

  private[internal] def alarmImpl(
      c: Context)(alarm: OptimusMacroAlarm, position: c.universe.Position, force: Boolean): Unit = {

    def pluginDataAccess = c match {
      case context: contexts.Context => {
        // we have to access this reflectively as it is typically loaded in a different classloader
        // it is also essential that all of the passed types are simple types in the parent classloader
        // e.g. String, Int etc
        context.global
          .findPhaseWithName(StagingPhase.names.optimus_staging)
          .asInstanceOf[PluginDataAccessFromReflection.PluginDataAccess]
      }
      case _ => throw new IllegalStateException(s"OptimusReporter failed ${c.getClass}")
    }
    if (pluginDataAccess.silence(alarm.id.sn)) return
    val msg = alarm.toString()
    alarm.id.tpe match {
      case OptimusAlarmType.ERROR   => c.error(position, msg)
      case OptimusAlarmType.WARNING => c.warning(position, msg)
      case OptimusAlarmType.INFO    => c.info(position, msg, force)
      case OptimusAlarmType.DEBUG   => if (pluginDataAccess.debugMessages) c.info(position, msg, force)
      case OptimusAlarmType.SILENT  => // ignore it
      case unexpected               => throw new IllegalStateException(s"unexpected level $unexpected")
    }
  }

  private[internal] def abortImpl(c: Context)(opalarm: OptimusMacroAlarm, position: c.universe.Position): Nothing = {
    assert(opalarm.isAbort, "Optimus macro internal error: abort expected")
    c.abort(position, opalarm.toString)
  }

  private[internal] def errorImpl(c: Context)(opalarm: OptimusMacroAlarm, position: c.universe.Position): Unit = {
    assert(opalarm.isError, "Optimus macro internal error: error expected")
    c.error(position, opalarm.toString())
  }

  // handle abort differently, because this means we don't want user to control these errors
  def abort(c: Context, builder: OptimusMacroAlarmBuilder0)(pos: c.universe.Position) = abortImpl(c)(builder(), pos)
  def abort(c: Context, builder: OptimusMacroAlarmBuilder1)(pos: c.universe.Position, arg: Any) =
    abortImpl(c)(builder(arg), pos)
  def abort(c: Context, builder: OptimusMacroAlarmBuilder2)(pos: c.universe.Position, arg1: Any, arg2: Any) =
    abortImpl(c)(builder(arg1, arg2), pos)

  // provide an error method which can be used in the places we don't want user to control these errors
  def error(c: Context, builder: OptimusMacroAlarmBuilder0)(pos: c.universe.Position) = errorImpl(c)(builder(), pos)
  def error(c: Context, builder: OptimusMacroAlarmBuilder1)(pos: c.universe.Position, arg: Any) =
    errorImpl(c)(builder(arg), pos)
  def error(c: Context, builder: OptimusMacroAlarmBuilder2)(pos: c.universe.Position, arg1: Any, arg2: Any) =
    errorImpl(c)(builder(arg1, arg2), pos)
  def error(c: Context, builder: OptimusMacroAlarmBuilder3)(pos: c.universe.Position, arg1: Any, arg2: Any, arg3: Any) =
    errorImpl(c)(builder(arg1, arg2, arg3), pos)

  // this API is used to allow user to config the error/warning
  def alarm(c: Context, builder: OptimusMacroAlarmBuilder0)(pos: c.universe.Position): Unit =
    alarmImpl(c)(builder(), pos, false)
  def alarm(c: Context, builder: OptimusMacroAlarmBuilder1)(pos: c.universe.Position, arg: Any): Unit =
    alarmImpl(c)(builder(arg), pos, false)
  def alarm(c: Context, builder: OptimusMacroAlarmBuilder2)(pos: c.universe.Position, arg1: Any, arg2: Any): Unit =
    alarmImpl(c)(builder(arg1, arg2), pos, false)
  def alarm(
      c: Context,
      builder: OptimusMacroAlarmBuilder3)(pos: c.universe.Position, arg1: Any, arg2: Any, arg3: Any): Unit =
    alarmImpl(c)(builder(arg1, arg2, arg3), pos, false)
  def alarm(
      c: Context,
      builder: OptimusMacroAlarmBuilder4)(pos: c.universe.Position, arg1: Any, arg2: Any, arg3: Any, arg4: Any): Unit =
    alarmImpl(c)(builder(arg1, arg2, arg3, arg4), pos, false)
  def alarm(c: Context, builder: OptimusMacroAlarmBuilder5)(
      pos: c.universe.Position,
      arg1: Any,
      arg2: Any,
      arg3: Any,
      arg4: Any,
      arg5: Any): Unit = alarmImpl(c)(builder(arg1, arg2, arg3, arg4, arg5), pos, false)
  def alarm(c: Context, builder: OptimusMacroAlarmBuilder6)(
      pos: c.universe.Position,
      arg1: Any,
      arg2: Any,
      arg3: Any,
      arg4: Any,
      arg5: Any,
      arg6: Any): Unit = alarmImpl(c)(builder(arg1, arg2, arg3, arg4, arg5, arg6), pos, false)
}
