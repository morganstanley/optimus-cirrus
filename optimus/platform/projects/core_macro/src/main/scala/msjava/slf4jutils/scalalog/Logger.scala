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
package msjava.slf4jutils.scalalog

import ch.qos.logback.core.AppenderBase
import optimus.utils.MacroUtils
import ch.qos.logback.classic.spi.ILoggingEvent
import org.slf4j.{Marker, Logger => SLF4JLogger}

/**
 * Adapter for the [[org.slf4j.Logger]] interface, making it more usable from scala.
 *
 * Implemented using macros so that the message and any arguments are evaluated only if the corresponding target is
 * enabled.
 *
 * This means that you can write things like
 *
 * log.debug(s"my message: $hugeObject")
 *
 * and pay almost zero runtime cost if log.isDebugEnabled is false. We skip the isEnabled check in the case of a single
 * constant string literal argument, with or without exception (e.g. log.info("hello world") or log.error("oh no!", ex))
 * to avoid unnecessary code generation with no runtime benefit.
 *
 * There is no benefit to using the log4j style string interpolators ("my message: {}"). They are retained only for
 * backward compatibility. For all new code, just use s-string interpolation as in the above example.
 *
 * (Note that even though we only have a varargs version of the interpolating log methods, the macro does actually call
 * the more efficient 1 or 2 arg versions where possible, and this is tested for in LoggingMacrosTest)
 */
trait Intercept {
  def enable: Boolean
  def capture(msg: String): Unit

}
class Logger(
    val javaLogger: SLF4JLogger /* exposed for java clients which can't call macros */,
    trace: Option[Intercept] = None) {
  require(javaLogger != null, "javaLogger cannot be null")
  def getName = javaLogger.getName

  def overrideEnable: Boolean = trace.exists(_.enable)
  for (intercept <- trace) {
    javaLogger match {
      case logger: ch.qos.logback.classic.Logger =>
        val appender = new AppenderBase[ILoggingEvent] {
          override def append(eventObject: ILoggingEvent): Unit = {
            intercept.capture(eventObject.getFormattedMessage)
          }
          override def doAppend(eventObject: ILoggingEvent): Unit = append(eventObject)
        }
        logger.addAppender(appender)
    }
  }

  // --- error ---
  def isErrorEnabled() = javaLogger.isErrorEnabled
  def error(msg: String): Unit = macro LoggerMacros.error
  def error(formatString: String, throwable: Throwable): Unit = macro LoggerMacros.errorThrowable
  def error(formatString: String, interpolates: Any*): Unit = macro LoggerMacros.errorVarArgs

  def isErrorEnabled(marker: Marker) = javaLogger.isErrorEnabled(marker)
  def error(marker: Marker, msg: String): Unit = macro LoggerMacros.errorMarker
  def error(marker: Marker, formatString: String, throwable: Throwable): Unit = macro LoggerMacros.errorMarkerThrowable
  def error(marker: Marker, formatString: String, interpolates: Any*): Unit = macro LoggerMacros.errorMarkerVarArgs

  // --- warn---
  def isWarnEnabled() = javaLogger.isWarnEnabled
  def warn(msg: String): Unit = macro LoggerMacros.warn
  def warn(formatString: String, throwable: Throwable): Unit = macro LoggerMacros.warnThrowable
  def warn(formatString: String, interpolates: Any*): Unit = macro LoggerMacros.warnVarArgs

  def isWarnEnabled(marker: Marker) = javaLogger.isWarnEnabled(marker)
  def warn(marker: Marker, msg: String): Unit = macro LoggerMacros.warnMarker
  def warn(marker: Marker, formatString: String, throwable: Throwable): Unit = macro LoggerMacros.warnMarkerThrowable
  def warn(marker: Marker, formatString: String, interpolates: Any*): Unit = macro LoggerMacros.warnMarkerVarArgs

  // --- info ---
  def isInfoEnabled() = javaLogger.isInfoEnabled
  def info(msg: String): Unit = macro LoggerMacros.info
  def info(formatString: String, throwable: Throwable): Unit = macro LoggerMacros.infoThrowable
  def info(formatString: String, interpolates: Any*): Unit = macro LoggerMacros.infoVarArgs

  def isInfoEnabled(marker: Marker) = javaLogger.isInfoEnabled(marker)
  def info(marker: Marker, msg: String): Unit = macro LoggerMacros.infoMarker
  def info(marker: Marker, formatString: String, throwable: Throwable): Unit = macro LoggerMacros.infoMarkerThrowable
  def info(marker: Marker, formatString: String, interpolates: Any*): Unit = macro LoggerMacros.infoMarkerVarArgs

  // --- debug ---
  def isDebugEnabled() = javaLogger.isDebugEnabled
  def debug(msg: String): Unit = macro LoggerMacros.debug
  def debug(formatString: String, throwable: Throwable): Unit = macro LoggerMacros.debugThrowable
  def debug(formatString: String, interpolates: Any*): Unit = macro LoggerMacros.debugVarArgs

  def isDebugEnabled(marker: Marker) = javaLogger.isDebugEnabled(marker)
  def debug(marker: Marker, msg: String): Unit = macro LoggerMacros.debugMarker
  def debug(marker: Marker, formatString: String, throwable: Throwable): Unit = macro LoggerMacros.debugMarkerThrowable
  def debug(marker: Marker, formatString: String, interpolates: Any*): Unit = macro LoggerMacros.debugMarkerVarArgs

  // --- trace ---
  def isTraceEnabled() = javaLogger.isTraceEnabled
  def trace(msg: String): Unit = macro LoggerMacros.trace
  def trace(formatString: String, throwable: Throwable): Unit = macro LoggerMacros.traceThrowable
  def trace(formatString: String, interpolates: Any*): Unit = macro LoggerMacros.traceVarArgs

  def isTraceEnabled(marker: Marker) = javaLogger.isTraceEnabled(marker)
  def trace(marker: Marker, msg: String): Unit = macro LoggerMacros.traceMarker
  def trace(marker: Marker, formatString: String, throwable: Throwable): Unit = macro LoggerMacros.traceMarkerThrowable
  def trace(marker: Marker, formatString: String, interpolates: Any*): Unit = macro LoggerMacros.traceMarkerVarArgs

}

object LoggerMacros {
  import scala.reflect.macros.blackbox.Context

  // --- error ---
  def error(c: Context)(msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Error", None, msg :: Nil, Nil)
  def errorThrowable(c: Context)(formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Error", None, formatString :: throwable :: Nil, Nil)
  def errorVarArgs(c: Context)(formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Error", None, formatString :: Nil, interpolates)

  def errorMarker(c: Context)(marker: c.Expr[Marker], msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Error", Some(marker), msg :: Nil, Nil)
  def errorMarkerThrowable(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Error", Some(marker), formatString :: throwable :: Nil, Nil)
  def errorMarkerVarArgs(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Error", Some(marker), formatString :: Nil, interpolates)

  // --- warn ---
  def warn(c: Context)(msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Warn", None, msg :: Nil, Nil)
  def warnThrowable(c: Context)(formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Warn", None, formatString :: throwable :: Nil, Nil)
  def warnVarArgs(c: Context)(formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Warn", None, formatString :: Nil, interpolates)

  def warnMarker(c: Context)(marker: c.Expr[Marker], msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Warn", Some(marker), msg :: Nil, Nil)
  def warnMarkerThrowable(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Warn", Some(marker), formatString :: throwable :: Nil, Nil)
  def warnMarkerVarArgs(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Warn", Some(marker), formatString :: Nil, interpolates)

  // --- info ---
  def info(c: Context)(msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Info", None, msg :: Nil, Nil)
  def infoThrowable(c: Context)(formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Info", None, formatString :: throwable :: Nil, Nil)
  def infoVarArgs(c: Context)(formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Info", None, formatString :: Nil, interpolates)

  def infoMarker(c: Context)(marker: c.Expr[Marker], msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Info", Some(marker), msg :: Nil, Nil)
  def infoMarkerThrowable(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Info", Some(marker), formatString :: throwable :: Nil, Nil)
  def infoMarkerVarArgs(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Info", Some(marker), formatString :: Nil, interpolates)

  // --- debug ---
  def debug(c: Context)(msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Debug", None, msg :: Nil, Nil)
  def debugThrowable(c: Context)(formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Debug", None, formatString :: throwable :: Nil, Nil)
  def debugVarArgs(c: Context)(formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Debug", None, formatString :: Nil, interpolates)

  def debugMarker(c: Context)(marker: c.Expr[Marker], msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Debug", Some(marker), msg :: Nil, Nil)
  def debugMarkerThrowable(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Debug", Some(marker), formatString :: throwable :: Nil, Nil)
  def debugMarkerVarArgs(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Debug", Some(marker), formatString :: Nil, interpolates)

  // --- trace ---
  def trace(c: Context)(msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Trace", None, msg :: Nil, Nil)
  def traceThrowable(c: Context)(formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Trace", None, formatString :: throwable :: Nil, Nil)
  def traceVarArgs(c: Context)(formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Trace", None, formatString :: Nil, interpolates)

  def traceMarker(c: Context)(marker: c.Expr[Marker], msg: c.Expr[String]): c.Expr[Unit] =
    guardedLog(c)("Trace", Some(marker), msg :: Nil, Nil)
  def traceMarkerThrowable(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], throwable: c.Expr[Throwable]): c.Expr[Unit] =
    guardedLog(c)("Trace", Some(marker), formatString :: throwable :: Nil, Nil)
  def traceMarkerVarArgs(
      c: Context)(marker: c.Expr[Marker], formatString: c.Expr[String], interpolates: c.Expr[Any]*): c.Expr[Unit] =
    guardedLog(c)("Trace", Some(marker), formatString :: Nil, interpolates)

  /**
   * generates a guarded log call AST like this:
   *
   * if (this.isLevelEnabled(markerArgs) this.level(markerArgs args interpArgs)
   *
   * There is no guard in the case of a string literal argument with no interpolation parameters
   *
   * The markerArgs and rawArgs are inserted as-is, but the interpArgs have their type forced to Any (if two or fewer)
   * or Object (if three or more) to force scalac to resolve the correct varargs vs. regular java logger methods.
   */
  def guardedLog(c: Context)(
      level: String,
      markerArg: Option[c.Expr[Any]],
      rawArgs: Seq[c.Expr[Any]],
      interpArgs: Seq[c.Expr[Any]]): c.Expr[Unit] = {
    import c.universe._

    def delegate = Select(c.prefix.tree, TermName("javaLogger"))

    val markerTree = markerArg.map(_.tree).toList
    val guardMethodCall = Apply(Select(delegate, TermName(s"is${level}Enabled")), markerTree)
    val doEnable = q"(${c.prefix.tree}.overrideEnable || $guardMethodCall)"

    val rawArgTrees: List[Tree] = rawArgs.iterator.map(_.tree).toList

    // we force the type of the interpolation arguments as Object or Any depending on the number so that Scala
    // resolves to the 1-arg, 2-arg or varargs version of the logger method unambiguously
    val targetType = TypeTree(if (interpArgs.size > 2) typeOf[Object] else typeOf[Any])
    val interpArgTrees: List[Tree] =
      interpArgs.iterator.map(e => TypeApply(Select(e.tree, TermName("asInstanceOf")), targetType :: Nil)).toList

    val logMethodCall =
      Apply(Select(delegate, TermName(level.toLowerCase)), markerTree ::: rawArgTrees ::: interpArgTrees)

    // we can skip the guard if it's a constant string message
    val isConstantString = rawArgTrees.head match {
      case Literal(Constant(_)) => true
      case _                    => false
    }

    if (isConstantString && interpArgs.isEmpty) {
      MacroUtils.typecheckAndValidateExpr(c)(c.Expr[Unit](logMethodCall))
    } else {
      MacroUtils.typecheckAndValidateExpr(c)(c.Expr[Unit] { If(doEnable, logMethodCall, EmptyTree) })
    }
  }
}
