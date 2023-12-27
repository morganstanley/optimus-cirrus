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
package optimus.buildtool.compilers.zinc

import java.util.function.Supplier
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.compilers.zinc.reporter.ZincReporter
import optimus.buildtool.config.ScopeId
import optimus.buildtool.trace.MessageTrace
import sbt.util.Level
import sbt.util.{Logger => SbtLogger}
import xsbti.CompileCancelled

import scala.collection.immutable.Seq

private class CancellationException extends CompileCancelled {
  override def arguments(): Array[String] = Array.empty
}

object ZincLogger {
  private val slog = msjava.slf4jutils.scalalog.getLogger(this)
}

private[zinc] trait ZincLogger extends SbtLogger {
  import ZincLogger.slog

  private val ls = System.getProperty("line.separator")
  protected def prefix: String

  private var isCancelled = false
  def cancel() = {
    log(Level.Warn, "Cancellation requested")
    isCancelled = true
  }

  override def success(message: => String): Unit = {
    log(Level.Info, s"Success: $message")
  }

  private val truncationExemptions = Seq("Found differing")
  private def foundTruncationExemption(msg: String): Boolean = {
    // we only look at the first chars of the message for performance reasons
    // as its size can be very big!
    val beginningOfMsg = msg.take(50)
    truncationExemptions.exists(beginningOfMsg.contains)
  }

  private def format(m: String): Seq[String] = {
    val maxLength = if (slog.isTraceEnabled()) Int.MaxValue else if (slog.isDebugEnabled()) 10000 else 500
    val trunc =
      if (m.length > maxLength && !foundTruncationExemption(m)) m.substring(0, maxLength) + "..."
      else m
    trunc.split(ls).toIndexedSeq.map(l => s"$prefix$l")
  }

  // SBT logger doesn't for some reason has tracing only for exceptions, and we can't overload to add the
  // string version because of erasure.
  override def trace(t: => Throwable): Unit = slog.trace("Exception from zinc", t)
  override def trace(t: Supplier[Throwable]): Unit = trace(t.get)
  def traceMessage(message: => String): Unit = {
    if (slog.isTraceEnabled()) format(message).foreach(slog.trace(_))
  }

  override def log(level: Level.Value, message: => String): Unit = {
    level match {
      case Level.Error =>
        if (slog.isErrorEnabled())
          format(message).foreach(slog.error(_)) // Want this on a separate line for debugging!
      case Level.Warn  => if (slog.isWarnEnabled()) format(message).foreach(slog.warn(_))
      case Level.Debug => if (slog.isDebugEnabled()) format(message).foreach(slog.debug(_))
      case _           => if (slog.isInfoEnabled()) format(message).foreach(slog.info(_))
    }
    if (isCancelled)
      throw new CancellationException
  }
}

private[zinc] class ZincGeneralLogger extends ZincLogger {
  override protected def prefix: String = "[zinc] "
}
private[zinc] class ZincScopeLogger(scopeId: ScopeId, traceType: MessageTrace) extends ZincLogger {
  override protected val prefix = s"[${scopeId.toString}:${traceType.name}-zinc] "
}

private[zinc] class ZincScopeLoggerWithErrorForwarding(
    reporter: ZincReporter,
    scopeId: ScopeId,
    traceType: MessageTrace)
    extends ZincScopeLogger(scopeId, traceType) {
  override def log(level: Level.Value, message: => String): Unit = {
    val m = message
    // Report error-level log messages as compilation errors for better visibility.
    // Note that the reporter will also send messages to its underlying logger, so to avoid duplication we don't log
    // errors, we only report them. Also note that error messages starting with "## Exception" shouldn't be reported
    // since Zinc actually throws them up to us and we handle them separately, so we just log them
    if (level == Level.Error && !m.startsWith("## Exception"))
      reporter.addMessage(CompilationMessage.error(m))
    else
      super.log(level, m)
  }
}
