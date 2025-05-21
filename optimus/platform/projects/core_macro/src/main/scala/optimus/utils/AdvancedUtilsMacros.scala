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
package optimus.utils

import optimus.utils.MacroUtils.splice
import optimus.utils.MacroUtils.typecheckAndValidate

import scala.reflect.macros.blackbox.Context
import msjava.slf4jutils.scalalog.Logger
import java.time.Duration

class AdvancedUtilsMacros(val c: Context) {
  import c.universe._
  final def timedImpl(f: c.Tree): c.Tree = {
    val startTime = c.freshName(TermName("startTime"))
    val result = c.freshName(TermName("result"))

    typecheckAndValidate(c)(q"""
      val $startTime = _root_.optimus.graph.OGTrace.nanoTime()
      val $result = ${splice(c)(f)}
      _root_.optimus.platform.AdvancedUtils.timedValueSink($startTime, $result)
    """)
  }

  final def traceLatencyWithDefaultingArgsImpl(log: c.Expr[Logger], action: c.Tree)(f: c.Tree): c.Tree = {
    val infoThresholdInNanos = c.Expr[Long](q"50_000_000L")
    val warnThresholdInNanos = c.Expr[Long](q"100_000_000L")

    traceLatencyImpl(log, action, infoThresholdInNanos, warnThresholdInNanos)(f)
  }

  final def traceLatencyWithDurationImpl(
      log: c.Expr[Logger],
      action: c.Tree,
      infoThreshold: c.Expr[Duration],
      warnThreshold: c.Expr[Duration])(f: c.Tree): c.Tree = {
    val infoThresholdInNanos = c.Expr[Long](q"$infoThreshold.toNanos")
    val warnThresholdInNanos = c.Expr[Long](q"$warnThreshold.toNanos")
    traceLatencyImpl(log, action, infoThresholdInNanos, warnThresholdInNanos)(f)
  }

  final def traceLatencyImpl(
      log: c.Expr[Logger],
      action: c.Tree,
      infoThreshold: c.Expr[Long],
      warnThreshold: c.Expr[Long])(f: c.Tree): c.Tree = {
    val durationInNanos = c.freshName(TermName("durationInNanos"))
    val result = c.freshName(TermName("result"))
    typecheckAndValidate(c)(q"""
      val ($durationInNanos, $result) = ${timedImpl(f)}
      _root_.optimus.platform.AdvancedUtils.doLog($log, $durationInNanos, $action, $infoThreshold, $warnThreshold)
      $result
    """)
  }

}
