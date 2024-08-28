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
package optimus.graph.diagnostics

import optimus.graph.diagnostics.gridprofiler.ProfilerResult

import scala.reflect.macros.blackbox.Context
import optimus.utils.MacroUtils.splice
import optimus.utils.MacroUtils.typecheckAndValidate

object ProfileUtil {

  /**
   * enters a new unique profiler scope, records the duration and call count of the enclosed block as a custom counter
   * with name profileKeyPrefix, and returns the profiler result
   */
  def profileTimeAndCount[T](key: String)(block: T): ProfilerResult[T] = macro ProfileUtilMacros.profileTimeAndCountImpl

  /**
   * records the duration and call count of the enclosed block as a custom counter with name profileKeyPrefix (does not
   * enter a new profiler scope)
   */
  def recordTimeAndCount[T](key: String)(block: T): T = macro ProfileUtilMacros.recordTimeAndCountImpl

  def prettyPrintCustomCounters(result: ProfilerResult[_], separator: String = "\n"): String = {
    val counters = result.flat.customCounters

    if (counters.nonEmpty) {
      counters.toList
        .sortBy { case (k, v) => k.toString }
        .map {
          case (k, v: Long) => f"$k = $v%,d"
          case (k, v: Int)  => f"$k = $v%,d"
          case (k, v)       => s"$k = $v"
        }
        .mkString(separator)
    } else "[No profiler custom counter data found; use '--profile-custom-metrics all' to enable]"
  }
}

private class ProfileUtilMacros(val c: Context) {
  import c.universe._
  val gpPackage = q"_root_.optimus.graph.diagnostics.gridprofiler"
  val gp = q"$gpPackage.GridProfiler"

  def profileTimeAndCountImpl(key: c.Tree)(block: c.Tree): c.Tree = {
    // we want a new unique profiler scope every time we enter one of these blocks, otherwise the results
    // get summed up with all other blocks of the same scope name which have run in this process
    typecheckAndValidate(c)(q"""
      $gp.profiled(scope = _root_.java.util.UUID.randomUUID().toString) {
        ${recordTimeAndCountImpl(key)(block)}
      }
    """)
  }

  def recordTimeAndCountImpl(key: c.Tree)(block: c.Tree): c.Tree = {
    val result = c.freshName(TermName("result"))
    val time = c.freshName(TermName("time"))
    typecheckAndValidate(c)(q"""
      val ($time, $result) = _root_.optimus.platform.AdvancedUtils.timed { ${splice(c)(block)} }
      $gp.recordCustomCounter($key, $gp.TimeAndCount($time, 1))
      $result
    """)
  }
}
