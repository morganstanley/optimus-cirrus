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

import optimus.buildtool.utils.Utils
import optimus.platform._
import optimus.platform.throttle.ThrottleState
import optimus.platform.util.Log

import scala.collection.compat._
import scala.collection.mutable

class CompilerThrottle(maxZincCompileBytes: Int, val maxNumZincs: Int) extends Log {

  private val stats = mutable.Buffer[ThrottleState]()

  // Set Int.MaxValue if maxZincCompileBytes is zero so that we can still use the throttle to limit the
  // number of zincs, even if it's not going to limit based on source size
  val zincByteLimit: Int =
    if (maxZincCompileBytes == 0) Int.MaxValue
    else if (maxZincCompileBytes > 0) maxZincCompileBytes
    else (Runtime.getRuntime.maxMemory / -maxZincCompileBytes).toInt

  // Set minWeight so that we guarantee to only ever have up to maxNumZincs running at one time. We
  // do this rather than having a separate number-based throttle so that a large compile can limit the
  // remaining number of compilations, even if they're very small.
  private val zincMinWeight =
    if (maxNumZincs > 0) zincByteLimit / maxNumZincs else 1

  private val zincSizeThrottle =
    if (maxZincCompileBytes == 0 && maxNumZincs == 0) None
    else {
      log.debug(
        s"Creating zinc size throttle. Max size: ${Utils.bytesToString(zincByteLimit)}, max count: ${maxNumZincs}"
      )
      Some(AdvancedUtils.newThrottle(zincByteLimit))
    }

  @async def throttled[T](sizeBytes: Int)(f: NodeFunction0[T]): T = zincSizeThrottle match {
    case Some(st) =>
      val actualWeight = math.max(sizeBytes, zincMinWeight)

      st(
        {
          stats.synchronized(stats += st.getCounters)
          f()
        },
        nodeWeight = actualWeight)
    case None => f()
  }

  def snapStats(): Seq[ThrottleState] = stats.synchronized {
    stats.to(Seq)
  }

  def snapAndResetStats(): Seq[ThrottleState] = stats.synchronized {
    val r = snapStats()
    stats.clear()
    r
  }
}
