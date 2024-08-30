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
package optimus.platform.runtime

import optimus.utils.PropertyUtils
import optimus.scalacompat.collection._

trait ZkOpsTimer {

  def timed[T](ops: String)(f: => T): T

  def isEmpty: Boolean

  def summary: Map[String, String]
}

private class ZkOpsTimerDefaultImpl() extends ZkOpsTimer {

  private var timings: Map[String, Long] = Map.empty

  override def timed[T](ops: String)(f: => T): T = {
    val start = System.currentTimeMillis()
    try {
      f
    } finally {
      val elapsed = System.currentTimeMillis() - start
      synchronized {
        timings = timings.updated(ops, elapsed + timings.getOrElse(ops, 0L))
      }
    }
  }

  override def isEmpty = synchronized {
    timings.isEmpty
  }

  override def summary: Map[String, String] = {
    if (isEmpty) Map.empty
    else {
      synchronized {
        val total = timings.values.sum
        (timings + ("_total" -> total)).mapValuesNow(_.toString)
      }
    }
  }
}

object ZkOpsTimer {

  val collectZkOpsMetrics =
    PropertyUtils.get("optimus.platform.runtime.collectZkOpsMetrics", false)

  def getDefault =
    if (collectZkOpsMetrics) {
      new ZkOpsTimerDefaultImpl()
    } else noop

  val noop = new ZkOpsTimer {
    override def timed[T](ops: String)(f: => T): T = f
    override def isEmpty: Boolean = true
    override def summary: Map[String, String] = Map.empty
  }
}
