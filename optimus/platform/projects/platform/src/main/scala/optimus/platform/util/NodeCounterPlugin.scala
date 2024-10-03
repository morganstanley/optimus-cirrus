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
package optimus.platform.util

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import optimus.graph.NodeTask
import optimus.graph.OGSchedulerContext
import optimus.graph.PropertyInfo
import optimus.graph.SchedulerPlugin
import optimus.platform.NonForwardingPluginTagKey

class NodeCounterPlugin extends SchedulerPlugin {
  private val counter = new AtomicInteger
  override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    val tag = n.scenarioStack().findPluginTag(NodeCounterPlugin.TagKey)
    if (tag.nonEmpty) {
      tag.get.find(v => v._1.getPlugin(n.scenarioStack()) == this).map(z => z._2.incrementAndGet())
    }
    counter.incrementAndGet()
    false
  }
  def count: Int = counter.get()
  def reset(): Unit = counter.set(0)
}

object NodeCounterPlugin {
  object TagKey extends NonForwardingPluginTagKey[Seq[(PropertyInfo[_], AtomicInteger)]]
}

class NodeCounterPluginWithWait(val latch: CountDownLatch) extends NodeCounterPlugin {
  override def adapt(n: NodeTask, ec: OGSchedulerContext): Boolean = {
    latch.await()
    super.adapt(n, ec)
  }
}
