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
package optimus.core

import com.github.benmanes.caffeine.cache.Caffeine
import optimus.graph.GraphStallInfo
import optimus.graph.NodeTask
import optimus.graph.Scheduler
import optimus.platform.EvaluationContext
import optimus.platform.EvaluationQueue

import java.util
import java.util.Objects
import java.util.concurrent.ConcurrentMap
import scala.collection.mutable

object NodeInfoAppender {

  // Global cache for data to be associated with live NodeTask.
  // Each type of data is assigned an index into the array associated weakly with each ntsk.
  private val infoMap: ConcurrentMap[NodeTask, Array[AnyRef]] =
    Caffeine.newBuilder().weakKeys().build[NodeTask, Array[AnyRef]]().asMap()
  @volatile private var nInfoTypes = 0
  private val infoTypes = mutable.HashMap.empty[Class[_], Accessor[_]]

  /**
   * Register a new type of node data.
   */
  def accessor[T <: AnyRef: Manifest]: Accessor[T] = synchronized {
    val clazz: Class[T] = implicitly[Manifest[T]].runtimeClass.asInstanceOf[Class[T]]
    infoTypes.getOrElseUpdate(
      clazz, {
        nInfoTypes += 1
        new Accessor[T](nInfoTypes - 1)
      })
  }.asInstanceOf[Accessor[T]]

  class Accessor[T <: AnyRef] private[NodeInfoAppender] (id: Int) {
    private def newSize = Math.max(nInfoTypes, id + 1)
    // All access to the array is done within ConcurrentMap#compute methods, and thus under synchronization by the
    // hashmap node.
    def get(ntsk: NodeTask): T = {
      var ret: AnyRef = null
      infoMap.computeIfPresent(
        ntsk,
        (_, arr) => {
          if (id < arr.length)
            ret = arr(id)
          arr
        })
      ret.asInstanceOf[T]
    }
    // Potential optimization: drop entry internally if now completely empty.
    def drop(ntsk: NodeTask): T = {
      set(ntsk, null.asInstanceOf[T])
    }

    def set(ntsk: NodeTask, t: T): T = {
      var old: AnyRef = null
      var newEntry = false
      infoMap.compute(
        ntsk,
        { (_: NodeTask, prev: Array[AnyRef]) =>
          {
            val arr = if (Objects.isNull(prev)) {
              newEntry = true
              new Array[AnyRef](newSize)
            } else if (id >= prev.length) {
              util.Arrays.copyOf(prev, newSize)
            } else prev
            old = arr(id)
            arr(id) = t
            arr
          }
        }
      )
      if (newEntry)
        ntsk.continueWithIfEverRuns((_: EvaluationQueue, n: NodeTask) => infoMap.remove(n), Scheduler.currentOrDefault)
      old.asInstanceOf[T]
    }

    def compute(ntsk: NodeTask, f: (NodeTask, T) => T): T = {
      var ret: AnyRef = null
      var newEntry = false
      infoMap.compute(
        ntsk,
        (ntsk, prev) => {
          val arr = if (Objects.isNull(prev)) {
            newEntry = true
            new Array[AnyRef](newSize)
          } else if (id >= prev.size) {
            util.Arrays.copyOf(prev, newSize)
          } else prev
          ret = f(ntsk, arr(id).asInstanceOf[T])
          arr(id) = ret
          arr
        }
      )
      if (newEntry)
        ntsk.continueWithIfEverRuns((_: EvaluationQueue, n: NodeTask) => infoMap.remove(n), EvaluationContext.current)
      ret.asInstanceOf[T]
    }

  }

}

class NodeInfoAppender[T <: AnyRef] {
  private val nodeInfoMap = Caffeine.newBuilder().weakKeys().build[NodeTask, T]().asMap()

  def attachExtraData(node: NodeTask, associatedData: T): Unit = {
    // first time we add to map attach the continuation to remove it from map
    if (nodeInfoMap.put(node, associatedData) eq null) {
      node.continueWithIfEverRuns((_: EvaluationQueue, n: NodeTask) => nodeInfoMap.remove(n), EvaluationContext.current)
    }
  }

  def getExtraData(node: NodeTask): T = nodeInfoMap.get(node)
  def removeExtraData(node: NodeTask): Unit = nodeInfoMap.remove(node)
}

object StallInfoAppender extends NodeInfoAppender[() => GraphStallInfo] {
  override def attachExtraData(node: NodeTask, associatedData: () => GraphStallInfo): Unit = {
    super.attachExtraData(node, associatedData)
  }
  override def getExtraData(node: NodeTask): () => GraphStallInfo = super.getExtraData(node)
}

object RunCalcDistributedTaskLabelAppender extends NodeInfoAppender[String]
