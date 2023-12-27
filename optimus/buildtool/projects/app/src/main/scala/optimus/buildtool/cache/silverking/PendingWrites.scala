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
package optimus.buildtool.cache.silverking

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import optimus.buildtool.cache.silverking.SilverKingStore.ArtifactKey
import optimus.buildtool.trace.CacheTraceType
import optimus.graph.Node
import optimus.graph.NodeAwaiter
import optimus.graph.NodeTask
import optimus.platform._

import scala.jdk.CollectionConverters._

class PendingWrites {
  private val pendingWrites = ConcurrentHashMap.newKeySet[SilverKingStore#_put$node]()

  private object PendingWriteRemover extends NodeAwaiter {
    override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = pendingWrites.remove(node)
  }

  def add(writeOperation: Node[Unit]): Unit = {
    pendingWrites.add(writeOperation.asInstanceOf[SilverKingStore#_put$node])
    writeOperation.continueOnCurrentContextWith(PendingWriteRemover)
  }

  def remaining: Seq[PendingWrite] = {
    Seq(pendingWrites.asScala.toSeq: _*).map { w =>
      PendingWrite(w, w.key, w.tpe, w.file)
    }
  }

}

final case class PendingWrite(node: Node[Unit], key: ArtifactKey, tpe: CacheTraceType, file: Path)
