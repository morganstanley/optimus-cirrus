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
package optimus.platform.dal.client

import optimus.platform.dal.ClientRequestLimiter
import optimus.platform.dsi.protobufutils.BatchContext

import scala.collection.concurrent.TrieMap

/*
 * CorrelationMap holds BatchContext which contains Nodes waiting for results. Care needs to be taken when using the
 * operations of this class to make sure nodes are completed before the corresponding entry is taken out of the map
 */
private[platform] final class CorrelationMapWithLimiter(limiter: ClientRequestLimiter) {
  private[this] val correlationMap = TrieMap.empty[Int, (BatchContext, Int)]
  def size: Int = correlationMap.size

  def getAllRequestIds: Iterable[String] = correlationMap.map { case (_, (bctx, _)) => bctx.requestUuid }

  def clear(): Unit = {
    // release all locks
    val size = correlationMap.foldLeft(0) { case (acc, (_, (_, size))) => acc + size }
    limiter.release(size, "Clear call")
    correlationMap.clear()
  }

  def +=(tuple: (Int, BatchContext)): CorrelationMapWithLimiter = {
    val (seqId, (batchCtx, noOfCmds)) = tuple match {
      case (s, b) => (s, (b, b.clientRequests.map { _.commands.size }.sum))
    }
    limiter.take(noOfCmds, batchCtx.requestUuid)
    correlationMap += ((seqId, (batchCtx, noOfCmds)))
    this
  }

  def get(seqId: Int): Option[BatchContext] = correlationMap.get(seqId) map { case (bCtx, _) => bCtx }

  def -=(seqId: Int): CorrelationMapWithLimiter = {
    correlationMap.remove(seqId) foreach { case (b, size) =>
      limiter.release(size, b.requestUuid)
    }
    this
  }

  def collect[B](pf: PartialFunction[(Int, BatchContext), B]): Iterable[B] = {
    correlationMap.collect { case (seqId, (batch, _)) if pf.isDefinedAt((seqId, batch)) => pf(seqId, batch) }
  }
}
