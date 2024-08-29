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
import optimus.platform.dsi.protobufutils.BatchRetryManager

private[optimus] abstract class DalClientBatchTracker(reqLimiter: ClientRequestLimiter) {
  private val correlationMap = new CorrelationMapWithLimiter(reqLimiter)

  private[optimus] def onBatchComplete(seqId: Int): Unit =
    correlationMap -= seqId

  private[optimus] def getBatch(seqId: Int): Option[BatchContext] =
    correlationMap.get(seqId)

  private[optimus] def onBatchStart(seqId: Int, batch: BatchContext): Unit =
    correlationMap += (seqId -> batch)

  private[optimus] def collectInFlightBatches[U](pf: PartialFunction[(Int, BatchContext), U]): Iterable[U] =
    correlationMap.collect(pf)

  private[optimus] def clearInFlightBatches(): Unit =
    correlationMap.clear()

  def batchRetryManager: Option[BatchRetryManager]
}
