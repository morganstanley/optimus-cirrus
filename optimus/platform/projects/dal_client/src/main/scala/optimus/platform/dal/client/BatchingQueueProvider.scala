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

import optimus.platform.dal.config.DalAsyncConfig
import optimus.platform.dsi.protobufutils.AsyncBatchingOutOfWorkQueue
import optimus.platform.dsi.protobufutils.AsyncBatchingQueue
import optimus.platform.dsi.protobufutils.BatchRetryManager
import optimus.platform.dsi.protobufutils.BatchingQueue
import optimus.platform.dsi.protobufutils.BatchingQueueShutdown
import optimus.platform.dsi.protobufutils.ClientRequest
import optimus.platform.dsi.protobufutils.ClientRequestBatchingQueueShutdown
import optimus.platform.dsi.protobufutils.SyncBatchingQueue

trait BatchingQueueProvider {
  def getBatchingQueue: BatchingQueue[ClientRequest] with BatchingQueueShutdown[ClientRequest]
}

final class BatchingQueueProviderImpl(
    config: DalAsyncConfig,
    write: Boolean,
    batchRetryManagerOpt: Option[BatchRetryManager])
    extends BatchingQueueProvider {
  override def getBatchingQueue: BatchingQueue[ClientRequest] with BatchingQueueShutdown[ClientRequest] = batchingQueue

  private lazy val batchingQueue: BatchingQueueShutdown[ClientRequest] =
    if (config.async && (!write || config.maxBatchSizeForWrite > 1)) {
      val maxBatchSize = if (write) config.maxBatchSizeForWrite else config.maxBatchSize
      if (config.useOutOfWorkListener) {
        new AsyncBatchingOutOfWorkQueue(config.batchAccumulationWaitTime, maxBatchSize, config.maxBatchBuildTime) {
          override val batchRetryManager: Option[BatchRetryManager] = batchRetryManagerOpt
        }
      } else {
        new AsyncBatchingQueue[ClientRequest](config.batchAccumulationWaitTime, maxBatchSize, config.maxBatchBuildTime)
          with ClientRequestBatchingQueueShutdown[ClientRequest] {
          override val batchRetryManager: Option[BatchRetryManager] = batchRetryManagerOpt
        }
      }
    } else {
      if (!write)
        new SyncBatchingQueue[ClientRequest] with ClientRequestBatchingQueueShutdown[ClientRequest] {
          override val batchRetryManager: Option[BatchRetryManager] = batchRetryManagerOpt
        }
      else
        new SyncBatchingQueue[ClientRequest](config.maxBatchSizeForWrite)
          with ClientRequestBatchingQueueShutdown[ClientRequest] {
          override val batchRetryManager: Option[BatchRetryManager] = batchRetryManagerOpt
        }
    }
}
