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
package optimus.platform.dsi.protobufutils

import java.util
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.ChainedID
import optimus.graph.NodeTask
import optimus.graph
import optimus.graph.Scheduler
import optimus.platform.dal.session.ClientSessionContext
import java.util.{ArrayList => JArrayList}

import scala.concurrent.Promise

abstract class AsyncBatchingOutOfWorkQueueBase[A](
    batchAccumulationWaitTime: Long,
    maxBatchSize: Int,
    maxBatchBuildTime: Long)
    extends AsyncBatchingQueue[A](batchAccumulationWaitTime, maxBatchSize, maxBatchBuildTime)
    with BatchingQueueShutdown[A] {

  override protected val log = getLogger(this)

  protected val outOfWorkToken: A

  override protected def isOutOfWorkToken(t: A): Boolean = t == outOfWorkToken

  object OutOfWorkListener extends graph.OutOfWorkListener {

    /*
     * You're supposed to synchronize when making structural changes to IdentityHashMap according to doc but
     * this is never used from multiple threads. It will always be called either from the one batching thread
     * or during shutdown.
     */
    val subscriptions = new util.IdentityHashMap[Scheduler, Unit]()

    // see doc for super
    override def onGraphStalled(scheduler: Scheduler, outstandingTasks: JArrayList[NodeTask]): Unit = {
      /*
       * no need to check if we're subscribed here, if this callback was invoked then it's us who subscribed
       * and we need to resubscribe automatically independently of any batch starting so even batches of 1 can benefit
       */
      log.trace(s"scheduler sent onGraphStalled")
      scheduler.addOutOfWorkListener(OutOfWorkListener)

      /*
       * there might be room for improvement here. currently we send the token if *any* scheduler tells us they're
       * out of work. we might consider turning this into "all schedulers involved in this batch"
       */
      offer(outOfWorkToken)
    }

    def maybeSubscribe(scheduler: Scheduler): Unit = {
      // we only need update this map once to make sure there's exactly one subscription
      if (!subscriptions.containsKey(scheduler)) {
        subscriptions.put(scheduler, ())
        log.info(s"new scheduler, subscribing to oow notifications")
        scheduler.addOutOfWorkListener(OutOfWorkListener)
      }
    }

    def unsubscribeAll(): Unit = {
      subscriptions.keySet.forEach(_.removeOutOfWorkListener(OutOfWorkListener))
    }
  }

  override def printParams(): Unit = {
    log.info(s"Out of work listener version")
    super.printParams()
  }

  // Can't put this within the shutdownLock as it might deadlock with the scheduler lock if it's trying to send an
  // OOW notification at the same time. Should not be a problem if it does, an OOW token by itself in a batch will be
  // ignored
  override protected def preShutdown(): Unit = {
    OutOfWorkListener.unsubscribeAll()
  }
}

class AsyncBatchingOutOfWorkQueue(batchAccumulationWaitTime: Long, maxBatchSize: Int, maxBatchBuildTime: Long)
    extends AsyncBatchingOutOfWorkQueueBase[ClientRequest](batchAccumulationWaitTime, maxBatchSize, maxBatchBuildTime)
    with ClientRequestBatchingQueueShutdown[ClientRequest] {

  override protected val outOfWorkToken: ClientRequest = ClientRequest.read(
    CompletablePromise(Promise.failed(new Error)),
    ChainedID.root,
    new ClientSessionContext,
    Seq.empty)

  override protected def isOutOfWorkToken(t: ClientRequest): Boolean = {
    if (super.isOutOfWorkToken(t)) {
      true
    } else {
      t.completable match {
        case NodeWithScheduler(scheduler, _, _, _) => OutOfWorkListener.maybeSubscribe(scheduler)
        case other                                 => throw new Error(s"Can't work with $other")
      }
      false
    }
  }
}
