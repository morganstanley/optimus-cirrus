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

import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.collection.mutable

abstract class BatchingQueue[T] {

  protected val log: Logger

  protected def capacity: Int = Int.MaxValue

  protected val batchQueue: BlockingQueue[T] = new LinkedBlockingQueue[T](capacity)

  val buffer = mutable.ArrayBuffer[T]()
  var previousBatchEnd = 0L

  def size: Int = batchQueue.size()

  def printParams(): Unit

  def offer(req: T): Boolean = {
    batchQueue.offer(req)
  }

  protected def isOutOfWorkToken(t: T): Boolean = false

  def drain(): Unit

  private def nextBatch(blockForBatch: Boolean) = {
    @tailrec
    def getFirst(): Option[T] = {
      log.trace(s"batch queue size before take - ${batchQueue.size()}")
      val candidate = if (!blockForBatch && batchQueue.size() == 0) None else { Some(batchQueue.take()) }

      candidate match {
        case Some(c) if isOutOfWorkToken(c) =>
          log.trace("ignoring batch with only the out of work token in it")
          getFirst()
        case x => x
      }

    }
    getFirst() foreach { first =>
      buffer += first
    }

    drain()

    val nextBatch = buffer.toVector
    buffer.clear()
    nextBatch
  }

  def getNextBatch() = {
    nextBatch(true)
  }

  // can return empty vector.
  final def getNextNonBlockingBatch() = {
    nextBatch(false)
  }

}

class AsyncBatchingQueue[T](batchAccumulationWaitTime: Long, maxBatchSize: Int, maxBatchBuildTime: Long)
    extends BatchingQueue[T] {

  override protected val log = getLogger[AsyncBatchingQueue[_]]

  override def printParams(): Unit = {
    log.info(
      s"Will wait at most ${batchAccumulationWaitTime}ms for a new item to arrive (optimus.dsi.batchAccumulationWaitTime)")
    log.info(s"Will build batches of at most $maxBatchSize entries (optimus.dsi.maxBatchSize)")
    log.info(s"Will close batch if ${maxBatchBuildTime}ms is exceeded (optimus.dsi.maxBatchBuildTime)")
  }

  override def drain(): Unit = {

    val start = System.currentTimeMillis
    val softDeadline = start + maxBatchBuildTime
    if (previousBatchEnd > 0L)
      log.trace(s"building batch, it's been ${start - previousBatchEnd}ms since the last one was completed")

    def underLimits = {
      val withinSizeLimit = buffer.size < maxBatchSize
      val withinTimeLimit = System.currentTimeMillis < softDeadline
      if (withinSizeLimit && withinTimeLimit) true
      else {
        if (!withinSizeLimit) log.trace(s"batch size limit of $maxBatchSize reached")
        if (!withinTimeLimit)
          log.trace(s"batch building time limit of ${maxBatchBuildTime}ms reached, collected ${buffer.size} entries")
        false
      }
    }

    @tailrec
    def doDrain: Unit = {
      val t1 = System.currentTimeMillis()
      batchQueue.poll(batchAccumulationWaitTime, TimeUnit.MILLISECONDS) match {
        case null =>
          log.trace(s"max wait time of $batchAccumulationWaitTime reached")
        case oowToken if isOutOfWorkToken(oowToken) =>
          def howEarly = batchAccumulationWaitTime - (System.currentTimeMillis() - t1)
          log.trace(s"out of work notification, closing ${howEarly}ms earlier")
        case item =>
          buffer += item
          if (underLimits) doDrain
      }
    }

    if (underLimits) doDrain

    val finished = System.currentTimeMillis
    previousBatchEnd = finished
    log.trace(s"batched ${buffer.size} entries in ${finished - start}ms")
  }

}

class SyncBatchingQueue[T](batchSize: Int = 999) extends BatchingQueue[T] {

  protected val log = getLogger[SyncBatchingQueue[_]]

  override def printParams(): Unit = {
    log.info("Sync batching queue")
  }

  override def drain(): Unit = {
    batchQueue.drainTo(buffer.asJava, batchSize)
  }

}
