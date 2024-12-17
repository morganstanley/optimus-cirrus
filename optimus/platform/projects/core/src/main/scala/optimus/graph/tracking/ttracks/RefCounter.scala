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
package optimus.graph.tracking.ttracks

import java.lang.ref.ReferenceQueue
import java.lang.ref.WeakReference
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.tailrec

/**
 * A reference queue that counts gc-ed referents and performs actions when a watermark is reached.
 *
 * Functions using the marker type Acc must take into account integer overflow, by modifying those values only by addition or
 * subtraction.
 */
abstract class RefCounter[T]() {

  /** To avoid issues with integer overflow, we mark integers with Acc that need to be modified only by difference. */
  type Acc = Int

  /**
   * Update the reference watermark. This should be done by doing an addition against the currentWatermark to avoid
   * overflows.
   */
  def nextWatermark(currentWatermark: Acc): Acc

  /**
   *  Perform an action when the watermark is breached. The call to this function will only occur once per watermark
   *  breach but it isn't synchronized.
   */
  def onWatermarkBreached(): Unit

  private val cleared = new AtomicInteger(0)
  private val queue: ReferenceQueue[T] = new ReferenceQueue[T]

  // watermark
  @volatile private var watermark: Int = nextWatermark(0)

  @tailrec private def cleanupLoop(accum: Int): Int = {
    val ref = queue.poll()
    if (ref eq null) accum
    else cleanupLoop(accum + 1)
  }

  private def maybeBreachWatermark(updated: Int): Unit = {
    val breached = {
      // needs double-checked locking so that only one thread actually breaches a given watermark
      //
      // the condition is written like this to account for overflow
      if ((watermark - updated) > 0) false
      else
        synchronized {
          if ((watermark - updated) > 0) false
          else {
            // We update the watermark based on the updated value. Note that this means that if we update by multiple times
            // the watermark, we will breach it only once
            watermark = nextWatermark(updated)
            true
          }
        }
    }

    if (breached) {
      // executed out of lock to avoid deadlock if the watermark action breaches watermark again
      onWatermarkBreached()
    }
  }

  private def emptyQueueAndTestWatermark(): Unit = {
    // This method is hot, it runs whenever a ttrack ref is created.

    // fast path: queue is empty, poll returns nothing, we exit immediately
    val ref = queue.poll()
    if (ref eq null) return

    // ref wasn't null, so we did clear 1 item already
    val found = cleanupLoop(1)
    maybeBreachWatermark(cleared.addAndGet(found))
  }

  def incrementCountManuallyForTestingAndTestWatermark(by: Int): Unit = {
    maybeBreachWatermark(cleared.addAndGet(by))
  }

  /**
   * Return the difference between lastColl and the current state.
   */
  def getUpdate(lastColl: Acc): Int = cleared.get() - lastColl

  /**
   * Return a snapshot of the current value. This doesn't poll or clear the refqueue, so the snapshot might not reflect
   * newly collected items.
   */
  def snapshot: Acc = cleared.get()

  def createWeakReference(ref: T): WeakReference[T] = {
    emptyQueueAndTestWatermark()
    new WeakReference[T](ref, queue)
  }
}
