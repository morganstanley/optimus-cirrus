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

import java.lang.ref.{Reference, ReferenceQueue, SoftReference, WeakReference}
import scala.annotation.nowarn

object CleaningReference {
  val queue = new ReferenceQueue[AnyRef]
  private object cleaningAction extends Runnable with Log {
    @nowarn("msg=class ThreadDeath in package lang is deprecated")
    def run: Unit = {
      log.info("CleaningReference start")
      try {
        while (true) {
          try {
            while (true) {
              val removed = queue.remove
              removed match {
                case toClean: CleaningReference[t] => toClean.startClean
                case unknown =>
                  log.error("Unexpected type in cleaner queue is ignored {} {} ", unknown.getClass, unknown)
              }
            }
          } catch {
            case t: ThreadDeath => throw t
            case t: Throwable   => log.error("CleaningReference error ignored while attempting to clean", t)
          }
        }
      } catch {
        case t: ThreadDeath =>
        case t: Throwable =>
          log.error("CleaningReference exiting", t)
          throw t
      } finally {
        log.info("CleaningReference exit")
      }
    }
  }
  init
  private def init = {
    val t = new Thread(cleaningAction)
    t.setDaemon(true)
    t.setName("CleaningReference")
    t.start
  }
}
trait CleaningReference[T <: AnyRef] { this: Reference[T] =>

  /**
   * starts a tidy-up. should not block for a significant time, otherwise it will block other CleaningReference to be
   * delayed
   */
  def startClean: Unit
}
trait CancellableCleaningReference[T <: AnyRef] { this: Reference[T] =>
  final def startClean = if (!cancelled) doStartClean
  @volatile private var cancelled = false
  def cancelCleaning = cancelled = true
  def doStartClean: Unit
}

abstract class WeakCleaningReference[T <: AnyRef](referent: T)
    extends WeakReference[T](referent, CleaningReference.queue)
    with CleaningReference[T]
abstract class SoftCleaningReference[T <: AnyRef](referent: T)
    extends SoftReference[T](referent, CleaningReference.queue)
    with CleaningReference[T]
