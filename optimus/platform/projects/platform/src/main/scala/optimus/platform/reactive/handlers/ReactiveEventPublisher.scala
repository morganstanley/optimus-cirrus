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
package optimus.platform.reactive.handlers

import scala.util.control.NonFatal
import optimus.platform.util.Log

import java.util.concurrent.locks.ReentrantReadWriteLock

trait ReactiveEventPublisher[T <: ReactiveEvent] extends ReactiveEventPublisherInternal[T] {
  type Sub = ReactiveEventSubscriber[T]

  // user should define these methods.
  //
  // We guarantee that startImpl will only ever be called once between calls to stopImpl.
  protected def startImpl: Unit
  protected def stopImpl: Unit

  // user should directly use these methods
  protected def publish(t: T): Unit

  def statusEventSource: Option[StatusEventSource] = None
}

trait WithPublicPublish[T <: ReactiveEvent] extends ReactiveEventPublisher[T] {
  override def publish(t: T) = super.publish(t)
  override def publishError(t: Throwable) = super.publishError(t)
}

trait WithPublicStart[T <: ReactiveEvent] extends ReactiveEventPublisher[T] {
  override def start = super.start
}

trait WithPublicStop[T <: ReactiveEvent] extends ReactiveEventPublisher[T] {
  override def stop = super.stop
}

trait WithPublicStartStop[T <: ReactiveEvent]
    extends ReactiveEventPublisher[T]
    with WithPublicStart[T]
    with WithPublicStop[T]

/** Container for an item that allows reads and updates with ReadWriteLock semantics. */
private[handlers] final class StatefulIO[State](initial: State) {
  private val lock = new ReentrantReadWriteLock(true)
  private var state: State = initial

  // Run f, returning an R. The state is not modified
  def read[R](f: State => R): R = {
    lock.readLock().lock()
    try f(state)
    finally lock.readLock().unlock()
  }

  // Update the state and read the new state.
  def update[R](update: State => (State, R)): R = {
    lock.writeLock().lock()
    try {
      val (updated, out) = update(state)
      state = updated
      out
    } finally lock.writeLock().unlock()
  }

  def notUpdating(): Boolean = !lock.writeLock().isHeldByCurrentThread
}

/**
 * internal API and implementation details
 */
private[reactive] trait ReactiveEventPublisherInternal[T <: ReactiveEvent] { self: ReactiveEventPublisher[T] =>
  import ReactiveEventPublisherInternal.logger
  val subscribers = new StatefulIO(Set.empty[ReactiveEventSubscriber[T]])

  // Report errors to subscribers produced when the publisher was starting.
  //
  // Must be called in a dependency tracker action.
  final def handleStartErrors(e: Throwable): Unit = {
    val snapshot = subscribers.read(identity)
    snapshot.foreach(_.handleStartError(e))
  }

  final def addSubscriber(subscriber: ReactiveEventSubscriber[T]): Unit = {
    val shouldStart = subscribers.update { current =>
      val shouldStart = current.isEmpty
      val updated = current + subscriber
      ReactiveEventPublisherInternal.logger.trace(s"publisher should start: $shouldStart, subscribers: ${updated}")
      (updated, shouldStart)
    }

    // call start outside of write() to avoid a dead lock if start calls publish()
    if (shouldStart) start
  }

  final def removeSubscriber(subscriber: ReactiveEventSubscriber[T]): Unit = {
    val shouldStop = subscribers.update { current =>
      val updated = current - subscriber
      val shouldStop = updated.isEmpty
      logger.trace(s"publisher should stop: $shouldStop, subscribers: ${updated}")
      (updated, shouldStop)
    }

    // call stop outside of write() to avoid a dead lock if start calls publish()
    if (shouldStop) stop
  }

  // Publish an event. Overrides need to call publishNow eventually.
  protected def publish(t: T): Unit = publishNow(t)

  protected final def publishNow(t: T): Unit = subscribers.read { subscribers =>
    // Note that we publish under the read lock. This means that if any subscribers try to add a new subscriber during
    // published, they will deadlock forever. This could be a problem, but removing the lock here could also cause
    // issues, mainly that it would make it possible to publish to subscribers that are no longer subscribed at all and
    // might have started cleanup actions.
    //
    // This is a read lock because it is possible to have many publishers publishing at the same time.
    logger.trace(s"publishNow: event=$t")
    logger.trace(s"publishNow toPublish: ${subscribers}")
    for (consumer <- subscribers) {
      try {
        consumer.published(t)
      } catch {
        case NonFatal(e) =>
          logger.error(s"unhandled [ignored] error while publishing data $t", e)
          consumer.forwardErrorToStatusHandler(e)
      }
    }
  }

  // Like publish, but for errors
  protected def publishError(t: Throwable): Unit = publishErrorNow(t)
  protected final def publishErrorNow(t: Throwable): Unit = subscribers.read { subscribers =>
    for (consumer <- subscribers) {
      try {
        consumer.error(t)
      } catch {
        case NonFatal(e) =>
          logger.error(s"unhandled [ignored] error while publishing error $t", e)
      }
    }
  }

  // read and written under synchronizaton
  @volatile private var started = false
  protected def start: Unit = startNow()
  protected final def startNow(): Unit = synchronized {
    if (started) return

    // We need to set started to true in case the startImpl wants to call the stopImpl, which should result in a final
    // "stopped" state, or if it wants to publish, which is allowed.
    started = true
    assert(subscribers.notUpdating(), "cannot start while holding write lock because publication would deadlock")
    self.startImpl
  }

  protected def stop = stopNow()
  protected final def stopNow(): Unit = synchronized {
    if (!started) return // already stopped
    started = false
    assert(subscribers.notUpdating(), "cannot stop while holding write lock because publication would deadlock")
    self.stopImpl
  }
}

private[reactive] object ReactiveEventPublisherInternal extends Log {
  def logger = log
}
