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
package optimus.graph.tracking

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeoutException
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.graph.DiagnosticSettings
import optimus.graph.EventCauseInInvalidState
import optimus.graph.OGTrace
import optimus.graph.Settings
import optimus.utils.MacroUtils.SourceLocation

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.Duration

/**
 * EventCause is a way to track completions of DependencyTracker events. This provides important stability for UI
 * testing, and a convenient place to put profiling data.
 *
 * EventCauses are threaded through DependencyTracker tasks and related tasks that start from them. They can have
 * children and tokens. The tokens are created and released when work is done in the EventCause, and when the last open
 * token is closed the event cause becomes complete. Children causes open and close tokens on their parents: they
 * represent dependencies of the parent task.
 */
private[optimus] object EventCause {
  val globalTagKey = "globalEventTag"
  val log: Logger = getLogger(getClass)
  private[tracking] val instrumentToken = Settings.instrumentEventCauseToken

  private val tokenIdGen = new AtomicLong

  @volatile private var profileAllEvents = DiagnosticSettings.diag_showConsole && OGTrace.observer.traceNodes
  private[optimus] def setProfiledAllEventsEnabled(v: Boolean): Unit = profileAllEvents = v
  private[optimus] def profileAllEventsEnabled: Boolean = profileAllEvents

  sealed abstract class Token(val cause: EventCause) {
    private[EventCause] val id = tokenIdGen.incrementAndGet()

    override def toString: String = s"#$id on '$cause'"

    def release(): Unit = cause.countDown(this)

    private[EventCause] def createdAt: Exception = null
    private[EventCause] def released: Exception = null
  }

  private sealed class IndividualToken(cause: EventCause) extends Token(cause)

  private final class InstrumentedToken(cause: EventCause) extends IndividualToken(cause) {
    private[EventCause] override val createdAt = new Exception("trace creation")
    private[this] var releasedAt: Exception = _
    private[EventCause] override def released = releasedAt
    override def release(): Unit = {
      if (releasedAt eq null) {
        releasedAt = new Exception("released")
      } else
        cause.throwOrLogException(s"cant release token it was released already $cause", this)

      super.release()
    }
  }

  // A token that doesn't track
  final class NullToken(cause: EventCause) extends Token(cause) {
    override def release(): Unit = () // do nothing
  }

  private[tracking] sealed trait NonTrackingEventCause extends RootEventCause {
    private val fakeToken = new NullToken(this)

    override final def createAndTrackToken(): EventCause.Token = fakeToken
    override protected[tracking] final def countUp(): Unit = {}
    override protected[tracking] final def countDown(t: EventCause.Token): Unit = {}
  }

  type ChildEventCauseBuilder[T <: ChildEventCause] =
    (String, EventCause, TrackingScope[_ >: Null <: TrackingMemo]) => T

  // The base class for child event cause, those event cause that aren't derived from Root
  private[optimus] class ChildEventCause(
      override val cause: String,
      val parent: EventCause,
      override val scope: TrackingScope[_ >: Null <: TrackingMemo],
      override val includeInHandlerProfCrumb: Boolean = false,
      // If set, this child task will not take a token on the parent (won't block completion) but will still report
      // profiling data via backgroundComplete().
      background: Boolean = false
  ) extends EventCause {
    override def toString = s"ChildEventCause($cause)"

    // This is a val so that we dont have to recurse up the tree every time we want to grab the root on a leaf
    override val root: RootEventCause = parent.root

    // Background steps are treated as foreground steps when waitForInBackgroundSteps is set to true
    val isBackgroundStep: Boolean = !Settings.waitForInBackgroundSteps && background

    override val profile: ECProfilerLeaf =
      parent.profile.newChild(cause, includeInHandlerProfCrumb)

    /* Why is this done lazily?
     *
     * Not all child event cause will call complete. Sometimes child event cause are created that never create tokens.
     * If no tokens are ever created, then the cause never completes, and if we were to eagerly grab tokens from the
     * parent, we would stop the parent from ever completing too. What we do instead is only start tracking on the
     * parent event cause when this event cause is asked for a token, because by that point we *know* that we will
     * eventually close the token (and eventually call complete.)
     */
    private lazy val parentToken: Token = {
      if (isBackgroundStep) root.createAndTrackBackgroundToken()
      else parent.createAndTrackToken()
    }

    override def createAndTrackToken(): EventCause.Token = synchronized {
      val res = super.createAndTrackToken()
      require(parentToken != null)
      res
    }

    override def onComplete(): Unit = parentToken.release()

    override def throwOrLogException(
        exceptionMsg: String,
        token: EventCause.Token = null,
        debug: Boolean = false): Unit =
      parent.throwOrLogException(exceptionMsg, token)
  }
}

private[optimus] trait WithEventCause {
  def eventCause: Option[EventCause]
}

// A RootEventCause is an event cause that isn't a child.
//
// Note that due to a compiler bug in 2.13 this trait can't be in the EventCause companion object. If you put it there
// you get a stack overflow in scala refchecks.
trait RootEventCause extends EventCause {

  // override final val here makes it impossible to extend Root with ChildEventCause so the two types are guaranteed to
  // be orthogonal
  override final val root = this

  // The place where we accumulate profiles
  final val rootProfiler = new ECProfiler(this)
  override final def profile: ECProfilerLeaf = rootProfiler.prof

  /* called on the root when all transitive child events including any InBackground steps are complete */
  def onBackgroundComplete(): Unit = ()

  private var error: Throwable = _
  private[optimus] def foldError(e: Throwable): Throwable = synchronized {
    // addSuppressed throws on null exception or on x.addSuppressed(x)
    if ((error != null) && (error ne e)) e.addSuppressed(error)
    error = e
    e
  }
  private[tracking] def maybeThrowEventCauseErrors(): Unit = synchronized {
    if (error ne null) throw error
  }

  private[tracking] final def createAndTrackBackgroundToken(): EventCause.Token = {
    countUpInBackground()
    new InBackgroundToken
  }

  private final class InBackgroundToken extends EventCause.Token(this) {
    override def release(): Unit = countDownInBackground(this)
  }

  /** Incremented when a background cause is created, decremented when it completes */
  @volatile private[this] var inBackgroundCounter = 0

  protected final def countUpInBackground(): Unit = synchronized {
    if (isInBackgroundComplete)
      throwOrLogException(s"EventCause inBackgroundCounter $this attempted to count up after in background complete")
    inBackgroundCounter += 1
  }

  protected final def countDownInBackground(token: EventCause.Token): Unit = {
    synchronized {
      if (inBackgroundCounter > 0)
        inBackgroundCounter -= 1
      else
        throwOrLogException("inBackgroundCounter must be more than 0 in order to countDown", token)
    }

    // callback outside the lock (out of an abundance of caution). This is safe because we never count down to zero
    // and then back up again when isComplete is true
    if (isInBackgroundComplete)
      onBackgroundComplete()
  }

  /**
   * true if all background events are complete and the root event cause is complete (in which case there can be
   * no further background events started)
   */
  final def isInBackgroundComplete: Boolean = inBackgroundCounter == 0 && isComplete

  override protected def countDown(token: EventCause.Token): Unit = {
    super.countDown(token)
    if (isInBackgroundComplete)
      onBackgroundComplete()
  }
}

private[optimus] trait EventCause {
  import EventCause._

  /** called once this event and all transitive children _excluding_ InBackground steps are complete */
  protected def onComplete(): Unit = ()
  def includeInHandlerProfCrumb: Boolean = false
  def keepAliveWhileEvaluating(o: AnyRef): Unit = {}
  def profile: ECProfilerLeaf
  def root: RootEventCause

  // Counter is zero when no token have been added to the event cause OR it has completed. In the former case, completed
  // will be false
  @volatile private var counter: Int = _
  @volatile private var completed: Boolean = _
  @volatile private var closedAt: Exception = _

  def createAndTrackToken(): EventCause.Token = {
    countUp()
    if (instrumentToken) new InstrumentedToken(this) else new IndividualToken(this)
  }

  def scope: TrackingScope[_ >: Null <: TrackingMemo] = NullScope

  protected def countUp(): Unit = synchronized {
    if (!completed) counter += 1
    else throwOrLogException(s"Cannot countUp already completed EventCause $this")
  }

  protected def countDown(token: EventCause.Token): Unit = synchronized {
    safeAssert(counter > 0, "Counter must be more than 0 in order to countDown")
    counter -= 1
    if (counter == 0) {
      if (completed) throwOrLogException(s"Already completed EventCause $this", token)
      else {
        // Note: because onComplete will publish the profile, the order of the following two lines matter.
        profile.markCompleted()
        onComplete()
        notifyAll()
        completed = true
        if (instrumentToken) closedAt = token.released
      }
    } else if (counter < 0) throwOrLogException(s"EventCause counter $this should not be negative", token)
  }

  // This is called from all blocking follow on step cause creation, and carries profiling information for crumbs.
  final def createProfiledChild(child: String, sourceLocation: SourceLocation): EventCause =
    createChildImpl(
      child + s" - ${sourceLocation.stackTraceString}",
      scope,
      includeInHandlerProfilingCrumb = true,
      background = false)

  // This creates a background child that will update profiling information when it is done but doesn't itself block
  // the remaining tasks.
  final def createBackgroundProfiledChild(child: String, sourceLocation: SourceLocation): EventCause =
    createChildImpl(
      child + s" - ${sourceLocation.stackTraceString}",
      scope,
      includeInHandlerProfilingCrumb = true,
      background = true)

  // Create a non-profiled child.
  final def createChild(child: String): EventCause =
    createChildImpl(child, scope, includeInHandlerProfilingCrumb = false, background = false)

  final def createChild(child: String, scope: TrackingScope[_ >: Null <: TrackingMemo]): EventCause =
    createChildImpl(child, scope, includeInHandlerProfilingCrumb = false, background = false)

  // Create a child using a builder
  def createChild[T <: ChildEventCause](builder: ChildEventCauseBuilder[T], child: String): T = {
    if (completed) throwOrLogException(s"Cannot create child $child of completed EventCause $this")
    builder(child, this, scope)
  }

  private def createChildImpl(
      child: String,
      scope: TrackingScope[_ >: Null <: TrackingMemo],
      includeInHandlerProfilingCrumb: Boolean,
      background: Boolean): ChildEventCause = {
    // A background task may very well be attached to an already completed foreground cause. That's fine.
    if (completed && (!background)) throwOrLogException(s"Cannot create child $child of completed EventCause $this")
    new ChildEventCause(child, this, scope, includeInHandlerProfilingCrumb, background)
  }

  // generally used to run a block of code that might generate child causes
  def counted[T](fn: => T): T = {
    val token = createAndTrackToken()
    try fn
    finally token.release()
  }

  def isComplete: Boolean = completed

  def cause: String = getClass.getSimpleName

  def safeAssert(condition: Boolean, message: String): Unit = {
    if (Settings.throwOnEventCauseErrors) {
      assert(condition, message)
    } else if (!condition) {
      log.warn(message)
    }
  }

  def throwOrLogException(baseExceptionMsg: String, token: EventCause.Token = null, debug: Boolean = false): Unit = {
    val exceptionMsg =
      if (token != null)
        s"$baseExceptionMsg alreadyReleased = ${token.released != null}"
      else
        baseExceptionMsg

    val toThrow = root.foldError(new EventCauseInInvalidState(exceptionMsg, this))

    // add more data for logging
    if (token ne null) {
      if (token.createdAt ne null) toThrow.addSuppressed(token.createdAt)
      if (token.released ne null) toThrow.addSuppressed(token.released)
    }
    val closedAt = this.closedAt
    if (closedAt ne null) {
      val e = new Exception("event cause was closed at")
      e.setStackTrace(closedAt.getStackTrace)
      toThrow.addSuppressed(e)
    }

    if (Settings.schedulerAsserts || Settings.throwOnEventCauseErrors) { throw toThrow }
    else if (debug) log.debug(exceptionMsg)
    else log.warn(exceptionMsg)
  }

  def isEmpty = false

  override def toString: String = cause

  // EventCause has identity semantics
  override final def hashCode(): Int = System.identityHashCode(this)

  override final def equals(obj: Any): Boolean = obj.asInstanceOf[AnyRef] eq this

  /** Wait for this event cause to complete. Throws if the timeout is exceeded or if there was an event cause error. */
  def awaitComplete(timeoutMs: Long): Unit = {
    if (!waitUntilMaybeComplete(timeoutMs)) throw new TimeoutException(s"timeout exceeded while waiting on ${cause}")
    else {
      root.maybeThrowEventCauseErrors()
    }
  }

  /**
   * See [[awaitComplete(Long)]]
   */
  def awaitComplete(timeout: Duration): Unit = awaitComplete(timeout.toMillis)

  // Wait up to maxWaitMs in ms either until this EventCause completes (returning true) or the timeout is reached (returning false).
  private def waitUntilMaybeComplete(maxWaitMs: Long): Boolean = synchronized {
    val max = System.currentTimeMillis() + maxWaitMs
    var remaining = maxWaitMs
    while (!completed && remaining > 0L) {
      wait(remaining) // interrupted exception can fail a test
      remaining = max - System.currentTimeMillis()
    }
    completed
  }

  private[optimus] def causeStack: String = {
    // evaluate f for every parent up
    @tailrec def forParents(c: EventCause)(f: EventCause => Unit): Unit = {
      f(c)
      c match {
        case child: ChildEventCause => forParents(child.parent)(f)
        case _                      =>
      }
    }

    val sb = new StringBuilder()
    forParents(this) { c =>
      if (c != this) sb.append('\n') // not on first cause
      sb.append("  - ")
      sb.append(c.cause)
    }
    sb.toString
  }
}

private[optimus] final case class TestEventCause(
    override val cause: String = "TestEventCause",
    override val scope: TrackingScope[_ >: Null <: TrackingMemo] = NullScope)
    extends RootEventCause

private[optimus] case object ExcelEventCause extends EventCause.NonTrackingEventCause

private[optimus] final case class TrackingActionEventCause(override val cause: String) extends RootEventCause

case object NoEventCause extends EventCause.NonTrackingEventCause {
  override def isEmpty = true
}

private[optimus] final case class BootstrapEventCause(override val cause: String) extends RootEventCause

private[optimus] trait DuplicateEvaluationDetection extends EventCause {
  var debugUITracks: ArrayBuffer[AnyRef] = _
  protected def cleanDebugTracks(): Unit = debugUITracks = null
  override def keepAliveWhileEvaluating(o: AnyRef): Unit = synchronized {
    if (debugUITracks eq null) debugUITracks = new ArrayBuffer[AnyRef]()
    debugUITracks += o
  }
}

/**
 * These are events that are not handled by the dependency tracker and therefore do not contain any actual event cause
 * information.
 */
private[optimus] case object ProgressTrackingEventCause extends EventCause.NonTrackingEventCause
