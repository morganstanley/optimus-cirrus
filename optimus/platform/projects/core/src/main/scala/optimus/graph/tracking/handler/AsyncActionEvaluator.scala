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
package optimus.graph.tracking.handler

import optimus.graph.ProgressTracker
import optimus.utils.MacroUtils.SourceLocation
import optimus.graph.tracking.EventCause
import optimus.graph.tracking.handler.AsyncActionEvaluator.Action

import scala.annotation.tailrec
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

object AsyncActionEvaluator {
  trait Action {
    def waitForAllPrevious: Boolean = false
    val details: ActionDetails

    def runAsync(onComplete: Try[List[Action]] => Unit, progressTracker: ProgressTracker = null): Unit
    def cause: EventCause

    // Ensure callback is called even if an exception is thrown by the run method
    def tryRunAsync(onComplete: Try[List[Action]] => Unit, progressTracker: ProgressTracker = null): Unit = {
      val t = Try(runAsync(onComplete, progressTracker))
      t match {
        case Failure(_) => onComplete(t.map(_ => List[Action]()))
        case _          =>
      }
    }
  }

  private final case class State(
      private var todo: Int,
      private var result: Try[Unit],
      private val completedCallback: Try[Unit] => Unit) {
    private var completed: Boolean = false

    def beginAction(): Boolean = synchronized {
      if (!result.isFailure) {
        todo += 1
        true
      } else false
    }

    private def injectSrcLoc(
        actionName: String,
        sourceLocation: SourceLocation
    ): PartialFunction[Throwable, Try[Unit]] = { case NonFatal(e) =>
      e.addSuppressed(
        new RuntimeException(
          s"Information: Exception thrown running $actionName constructed at line " +
            s"${sourceLocation.line} in method ${sourceLocation.method}"))
      Failure(e)
    }

    def completeAction(action: Action): Unit = synchronized {
      todo -= 1
      if ((result.isFailure || todo <= 0) && !completed) {
        completed = true
        completedCallback(result recoverWith injectSrcLoc(action.getClass.getSimpleName, action.details.sourceLocation))
      }
    }

    def combine(t: Try[_]): Unit = synchronized {
      result = result.flatMap(_ => t.map(_ => ()))
    }
  }

  private def StartState(completedCallback: Try[Unit] => Unit) =
    State(0, Success(()), completedCallback)

  /**
   * Pre-order evaluation of a graph of async actions
   *
   * If an action fails, then no further actions will begin evaluating, and the completion callback will be called
   * regardless of whether there are any in-flight actions to be completed.
   */
  def start(action: Action, callback: Try[Unit] => Unit, progressTracker: ProgressTracker = null): Unit = {
    processLevel(StartState(callback), List(action), PendingActionList(action), progressTracker)
  }

  private def processLevel(
      state: State,
      actions: List[Action],
      parentPrevious: PendingActionList#Node,
      progressTracker: ProgressTracker): Unit = {
    // link up the pending action list nodes *before* we complete the parent (otherwise subsequent actions could start
    // without waiting for these actions)
    var current = parentPrevious
    val actionsAndNodes = actions.map { action =>
      current = current.insertNext(action)
      (action, current)
    }
    parentPrevious.actionComplete()
    processNext(state, actionsAndNodes, progressTracker)
  }

  def processNext(
      state: State,
      actionsAndPreviousAndCurrent: List[(Action, PendingActionList#Node)],
      progressTracker: ProgressTracker): Unit =
    processNextRec(state, actionsAndPreviousAndCurrent, progressTracker)

  @tailrec private def processNextRec(
      state: State,
      actionsAndPreviousAndCurrent: List[(Action, PendingActionList#Node)],
      progressTracker: ProgressTracker): Unit = actionsAndPreviousAndCurrent match {
    case Nil =>
    case (action, pendingNode) :: more =>
      if (action.waitForAllPrevious) {
        // if we need to wait for previous, register callback, else run immediately
        pendingNode.onPreviousComplete { () =>
          if (state.beginAction()) {
            action.tryRunAsync(onComplete = complete(state, action, pendingNode, progressTracker), progressTracker)
            // process next immediately - no need to wait for completion of previous action here because that's taken
            // care of before runAction() is called.

            // This isn't stack safe, but we aren't super likely to have many .waitForAllPreviousSteps in a row
            processNext(state, more, progressTracker)
          }
        }
      } else {
        if (state.beginAction()) {
          action.tryRunAsync(onComplete = complete(state, action, pendingNode, progressTracker), progressTracker)
          processNextRec(state, more, progressTracker)
        }
      }
  }

  private def complete(state: State, action: Action, current: PendingActionList#Node, progressTracker: ProgressTracker)(
      result: Try[List[Action]]): Unit = {
    state.combine(result)
    result
      .map { newActions =>
        if (newActions.nonEmpty) {
          processLevel(state, newActions, current, progressTracker)
        } else current.actionComplete()
      }
      .recover { case _ =>
        current.actionComplete()
        Nil
      }

    state.completeAction(action)
  }
}

object PendingActionList {
  def apply(action: Action): PendingActionList#Node = {
    val l = new PendingActionList
    new l.Node(null, null, action)
  }
}

/**
 * A doubly linked list which tracks the completion state of previous Actions.
 */
class PendingActionList {
  // a lock shared by all Nodes in this list (because we don't expect this to be contended enough to justify reasoning
  // about a lockless or per-Node locking scheme)
  private val lock = new Object

  class Node(var previous: Node, var next: Node, var watching: Action) {
    private var waiter: () => Unit = _

    /*
     * Inserts a new Node immediately after this one (and therefore *before* any nodes returned by previous calls to
     * insertNext on this node). You cannot call insertNext after actionComplete() because in that case subsequent
     * Nodes might have already be marked complete.
     *
     * for example, in:
     *
     * val a = PendingActionList(A)
     * val b = a.insertNext(B)
     * val c = b.insertNext(C)
     * val d = b.insertNext(D)
     *
     * the list will be A -> B -> D -> C. This is desirable for our use case, because in the below handler tree, C needs
     * to wait for B and its child D
     *
     * A -> B -> D
     *  \-> C
     */
    def insertNext(a: Action): Node = lock.synchronized {
      require(watching != null)
      val l = new Node(previous = this, next = this.next, watching = a)
      this.next = l
      l
    }

    /**
     * f will be called once all previous Nodes in the list are complete
     */
    def onPreviousComplete(f: () => Unit): Unit = {
      lock.synchronized {
        require(waiter == null)
        waiter = f
      }
      notifyWaiterIfPreviousDone()
    }

    /**
     * call this once the watched Action is complete
     */
    def actionComplete(): Unit = {
      lock.synchronized {
        watching = null
      }
      notifyNextIfAllDone()
    }

    /** notifies this Node that the previous Node is complete */
    private def previousComplete(): Unit = {
      lock.synchronized {
        previous = null
      }
      notifyWaiterIfPreviousDone()
      notifyNextIfAllDone()
    }

    /**
     * if previous is done, run the waiter callback
     */
    private def notifyWaiterIfPreviousDone(): Unit = {
      val toRun = lock.synchronized {
        if (previous == null) {
          val w = waiter
          waiter = null
          w
        } else null
      }
      // callback outside of lock for safety
      if (toRun != null) toRun()
    }

    /**
     * if previous and our action are done, notify the next Node (if any)
     */
    private def notifyNextIfAllDone(): Unit = {
      val toNotify = lock.synchronized {
        if (previous == null && watching == null) next
        else null
      }
      // callback outside of lock for safety
      if (toNotify != null) toNotify.previousComplete()
    }

  }
}
