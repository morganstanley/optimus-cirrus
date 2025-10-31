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
package optimus.core

import optimus.breadcrumbs.BreadcrumbRegistration
import optimus.breadcrumbs.crumbs.CrumbNodeType
import optimus.core.CoreAPI.delayPromise
import optimus.graph._
import optimus.graph.cache.StargazerNodeHotSwap
import optimus.platform.PluginHelpers._
import optimus.platform._
import optimus.platform.annotations._
import optimus.platform.storable.Entity

import scala.annotation.unused

// Still defaulting to mutable Seq ... Some places have to enforce immutable */
import scala.collection.immutable.{Seq => ImmSeq}

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Success

trait AsyncResultAPI {
  @impure
  @scenarioIndependentTransparent
  @nodeSync
  @nodeSyncLift
  def asyncResult[T](@nodeLiftByName @nodeLift f: => T): NodeResult[T] = asyncResult$withNode(toNode(f _))
  // noinspection ScalaWeakerAccess
  def asyncResult$withNode[T](f: Node[T]): NodeResult[T] = new NodeResultNode(f).get
  // noinspection ScalaUnusedSymbol
  def asyncResult$queued[T](f: Node[T]): Node[NodeResult[T]] = new NodeResultNode(f).enqueue
  // noinspection ScalaUnusedSymbol
  def asyncResult$queued[T](f: => T): NodeFuture[NodeResult[T]] = new NodeResultNode(toNode(f _)).enqueue

  @impure
  @scenarioIndependentTransparent
  @nodeSync
  @nodeSyncLift
  def asyncResult[T](cs: CancellationScope)(@nodeLiftByName @nodeLift f: => T): NodeResult[T] =
    asyncResult$withNode(cs)(toNode(f _))
  // noinspection ScalaWeakerAccess
  @impure def asyncResult$withNode[T](cs: CancellationScope)(f: Node[T]): NodeResult[T] = new NodeResultNode(f, cs).get
  // noinspection ScalaUnusedSymbol
  @impure def asyncResult$queued[T](cs: CancellationScope)(f: Node[T]): Node[NodeResult[T]] =
    new NodeResultNode(f, cs).enqueue

}

trait NodeAPI {
  // methods to get nodes

  /**
   * Returns the [[Node]] of an expression in the current [[ScenarioStack]]. Will reuse a valid cached value and apply
   * the current tweaks if needed.
   */
  @nodeLift
  @parallelizable
  def nodeOf[T](@unused v: T): Node[T] = needsPlugin
  def nodeOf$node[T](node: NodeKey[T]): Node[T] = EvaluationContext.lookupNode(node)

  /**
   * Enqueues and returns the [[Node]] of an expression, as above.
   */
  @nodeLiftQueued
  @parallelizable
  def queuedNodeOf[T](@unused v: T): Node[T] = needsPlugin
  // noinspection ScalaUnusedSymbol
  def queuedNodeOf$nodeQueued[T](nodeFuture: NodeFuture[T]): Node[T] = nodeFuture.asNode$

  /**
   * Enqueues and returns the [[NodeFuture]] of an expression, as above.
   */
  // noinspection ScalaUnusedSymbol
  @nodeLiftQueued
  @parallelizable
  def nodeFutureOf[T](v: T): NodeFuture[T] = needsPlugin
  // noinspection ScalaUnusedSymbol
  def nodeFutureOf$nodeQueued[T](nodeFuture: NodeFuture[T]): NodeFuture[T] = nodeFuture

  /**
   * Returns the `NodeKey` of an expression (a.k.a. node template itself) The returned node is always relative to the
   * constant scenario stack; use [[NodeTask#prepareForExecutionIn]] to migrate it into a different scenario.
   */
  @nodeLift
  @parallelizable
  def nodeKeyOf[T](@unused v: T): NodeKey[T] = needsPlugin
  // noinspection ScalaUnusedSymbol
  def nodeKeyOf$node[T](node: NodeKey[T]): NodeKey[T] = node.tidyKey.asInstanceOf[NodeKey[T]]

  /** Returns the new, untouched node .... */
  @nodeLift
  @parallelizable
  def freshNodeOf[T](@unused v: T): PropertyNode[T] = needsPlugin
  // noinspection ScalaUnusedSymbol (Called as part of plugin functionality)
  def freshNodeOf$node[T](node: NodeKey[T]): PropertyNode[T] = node.asInstanceOf[PropertyNode[T]]

  // methods to un-get nodes

  /**
   * A convenience method to allow nodes to call manually held nodes in async fashion Note: Since NodeKey is the sealed
   * trait and the only implementation is PropertyNode we can just call lookupAndEnqueue
   */
  @nodeSync
  final def asyncLookupAndGet[T](node: NodeKey[T]): T = node.asInstanceOf[PropertyNode[T]].lookupAndGet
  // noinspection ScalaUnusedSymbol (Called as part of plugin functionality)
  final def asyncLookupAndGet$queued[T](node: NodeKey[T]): Node[T] = node.asInstanceOf[PropertyNode[T]].lookupAndEnqueue

  /**
   * If you already have a node (for some very advanced reason). This is an easy way to get back to optimus way of
   * coding
   */
  @nodeSync
  final def asyncGet[T](node: Node[T]): T = node.get
  // noinspection ScalaUnusedSymbol (Called as part of plugin functionality)
  final def asyncGet$queued[T](node: Node[T]): NodeFuture[T] = node.enqueue

  /**
   * If you already have a node WHICH DOES NOT REQUIRE ENQUEUING (for some very advanced reason). This is an easy way
   * to get back to optimus way of coding
   */
  @nodeSync
  final def asyncGetWithoutEnqueue[T](node: Node[T]): T = node.get
  // noinspection ScalaUnusedSymbol (Called as part of plugin functionality)
  final def asyncGetWithoutEnqueue$queued[T](node: Node[T]): Node[T] = node

  /** You almost certainly don't want to use this. */
  @nodeSync
  final def asyncGetAttached[T](node: Node[T]): T = asyncGetAttached$queued(node).get
  // noinspection ScalaWeakerAccess
  final def asyncGetAttached$queued[T](node: Node[T]): Node[T] = node.enqueueAttached

  /**
   * Enqueue a node on the graph that is completed when the given future completes and get the result asynchronously.
   * This is an easy way of waiting on the results of a future within a node without creating a sync-stack. Note that
   * this doesn't propagate xInfo, and therefore should only be used to integrate with frameworks which don't call back
   * into the graph.
   */
  @nodeSync
  final def asyncGetFuture[A](f: Future[A], timeoutMillis: Option[Int] = None): A = needsPlugin
  // noinspection ScalaUnusedSymbol
  final def asyncGetFuture$queued[A](f: Future[A], timeoutMillis: Option[Int] = None): NodeFuture[A] =
    AsyncInterop.future$queued(f, timeoutMillis)(sameThreadExecutionContext)

  private val sameThreadExecutionContext = ExecutionContext.fromExecutor(_.run())
}

/** Access to the [[NodeAPI]] methods for use in projects that do not depend on `platform`. */
object NodeAPI extends NodeAPI

@loom
trait CoreAPI
    extends NodeAPI
    with AsyncCollectionHelpers
    with AsyncImplicits
    with AsyncResultAPI
    with BreadcrumbRegistration
    with NodeFunctionImplicits {

  @nodeSync
  @nodeSyncLift
  def track[T](label: String, tpe: CrumbNodeType.CrumbNodeType)(@nodeLift @nodeLiftByName f: => T): T =
    track$withNode(label, tpe)(toNode(f _))
  def track$withNode[T](label: String, tpe: CrumbNodeType.CrumbNodeType)(f: Node[T]): T =
    EvaluationContext.track(f).get
  def track$queued[T](label: String, tpe: CrumbNodeType.CrumbNodeType)(f: Node[T]): Node[T] =
    EvaluationContext.track(f).enqueueAttached

  @nodeSync
  @nodeSyncLift
  def trackUI[T](label: String)(@nodeLift @nodeLiftByName f: => T): T = trackUI$withNode(label)(toNode(f _))
  // noinspection ScalaWeakerAccess
  def trackUI$withNode[T](label: String)(f: Node[T]): T =
    EvaluationContext.track(f).get
  def trackUI$queued[T](label: String)(f: Node[T]): Node[T] =
    EvaluationContext.track(f).enqueueAttached

  @nodeSync
  @nodeSyncLift
  def track[T](@nodeLift @nodeLiftByName f: => T): T = track$withNode(toNode(f _))
  def track$withNode[T](f: Node[T]): T =
    EvaluationContext.track(f).get
  def track$queued[T](f: Node[T]): NodeFuture[T] =
    EvaluationContext.track(f).enqueueAttached
  def track$queued[T](f: => T): NodeFuture[T] = track$queued(toNode(f _))

  @nodeSync
  @nodeSyncLift
  @expectingTweaks
  @scenarioIndependentInternal
  def given[T](tweaks: Tweak*)(@nodeLift @nodeLiftByName f: => T): T = EvaluationContext.givenL(Scenario(tweaks: _*), f)
  def given$withNode[T](tweaks: Tweak*)(f: Node[T]): T = EvaluationContext.given(Scenario(tweaks: _*), f).get
  def given$queued[T](tweaks: Tweak*)(f: Node[T]): NodeFuture[T] =
    EvaluationContext.given(Scenario(tweaks: _*), f).enqueueAttached
  def given$queued[T](tweaks: ImmSeq[Tweak], f: () => T): NodeFuture[T] =
    given$queued(tweaks)(toNode(f))

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentInternal
  def given[T](scenario: Scenario)(@nodeLift @nodeLiftByName f: => T): T = EvaluationContext.givenL(scenario, f)
  def given$withNode[T](scenario: Scenario)(f: Node[T]): T = EvaluationContext.given(scenario, f).get
  def given$queued[T](scenario: Scenario)(f: Node[T]): Node[T] = EvaluationContext.given(scenario, f).enqueueAttached
  // noinspection ScalaUnusedSymbol
  def given$queued[T](scenario: Scenario, f: => T): NodeFuture[T] =
    EvaluationContext.given(scenario, toNode(f _)).enqueueAttached

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentInternal
  def given[T](tweaks: Iterable[Tweak])(@nodeLift @nodeLiftByName f: => T): T =
    EvaluationContext.givenL(Scenario(tweaks), f)
  def given$withNode[T](tweaks: Iterable[Tweak])(f: Node[T]): T = EvaluationContext.given(Scenario(tweaks), f).get
  def given$queued[T](tweaks: Iterable[Tweak])(f: Node[T]): Node[T] =
    EvaluationContext.given(Scenario(tweaks), f).enqueueAttached
  // noinspection ScalaUnusedSymbol (compiler plugin forwards to this def)
  def given$queued[T](tweaks: Iterable[Tweak], f: => T): NodeFuture[T] = given$queued(tweaks)(toNode(f _))

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentInternal
  def givenNested[T](scenarios: Seq[Scenario])(@nodeLift @nodeLiftByName f: => T): T =
    givenNested$withNode(scenarios)(toNode(f _))
  def givenNested$withNode[T](scenarios: Seq[Scenario])(f: Node[T]): T =
    EvaluationContext.given(scenarios, f).get
  def givenNested$queued[T](scenarios: Seq[Scenario])(f: Node[T]): Node[T] =
    EvaluationContext.given(scenarios, f).enqueueAttached
  def givenNested$queued[T](scenarios: Seq[Scenario], f: => T): NodeFuture[T] =
    givenNested$queued(scenarios)(toNode(f _))

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentInternal
  def givenIf[A](tweakCondition: Boolean)(@nodeLift @nodeLiftByName tweaks: => Seq[Tweak])(
      @nodeLift @nodeLiftByName f: => A): A =
    givenIf$withNode[A](tweakCondition)(toNode(tweaks _))(toNode(f _))
  // noinspection ScalaWeakerAccess
  def givenIf$withNode[A](tweakCondition: Boolean)(tweaks: Node[Seq[Tweak]])(f: Node[A]): A =
    if (tweakCondition) EvaluationContext.given(Scenario(tweaks.get), f).get else f.get
  // noinspection ScalaUnusedSymbol
  def givenIf$queued[A](tweakCondition: Boolean)(tweaks: Node[Seq[Tweak]])(f: Node[A]): Node[A] =
    if (tweakCondition) {
      val outer = tweaks.map(ts => EvaluationContext.given(Scenario(ts), f))
      val intermediate = new CompletableRawNode[A] {
        self =>
        override def run(ec: OGSchedulerContext): Unit = {
          // 1. run at first to schedule the outer node
          outer.attach(scenarioStack())
          ec.enqueue(outer)
          outer.continueWith(
            (eq: EvaluationQueue, _: NodeTask) => {
              // 2. when outer is completed, use the generated node and set continuation as intermediate node
              // N.B. we don't need to attach a ScenarioStack again, because the given already does the `attach`.
              combineInfo(outer, eq)
              eq.enqueue(self, outer.result)
              outer.result.continueWith(self, eq)
            },
            ec
          )
        }

        override def onChildCompleted(eq: EvaluationQueue, task: NodeTask): Unit = {
          // 3. last step, use the result of the generated node to complete this node
          val resNode = outer.result
          combineInfo(resNode, eq)
          if (resNode.isDoneWithResult) completeWithResult(resNode.result, eq)
          else if (resNode.isDoneWithException) completeWithException(resNode.exception, eq)
          else throwNodeCompletionException(resNode)
        }
      }

      intermediate.enqueue
    } else {
      f.enqueue
    }

  /**
   * Methods annotated with @nodeLift will be replaced with method$node method In addition the argument will be replaced
   * with arg$node
   */
  @nodeLift
  @parallelizable
  implicit def value2TweakTarget[A](@unused value: A): TweakTargetKey[A, Entity => A] = needsPlugin
  implicit def value2TweakTarget$node[A](node: NodeKey[A]): TweakTargetKey[A, Entity => A] =
    new InstancePropertyTarget(node)

  implicit class OptionFlowNullSupport[T](o: Option[T]) {
    def getOrThrowFlowNull: T = o.getOrElse(throw new FlowNullException())
  }

  /** These functions are mostly for testing code */
  @nodeSync
  def delay(msDelay: Long): Unit = delay$queued(msDelay).get$ // NOT Thread.sleep - need to support Cancellation
  // noinspection ScalaWeakerAccess
  def delay$queued(msDelay: Long): NodeFuture[Unit] = {
    val promise = NodePromise[Unit](NodeTaskInfo.Delay)
    delayPromise(promise, msDelay, TimeUnit.MILLISECONDS, ())
    promise.underlyingNode
  }

  // Delay with forced dependency just to be careful
  @nodeSync
  def delay[X](msDelay: Long, x: X): X = delay$queued(msDelay, x).get // NOT Thread.sleep - need to support Cancellation
  // noinspection ScalaWeakerAccess
  def delay$queued[X](msDelay: Long, x: X): Node[X] = {
    val promise = NodePromise[X](NodeTaskInfo.Delay)
    delayPromise(promise, msDelay, TimeUnit.MILLISECONDS, x)
    promise.underlyingNode
  }

  def attachWarning(warning: String): Unit = {
    attachWarning(new MessageAsWarning(warning))
  }

  def attachWarning(warning: Warning): Unit = {
    EvaluationContext.currentNode.attach(warning)
  }

  def attachNote(note: Note): Unit = {
    EvaluationContext.currentNode.attach(note)
  }

  def attachNote(note: String): Unit = {
    EvaluationContext.currentNode.attach(new MessageAsNote(note))
  }

  @nodeSync
  @nodeSyncLift
  def withNotes[T](@nodeLiftByName @nodeLift f: => T): (Set[Note], T) = new ExtractNotesNode(toNode(f _)).get
  // noinspection ScalaUnusedSymbol
  def withNotes$withNode[T](f: Node[T]): (Set[Note], T) = new ExtractNotesNode(f).get
  // noinspection ScalaUnusedSymbol
  def withNotes$queued[T](f: Node[T]): Node[(Set[Note], T)] = new ExtractNotesNode(f).enqueue

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def NodeTry[T](@nodeLiftByName @nodeLift f: => T): NodeTry[T] = NodeTry$withNode(toNode(f _))
  // noinspection ScalaWeakerAccess
  def NodeTry$withNode[T](f: Node[T]): NodeTry[T] = new NodeTryNode(f).get
  // noinspection ScalaUnusedSymbol
  def NodeTry$queued[T](f: Node[T]): Node[NodeTry[T]] = new NodeTryNode(f).enqueue
  // noinspection ScalaUnusedSymbol
  def NodeTry$queued[T](f: => T): NodeFuture[NodeTry[T]] = new NodeTryNode(toNode(f _)).enqueue

  @deprecating(
    "Use NodeTry to handle RT exceptions; otherwise use asyncResult, which is @impure.  Use of nodeResult in @nodes is COMPLETELY UNSUPPORTED, and you should understand the consequences.")
  @aspirationallyimpure
  @nodeSync
  @nodeSyncLift
  def nodeResult[T](@nodeLiftByName @nodeLift f: => T): NodeResult[T] = nodeResult$withNode(toNode(f _))
  // noinspection ScalaWeakerAccess
  def nodeResult$withNode[T](f: Node[T]): NodeResult[T] = new NodeResultNode(f).get
  // noinspection ScalaWeakerAccess
  def nodeResult$queued[T](f: Node[T]): Node[NodeResult[T]] = new NodeResultNode(f).enqueue
  // noinspection ScalaUnusedSymbol
  def nodeResult$queued[T](f: => T): NodeFuture[NodeResult[T]] = nodeResult$queued(toNode(f _))

  @impure
  @nodeSync
  @nodeSyncLift
  def withTimeout[T](timeout: Long)(@nodeLiftByName @nodeLift f: => T): TimeoutResult[T] =
    withTimeout$withNode(timeout)(toNode(f _))
  // noinspection ScalaWeakerAccess
  def withTimeout$withNode[T](timeout: Long)(f: Node[T]): TimeoutResult[T] =
    new NodeResultCancelNode(f, EvaluationContext.cancelScope.childScope(), timeout).map(TimeoutResult(timeout)).get
  // noinspection ScalaUnusedSymbol
  def withTimeout$queued[T](timeout: Long)(f: Node[T]): Node[TimeoutResult[T]] =
    new NodeResultCancelNode(f, EvaluationContext.cancelScope.childScope(), timeout).map(TimeoutResult(timeout)).enqueue

  /**
   * Run a block of code as a node, within a child scenario stack, with the given cancellation scope. This will be
   * rewritten by the optimus plugin into code that accomplishes this.
   *
   * @param cs
   *   The cancellation scope.
   * @param f
   *   The body to execute.
   * @tparam T
   *   The result type of the block.
   * @return
   *   The result of executing the block.
   */
  @impure
  @nodeSync
  @nodeSyncLift
  def withCancellation[T](cs: CancellationScope)(@nodeLiftByName @nodeLift f: => T): Option[T] =
    withCancellation$withNode(cs)(toNode(f _))
  // noinspection ScalaWeakerAccess
  def withCancellation$withNode[T](cs: CancellationScope)(f: Node[T]): Option[T] =
    new NodeResultCancelNode(f, cs, 0).get
  // noinspection ScalaWeakerAccess
  def withCancellation$queued[T](cs: CancellationScope)(f: Node[T]): Node[Option[T]] =
    new NodeResultCancelNode(f, cs, 0).enqueue
  // noinspection ScalaUnusedSymbol
  def withCancellation$queued[T](cs: CancellationScope, f: => T): NodeFuture[Option[T]] =
    withCancellation$queued(cs)(toNode(f _))

  @nodeSync
  @nodeSyncLift
  def withProgress[T](weight: Double = 1, message: String = null, reportingIntervalMs: Long = -1)(
      @nodeLiftByName @nodeLift f: => T): T =
    withProgress$withNode(weight, message, reportingIntervalMs)(toNode(f _))
  // noinspection ScalaWeakerAccess (non-loom can be referenced externally)
  def withProgress$withNode[T](weight: Double, message: String, reportingIntervalMs: Long)(f: Node[T]): T =
    new NodeResultProgressNode(f, weight, message, reportingIntervalMs).get
  // noinspection ScalaWeakerAccess (non-loom can be referenced externally)
  def withProgress$queued[T](weight: Double, message: String, reportingIntervalMs: Long)(f: Node[T]): Node[T] =
    new NodeResultProgressNode(f, weight, message, reportingIntervalMs).enqueue
  // noinspection ScalaUnusedSymbol
  def withProgress$queued[T](weight: Double, message: String, reportingIntervalMs: Long, f: => T): NodeFuture[T] =
    withProgress$queued(weight, message, reportingIntervalMs)(toNode(f _))

  // only used in UI bindings with optimus.ui.server.graph.ProgressChannel (to indicate what handlers can be cancelled)
  @nodeSync
  @nodeSyncLift
  def enableCancellation[T](@nodeLiftByName @nodeLift f: => T): T = enableCancellation$withNode(toNode(f _))
  // noinspection ScalaWeakerAccess
  def enableCancellation$withNode[T](f: Node[T]): T = enableCancellation$n(f).get
  // noinspection ScalaUnusedSymbol
  def enableCancellation$queued[T](f: Node[T]): Node[T] = enableCancellation$n(f).enqueueAttached
  private def enableCancellation$n[T](f: Node[T]): Node[T] = {
    val ec = EvaluationContext.current
    val ss = ec.scenarioStack()
    val finalSS = if (ss.progressTracker ne null) {
      val params = ss.progressTracker.params
      val trackerWithCancellation = ProgressTracker.newTracker(params.copy(cancellable = true))
      ss.withProgressTracker(trackerWithCancellation)
    } else ss

    f.attach(finalSS)
    f
  }

  @nodeSync
  @nodeSyncLift
  def withProgressListener[T](listener: ProgressListener, reportingIntervalMs: Long = -1)(
      @nodeLiftByName @nodeLift f: => T): T =
    withProgressListener$withNode(listener, reportingIntervalMs)(toNode(f _))
  // noinspection ScalaWeakerAccess
  def withProgressListener$withNode[T](listener: ProgressListener, reportingIntervalMs: Long)(f: Node[T]): T =
    new NodeResultProgressNode(f, listener, reportingIntervalMs).get
  // noinspection ScalaUnusedSymbol
  def withProgressListener$queued[T](listener: ProgressListener, reportingIntervalMs: Long)(f: Node[T]): Node[T] =
    new NodeResultProgressNode(f, listener, reportingIntervalMs).enqueue

  def logProgress(message: String): Unit = {
    val progressTracker = EvaluationContext.scenarioStack.progressTracker

    if (progressTracker ne null)
      progressTracker.sendProgressMessage(message)
  }

  final def tickleProgress(amt: Double): Unit = {
    ProgressTracker.tickle(amt)
  }

  /**
   * Evaluate f. If an exception is thrown, invoke the handler, to log it or perform some other operation for side
   * effects, and (no matter what the handler does) rethrow the exception. Note that the handler receives a full
   * NodeResult, including warnings if present.
   */
  @nodeSync
  @nodeSyncLift
  def withRethrow[T](@nodeLiftByName @nodeLift f: => T)(@nodeLiftByName @nodeLift handler: NodeResult[T] => Unit): T =
    withRethrow$queued(toNode(f _))(toNodeFactory(handler)).get
  // noinspection ScalaUnusedSymbol
  def withRethrow$withNode[T](f: Node[T])(handler: NodeResult[T] => Node[Unit]): T =
    withRethrow$queued(f)(handler).get
  // noinspection ScalaWeakerAccess
  def withRethrow$queued[T](f: Node[T])(handler: NodeResult[T] => Node[Unit]): Node[T] =
    new NodeResultRethrow[T](f, handler).enqueue
  // noinspection ScalaUnusedSymbol
  def withRethrow$queued[T](f: => T, handler: NodeResult[T] => Unit): NodeFuture[T] =
    withRethrow$queued(toNode(f _))(toNodeFactory(handler))

  /**
   * lifts the lambda "() => B" in to a node factory "() => Node[B]"
   */
  @nodeSyncLift
  def liftNode[B](@nodeLift @withNodeClassID f: () => B): () => Node[B] = () => toNode(f)
  // noinspection ScalaUnusedSymbol
  final def liftNode$withNode[B](f: () => Node[B]): () => Node[B] = f

  /**
   * lifts the lambda "A => B" in to a node factory "A => Node[B]"
   */
  @nodeSyncLift
  def liftNode[A, B](@nodeLift @withNodeClassID f: A => B): A => Node[B] = toNodeFactory(f)
  // noinspection ScalaUnusedSymbol
  final def liftNode$withNode[A, B](f: A => Node[B]): A => Node[B] = f

  /**
   * lifts the lambda "(A1,A2) => B" in to a node factory "(A1,A2) => Node[B]"
   */
  @nodeSyncLift
  def liftNode[A1, A2, B](@nodeLift @withNodeClassID f: (A1, A2) => B): (A1, A2) => Node[B] = liftNode$withNode(
    toNodeFactory(f))
  // noinspection ScalaWeakerAccess
  final def liftNode$withNode[A1, A2, B](f: (A1, A2) => Node[B]): (A1, A2) => Node[B] = f

  private val liftedIdentifyForAny = (a: Any) => new AlreadyCompletedNode(a)
  def liftedIdentity[A]: A => Node[A] = liftedIdentifyForAny.asInstanceOf[A => Node[A]] // exploiting erasure

  // noinspection ConvertExpressionToSAM cause it breaks priql's serialization!
  // workaround for https://github.com/scala/bug/issues/10222 / https://github.com/scala/bug/issues/8541
  // suggested by core compiler team from Lightbend
  implicit def comparableToOrdering[A <: Comparable[A]]: Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = x.compareTo(y)
  }
}

object CoreAPI extends AsyncResultAPI {
  @nodeSyncLift
  def nodify[T](@nodeLiftByName @nodeLift f: => T): Node[T] = nodify$withNode(toNode(f _))
  // noinspection ScalaWeakerAccess (non-loom can be referenced externally)
  final def nodify$withNode[T](n: Node[T]): Node[T] = n

  @nodeSyncLift
  def nodifyProperty[T](@propertyNodeLift @nodeLiftByName @nodeLift f: => T): PropertyNode[T] = needsPlugin
  // noinspection ScalaUnusedSymbol
  final def nodifyProperty$withNode[T](n: PropertyNode[T]): PropertyNode[T] = {
    StargazerNodeHotSwap.getMagicKey(n)
  }

  @deprecating("Use NodeTry to handle RT exceptions; otherwise use asyncResult.")
  @aspirationallyimpure
  @nodeSync
  @nodeSyncLift
  def nodeResult[T](@nodeLiftByName @nodeLift f: => T): NodeResult[T] = nodeResult$withNode(toNode(f _))
  // noinspection ScalaWeakerAccess
  def nodeResult$withNode[T](f: Node[T]): NodeResult[T] = new NodeResultNode(f).get
  // noinspection ScalaWeakerAccess
  def nodeResult$queued[T](f: Node[T]): Node[NodeResult[T]] = new NodeResultNode(f).enqueue
  // noinspection ScalaUnusedSymbol
  def nodeResult$queued[T](f: => T): NodeFuture[NodeResult[T]] = nodeResult$queued(toNode(f _))

  @deprecating(
    "Use NodeTry to handle RT exceptions; otherwise use asyncResult, which is @impure.  Use of nodeResult in @nodes is COMPLETELY UNSUPPORTED, and you should understand the consequences.")
  @aspirationallyimpure
  @nodeSync
  @nodeSyncLift
  def nodeResultCurrentCS[T](@nodeLiftByName @nodeLift f: => T): NodeResult[T] =
    nodeResultCurrentCS$withNode(toNode(f _))
  // noinspection ScalaWeakerAccess
  def nodeResultCurrentCS$withNode[T](f: Node[T]): NodeResult[T] = new NodeResultNode(f).get
  // noinspection ScalaWeakerAccess
  def nodeResultCurrentCS$queued[T](f: Node[T]): NodeFuture[NodeResult[T]] =
    new NodeResultNode(f, EvaluationContext.scenarioStack.cancelScope).enqueue
  // noinspection ScalaUnusedSymbol
  def nodeResultCurrentCS$queued[T](f: => T): NodeFuture[NodeResult[T]] = nodeResultCurrentCS$queued(toNode(f _))

  /** General timer to be shared for all optimus applications */
  lazy val optimusScheduledThreadPool: ScheduledExecutorService = {
    val pool = new ScheduledThreadPoolExecutor(
      3,
      new ThreadFactory {
        private val count = new AtomicInteger(0)
        override def newThread(r: Runnable): Thread = {
          val t = Executors.defaultThreadFactory.newThread(r)
          t.setName(s"OPTIMUS_TIMER_THREAD_${count.getAndIncrement()}")
          t.setDaemon(true)
          t
        }
      }
    )
    pool.setKeepAliveTime(1, TimeUnit.MINUTES)
    pool.allowCoreThreadTimeOut(true)
    pool.setRemoveOnCancelPolicy(true)
    pool
  }

  /* Probably not the API you are looking for, this is meant for internal graph uses only! */
  private[optimus] def delayPromise[X](promise: NodePromise[X], msDelay: Long, unit: TimeUnit, op: => X): Unit = {
    internalScheduledThreadPool.schedule(
      new Runnable {
        override def run(): Unit = promise.complete(Success(op))
      },
      msDelay,
      unit)
  }

  /** A dedicated thread pool for internal and quick (the thread pool has size one!) graph tasks, such as [delay] */
  private lazy val internalScheduledThreadPool: ScheduledExecutorService = {
    val pool = new ScheduledThreadPoolExecutor(
      1,
      new ThreadFactory {
        private val count = new AtomicInteger(0)
        override def newThread(r: Runnable): Thread = {
          val t = Executors.defaultThreadFactory.newThread(r)
          t.setName(s"OPTIMUS_SCHEDULER_THREAD_${count.getAndIncrement()}")
          t.setDaemon(true)
          t
        }
      }
    )
    pool.setKeepAliveTime(1, TimeUnit.MINUTES)
    pool.allowCoreThreadTimeOut(true)
    pool.setRemoveOnCancelPolicy(true)
    pool
  }
}
