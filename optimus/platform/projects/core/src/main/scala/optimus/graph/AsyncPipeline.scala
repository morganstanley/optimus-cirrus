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
package optimus.graph

import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.annotations.scenarioIndependentTransparent
import optimus.platform.util.Log
import optimus.platform._
import optimus.platform.throttle.Throttle
import optimus.platform.PluginHelpers.toNodeFactory
import optimus.platform.throttle.OscillatingThrottleLimiter
import optimus.platform.throttle.SimpleThrottleLimiter
import optimus.scalacompat.collection.IterableLike

import scala.collection.GenTraversableOnce
import scala.collection.mutable
import scala.collection.compat._

/*

  A pipeline comprises a series of collection operations like map and flatMap, which can have async closures.

  There is a single CommutativeAggregatorNode (CAN - think of it as a SequenceNode but more general) controlling the execution
  of the entire pipeline. As in a regular SequenceNode, there is an input collection from which Iterations are constructed.
  Recall that the Iteration holds a Node, which will deliver the result of the closure operation on the input value.

  Unlike the input collection in a regular SequenceNode,this one is a queue, initially populated with the input collection of pipeline.
  In generality, there are multiple queues, but assume one for now.

  The elements of this queue are of class InputElem, which defines both an input value and a StageManager, who
  knows what we're going to do with the input value. I.e. it knows how to create the Node for the CAN#Iteration.
  Complicating the pun/metaphor, the StageManager is really a linked list, pointing to the StageManager for the next
  operation in the pipeline.

  The CAN takes the Iteration, launches the Node and listens for completion, as a regular SN does.
  Unlike the SN, it passes the results to consumeIteration immediately, rather than buffering them to return in
  input order.

  The Node is Node[Intermediate[_]] whose .result will hold the result of the closure operation, as well as the current StageManager.
  Intermediate#accumulateAndEmitElems decides what to do with these results.  In the simplest case, it doesn't do any
  accumulating, but just packages the results in new InputElems, now embedding the tail of the StageManager, so they
  can be added to the input queue, from which Iterations will be built, eventually dumping into an InputElems pointing to the
  following StageManager and enqueued.

  In the second simplest case, accumulateAndEmitElems takes care of the ordering that SN no longer does for us, keeping
  a buffer of placeholders and releasing them as the head of the buffer completes.  This way, we can maintain order
  for each StageManager separately, allowing the CAN to dispatch Iterations from any state in any order.  The
  map, flatMap, filter, etc. stages will create Intermediates of this sort.

  The scan operation allows for more complicated buffering, which might, for example, accumulate batches according to a
  weighting algorithm.

  Since these StageManagers are stateful, it's important that they don't leak into RT code.  That's the reason for
  StageManagerCreator, which contains a composition of immutable Stage descriptions and can create the mutable
  StageManager on demand.

  As noted above, there might be multiple queues.  Specifically, if favorLaterStages=true, then each StageManager will
  maintain its own input queue, and AsyncPipeline will draw from the last non-empty queue in the list of managers.  So
  if we had a pipeline
       map -> flatMap -> ...
  inputs would initially be dumped into map's queue.  As each map closure completes, the results are added to flatMaps's
  queue.  As long as there are flatMap closures to run, the pipeline will run them, falling back to map closures only if
  there's nothing in the flatMap queue.  This strategy minimizes the number of accumulated intermediate results, but at
  the same time it will reduce the concurrency of the earlier operations.


  An individual Stage can be built from utilities in the Stages object, e.g.
     val m = Stages.map(i: Int => i+1)
     val f = Stages.filter(i: Int = (i%2)==0)
  and then combined, along with a sink to create a StageManagerCreator
     val SMC = m :: f :: toColl[List]
  which can be run on actual input as
     inputs.pipeline.run(scm)
  The the Stages and StageManagerCreators can be stitched together and re-used on different input as desired, in a vaguely
  transducerish manner.


 */

sealed class AsyncPipeline[A, CC <: Iterable[A]](
    c: CC with IterableLike[A, CC],
    workMarker: ProgressMarker,
    maxConcurrency: Int,
    runtimeCheck: Boolean)
    extends AsyncBase[A, CC](c, workMarker, maxConcurrency, runtimeCheck) {
  def this(c: CC with IterableLike[A, CC]) = this(c, null, Int.MaxValue, false)
  def this(c: CC with IterableLike[A, CC], runtimeCheck: Boolean) = this(c, null, Int.MaxValue, runtimeCheck)

  import AsyncPipeline._

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def run[Result](sc: StageManagerCreator[A, Result]): Result = run$newNode(sc).get
  def run$queued[Result](sc: StageManagerCreator[A, Result]): Node[Result] = run$newNode(sc).enqueue
  // noinspection ScalaUnusedSymbol
  def run$withNode[Result](sc: StageManagerCreator[A, Result]): Result = run$newNode(sc).get
  def run$newNode[Result](sc: StageManagerCreator[A, Result]): Node[Result] = runNode(c, workMarker, maxConcurrency, sc)

}

object AsyncPipeline extends Log {

  def runNode[A, CC <: Iterable[A], Result](
      c: CC,
      workMarker: ProgressMarker,
      maxConcurrency: Int,
      sc: StageManagerCreator[A, Result],
      favorLaterStages: Boolean = false
  ): Node[Result] = {
    val manager: StageManager[A, Result] = sc()
    // This node will complete with the accumulation so far, plus any queue of unaccumulated mapped values
    val can: Node[Result] =
      new CommutativeAggregatorNode[Intermediate[Result], Result](workMarker, maxConcurrency, -1) {
        var finalResult: Option[Result] = None

        // If we're not favoring later stages, then we have one global queue, onto which all stages will feed.
        // If we are favoring later stages, then each stage gets its own input queue
        val q: StagedQueue[Result] = if (favorLaterStages) manager else manager.globalQueue

        // Queue initially holds input elements - with the last marked as such
        // val q = mutable.Queue.empty[InputElem[Result]]
        c.foreach(a => q.enqueue(ElemImpl(a, manager)))
        q.enqueue(EndMarker(manager))

        // This is called (under lock) after every iteration completion, even if it has previously returned false
        override def hasNextIteration: Boolean = !manager.isEmpty

        override def nextIteration: Iteration = {
          val elem = q.dequeue()
          log.debug(s"$this nextIteration mapping $elem (${q.remaining()} remaining, q=${q.debug}")
          new Iteration(elem.mapped)
        }

        // Consume queued up completed iterations if we're running in order
        override def consumeIteration(i: Iteration): Unit = {
          val elems: Iterable[Elem[Result]] = i.node.result.accumulateAndEmit
          elems.foreach {
            case FinalElem(r) =>
              log.debug(s"$this consumeIteration got FinalElem($r)")
              assert(elems.size == 1)
              finalResult = Some(r)
            case e: InputElem[Result] =>
              log.debug(s"$this consumeIteration enqueuing $e")
              if (favorLaterStages)
                e.enqueueAtStage()
              else
                q.enqueue(e)
            case _ =>
              throw new IllegalStateException("This should not happen")
          }
        }
        override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.FoldLeft
        // this will be called when there's nothing in the queue and no tasks running
        override def getFinalResult: Result = finalResult.getOrElse(manager.empty)
      }
    can

  }

  private sealed trait Elem[Result] {
    type A
  }

  private sealed trait InputElem[Result] extends Elem[Result] {
    def mapped: Node[Intermediate[Result]]
    def manager: StageManager[_, Result]
    final def enqueueAtStage(): Unit = manager.enqueue(this)
  }

  // An input value, packaged with its processing stage, which handles the mapping.
  private final case class ElemImpl[A, Result](x: A, val manager: StageManager[A, Result]) extends InputElem[Result] {
    override def mapped: Node[Intermediate[Result]] = manager.mappedIntermediate(x)
  }

  private final case class EndMarker[A, Result](val manager: StageManager[A, Result]) extends InputElem[Result] {
    override def mapped: Node[Intermediate[Result]] = manager.mappedIntermediateLast()
  }

  // Special case element to hold a final result.
  private final case class FinalElem[Result](result: Result) extends Elem[Result]

  // Holds a mapped result.  We don't care what type of result is, as long as it can be converted into an iterable of
  // Elem[Result]
  private[AsyncPipeline] sealed trait Intermediate[Result] {
    // Note: accumulatedAndEmit is executed under SequenceNode lock.
    def accumulateAndEmit: Iterable[Elem[Result]]
  }

  // Mutable container for queued results when we're preserving order
  private class PlaceHolder[B] {
    // This will get populated when the map returns
    var result: Option[B] = None
  }

  // Linked list of queue - we dequeue from the last non-empty one.
  private trait StagedQueue[Result] {
    protected def nextQueue: StagedQueue[Result]

    protected val stageQueue = mutable.Queue.empty[InputElem[Result]]
    def name: String
    final def enqueue(e: InputElem[Result]): Unit = stageQueue.enqueue(e)
    def dequeue(): InputElem[Result] =
      if (nextQueue.isEmpty && stageQueue.nonEmpty) stageQueue.dequeue() else nextQueue.dequeue()
    def isEmpty: Boolean = stageQueue.isEmpty && nextQueue.isEmpty
    def remaining(): Int = stageQueue.size + nextQueue.remaining()
    def globalQueue: StagedQueue[Result] = nextQueue.globalQueue
    def debug: String = s"$name($stageQueue)" + nextQueue.debug
  }

  // A StageManager is a potentially mutable object that exists during a single run.  It knows who the
  // next StageManager is.
  private trait StageManager[A, Result] extends StagedQueue[Result] {
    stage =>
    protected type B // Map A elements to B
    protected type Z //
    protected def mapperNode(a: A): Node[B]
    // Next stage will convert Z's to ultimate Result type, possibly via intermediate stages.
    protected val next: StageManager[Z, Result]
    override final protected def nextQueue: StagedQueue[Result] = next
    def empty: Result = next.empty

    // Accumulate B's into zero or more Z's, possibly holding B's until some threshold is met, or possibly
    // expanding B's into multiple Z's, or both...
    protected def accumulate(b: collection.Seq[B], isLast: Boolean): collection.Seq[Z]

    // Return elements to enqueue, embedding the next stage, which knows how to process them.  If isLast==true,
    // then we should release any state we've been accumulating.
    final def elemsToEnqueue(b: collection.Seq[B], isLast: Boolean): collection.Seq[InputElem[Result]] = {
      val zs: collection.Seq[Z] = accumulate(b, isLast)
      val es = zs.map(ElemImpl(_, next))
      if (isLast) es :+ EndMarker(next) else es

    }

    private var seenLast = false
    private var running = 0
    // Possibly throttle mapper nodes
    protected val throttle: Option[Throttle]
    // Possibly maintain order of mapped results in a queue
    protected val orderedResults: Option[mutable.Queue[PlaceHolder[B]]]

    // Will be returned to mark that the last input element of a stage has been launched.  (Others might still be running)
    // Note: accumulatedAndEmit is executed under SequenceNode lock.
    private class IntermediateLastInStage extends Intermediate[Result] {
      override def accumulateAndEmit: Iterable[Elem[Result]] = {
        running -= 1
        orderedResults.fold[Iterable[Elem[Result]]] {
          // Running unordered, so, while this is the last input element, there may be more map results
          val es = stage.elemsToEnqueue(collection.Seq.empty, isLast = running == 0)
          log.debug(s"$this accumulateAndEmit 2: ao=empty empty, $running -> $es")
          es
        } { placeholderQueue =>
          if (running == 0) {
            // Dequeue all placeholders that have been completed.  That should actually be all of them, since nothing
            // else is running.
            val bs = placeholderQueue.dequeueAll(_.result.isDefined).map(_.result.get)
            assert(placeholderQueue.isEmpty)
            val es = stage.elemsToEnqueue(bs, isLast = true)
            log.debug(s"$this accumulateAndEmit 3 : ao=empty $bs $running -$es")
            es
          } else {
            // Some mapped results are still outstanding, so don't enqueue anything
            val es = stage.elemsToEnqueue(collection.Seq.empty, isLast = false)
            log.debug(s"$this accumulateAndEmit 4: ao=empty $running -> $es")
            es
          }
        }
      }
    }

    // Create an intermediate node marking the last input for this stage.  Record that we've seen it.
    def mappedIntermediateLast(): Node[Intermediate[Result]] = {
      running += 1
      assert(!seenLast, "Apparently two lasts")
      seenLast = true
      new AlreadyCompletedNode(new IntermediateLastInStage)
    }

    // This will be created after completion of the mapper.  For the unordered case, we just emit that result.
    private class IntermediateMappedUnordered(b: B) extends Intermediate[Result] {
      override def accumulateAndEmit: Iterable[Elem[Result]] = {
        running -= 1
        // Unordered - process this result and enqueue any emitted batches.  We know we're last
        // if processing has finished for the last element, and no more results are in flight
        val es = stage.elemsToEnqueue(collection.Seq(b), seenLast && running == 0)
        log.debug(s"$this accumulatAndEmit 6 unordered seenlast=$seenLast, running=$running -> $es")
        es
      }
    }

    // This will be created after the comppletion of the mapper.  Since we're maintaining order, the result should be
    // placed into its waiting placeholder. If we happen to have filled the first placeholder in the queue, that
    // might cause us to release other completed results.
    private class IntermediateMappedOrdered(b: B, h: PlaceHolder[B], hs: mutable.Queue[PlaceHolder[B]])
        extends Intermediate[Result] {
      override def accumulateAndEmit: Iterable[Elem[Result]] = {
        running -= 1
        // Fill in the result we just got
        h.result = Some(b)
        val es = mutable.ArrayBuffer.empty[Elem[Result]]
        log.debug(s"$this accumulateAndEmit 7 unordered $b seenlast=$seenLast, running=$running")
        // Accumulate all complete intermediate results
        while (hs.headOption.exists(_.result.isDefined)) {
          val b: B = hs.dequeue().result.get
          val isLast = seenLast && running == 0 && hs.isEmpty
          es ++= stage.elemsToEnqueue(collection.Seq(b), isLast)
        }
        log.debug(s"$this accumulatAndEmit 8 ordered seenlast=$seenLast, running=$running -> $es")
        es
      }
    }

    // This runs under lock, to create the node that will be enqueued by SequenceNode
    def mappedIntermediate(a: A): Node[Intermediate[Result]] = {
      log.debug(s"$this mappedIntermediate $this got $a (seenLast=$seenLast)")
      running += 1
      val bNode: Node[B] = mapperNode(a)
      val intermediateNode: Node[Intermediate[Result]] = orderedResults match {
        case None =>
          bNode.map(new IntermediateMappedUnordered(_))
        case Some(hs) =>
          val h = new PlaceHolder[B] // empty
          hs.enqueue(h)
          bNode.map(new IntermediateMappedOrdered(_, h, hs))
      }
      // Possibly throttle
      throttle match {
        case None    => intermediateNode
        case Some(t) => t.apply$queued(asNode.apply0$withNode(intermediateNode))
      }
    }
  }

  // A StageCreator is an immutable object that creates StageManagers on demand, for a run.
  sealed trait StageManagerCreator[A, Result] {
    // Combine previous Stage into a creator of deeper stages.
    final def ::[AA](stage: Stage[AA, A]): StageManagerCreator[AA, Result] = stage.feedsTo(this)
    private[AsyncPipeline] def apply(): StageManager[A, Result]
  }

  // Segment in the pipeline. Can create a StageCreator that can create Stages that consume
  // A's and emit Z's (possibly via intermediate B's) into a Stage created by the next StageCreator.
  trait Stage[A, B] {
    private[AsyncPipeline] def feedsTo[Result](next: StageManagerCreator[B, Result]): StageManagerCreator[A, Result]
    final def toColl[CC[_]](cbf: Factory[B, CC[B]]): StageManagerCreator[A, CC[B]] =
      feedsTo(new SinkStageManagerCreator[B, CC](cbf))
  }

  private class SinkStageManagerCreator[B, CC[_]](cbf: Factory[B, CC[B]]) extends StageManagerCreator[B, CC[B]] {
    override private[AsyncPipeline] def apply(): StageManager[B, CC[B]] = new SinkStageManager[B, CC](cbf)
  }

  private trait ComposableStage[A, B] extends Stage[A, B] {
    private[AsyncPipeline] final def compose[C](stage: Stage[B, C]): ComposableStage[A, C] =
      new CompoundStageImpl(this, stage)
  }

  private class CompoundStageImpl[A, I, B](prev: Stage[A, I], curr: Stage[I, B]) extends ComposableStage[A, B] {
    private[AsyncPipeline] def feedsTo[Result](next: StageManagerCreator[B, Result]): StageManagerCreator[A, Result] = {
      prev.feedsTo(curr.feedsTo(next))
    }
  }

  final private case class EmptyStage[Z](ccyMax: Int, isOrdered: Boolean) extends ComposableStage[Z, Z] {
    override private[AsyncPipeline] def feedsTo[Result](
        next: StageManagerCreator[Z, Result]): StageManagerCreator[Z, Result] = next
  }

  private def emptyStage[Z] = new EmptyStage[Z](-1, false)

  private final class NilStageManager[Result](lazyEmptyResult: => Result) extends StageManager[Result, Result] {
    stage =>
    override val name = "Nil"
    override def dequeue(): InputElem[Result] = stageQueue.dequeue()
    override def isEmpty: Boolean = stageQueue.isEmpty
    override def remaining(): Int = stageQueue.size
    override def globalQueue: StagedQueue[Result] = this
    override def debug: String = name
    override def empty: Result = lazyEmptyResult
    override protected val orderedResults: Option[mutable.Queue[PlaceHolder[Result]]] = None
    override type B = Result
    override type Z = Result
    override def mapperNode(x: Result) = throw new NotImplementedError()
    override val next = this
    override protected def accumulate(b: collection.Seq[Result], isLast: Boolean) = throw new NotImplementedError()
    override val throttle: Option[Throttle] = None
    override def mappedIntermediateLast(): Node[Intermediate[Result]] =
      new AlreadyCompletedNode[Intermediate[Result]](new Intermediate[Result] {
        override def accumulateAndEmit: Iterable[Elem[Result]] = collection.Seq.empty
      })
    override def mappedIntermediate(a: Result): Node[Intermediate[Result]] =
      new AlreadyCompletedNode[Intermediate[Result]](new Intermediate[Result] {
        val accumulateAndEmit: Iterable[Elem[Result]] = collection.Seq(FinalElem(a))
      })

  }

  // Wrap the node with one to which a ss may be safely attached.
  private def switchState[A](ss: ScenarioState, node: Node[A]): Node[A] = new CompletableNode[A] {
    override def run(ec: OGSchedulerContext): Unit = {
      val withSS = AdvancedUtils.givenFullySpecifiedScenario$newNode(ss, null)(node)
      ec.enqueue(withSS)
      withSS.continueWith(this, ec)
    }
    override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
      completeFromNode(child.asInstanceOf[Node[A]], eq)
    }
  }

  // Most of the guts of a Stage.
  private abstract class StageImpl[A, B, Z](max: Int = -1, min: Int = -1, ordered: Boolean = false)
      extends Stage[A, Z] {
    type BB = B
    type ZZ = Z
    private val ss = AdvancedUtils.currentScenarioState() // Capture scenario state on creation
    abstract protected class StageBase[Result](createNextStage: StageManagerCreator[ZZ, Result])
        extends StageManager[A, Result] {
      val name: String = this.getClass.toString
      override protected final type B = BB
      override protected final type Z = ZZ
      override final val orderedResults = if (ordered) Some(mutable.Queue.empty[PlaceHolder[B]]) else None
      override protected final val throttle =
        if (max > 0)
          if (min > 0) {
            require(max > min, "Max concurrency must be more than minimum concurrency!")
            Some(new Throttle(new OscillatingThrottleLimiter(max, min)))
          } else Some(new Throttle(new SimpleThrottleLimiter(max)))
        else None
      override protected final val next = createNextStage()
      override protected final def mapperNode(a: A): Node[BB] = switchState(ss, mapper(a))
      protected def mapper(a: A): Node[B]
      override protected def accumulate(b: collection.Seq[B], isLast: Boolean): collection.Seq[Z]
    }
    override final def feedsTo[Result](next: StageManagerCreator[Z, Result]): StageManagerCreator[A, Result] =
      new StageManagerCreator[A, Result] {
        override private[AsyncPipeline] def apply(): StageManager[A, Result] = newStage(next)
      }

    def newStage[Result](next: StageManagerCreator[Z, Result]): StageManager[A, Result]
  }

  class Stages private[AsyncPipeline] (max: Int = -1, min: Int = -1, ordered: Boolean = false) {
    @nodeSyncLift
    def scan[A, BB, State, ZZ](@nodeLift f: A => BB)(zero: State)(
        acc: (State, Option[BB]) => (State, collection.Seq[ZZ])): Stage[A, ZZ] =
      scan$withNode { toNodeFactory(f) }(zero)(acc)
    def scan$withNode[A, BB, State, ZZ](f: A => Node[BB])(zero: State)(
        acc: (State, Option[BB]) => (State, collection.Seq[ZZ])): Stage[A, ZZ] =
      new StageImpl[A, BB, ZZ](max, min, ordered) {
        override def newStage[Result](next: StageManagerCreator[ZZ, Result]): StageManager[A, Result] =
          new StageBase[Result](next) {
            override val name = "scan"
            override protected def mapper(a: A): Node[B] = f(a)
            private var state: State = zero
            override protected def accumulate(bs: collection.Seq[B], isLast: Boolean): collection.Seq[Z] = {
              val emit: collection.Seq[ZZ] = bs.flatMap { b =>
                val (newState, zs) = acc(state, Some(b))
                state = newState
                zs
              }
              if (isLast) emit ++ acc(state, None)._2 else emit
            }
          }
      }

    @nodeSyncLift
    def map[A, B](@nodeLift f: A => B): Stage[A, B] = map$withNode { toNodeFactory(f) }
    def map$withNode[A, B](f: A => Node[B]): Stage[A, B] =
      new StageImpl[A, B, B](max, min, ordered) {
        override def newStage[Result](next: StageManagerCreator[BB, Result]): StageManager[A, Result] =
          new StageBase[Result](next) {
            override val name = "map"
            override protected def mapper(a: A): Node[B] = f(a)
            override protected def accumulate(b: collection.Seq[B], isLast: Boolean): collection.Seq[B] = b
          }
      }

    @nodeSyncLift
    def flatMap[A, B1](@nodeLift f: A => GenTraversableOnce[B1]): Stage[A, B1] =
      flatMap$withNode { toNodeFactory(f) }
    def flatMap$withNode[A, B1](f: A => Node[GenTraversableOnce[B1]]): Stage[A, B1] =
      new StageImpl[A, GenTraversableOnce[B1], B1](max, min, ordered) {
        override def newStage[Result](next: StageManagerCreator[B1, Result]): StageManager[A, Result] =
          new StageBase[Result](next) {
            override val name = "flatMap"
            override protected def mapper(a: A): Node[GenTraversableOnce[B1]] = f(a)
            override protected def accumulate(
                b: collection.Seq[GenTraversableOnce[B1]],
                isLast: Boolean): collection.Seq[B1] = b.flatten
          }
      }
    @nodeSyncLift
    def filter[A](@nodeLift f: A => Boolean): Stage[A, A] = filter$withNode { toNodeFactory(f) }
    def filter$withNode[A](f: A => Node[Boolean]): Stage[A, A] =
      new StageImpl[A, (A, Boolean), A] {
        override def newStage[Result](next: StageManagerCreator[A, Result]): StageManager[A, Result] =
          new StageBase[Result](next) {
            override val name = "filter"
            override protected def mapper(a: A): Node[(A, Boolean)] = f(a).map((a, _))
            override protected def accumulate(b: collection.Seq[(A, Boolean)], isLast: Boolean): collection.Seq[A] =
              b.collect { case (a, true) =>
                a
              }
          }
      }
  }

  object Stages extends Stages(-1, -1, false) {
    private[AsyncPipeline] def apply(max: Int, ordered: Boolean): Stages = new Stages(max, -1, ordered)
    def opts(max: Int = -1, ordered: Boolean = false): Stages = Stages(max, ordered)
  }
  def StagesOpts(max: Int = -1, ordered: Boolean = false): Stages = Stages(max, ordered)
  val Step = Stages // this has no purpose other than not having to change Calc.scala yer
  def stepOpts(max: Int = -1, ordered: Boolean = false): Stages = Stages(max, ordered)
  def stepOptsWithMinConcurrency(max: Int = -1, min: Int = -1, ordered: Boolean = false) = new Stages(max, min, ordered)

  private class SinkStageManager[A, CC[_]](cbf: scala.collection.compat.Factory[A, CC[A]])
      extends StageManager[A, CC[A]] {
    override protected type B = A
    override protected type Z = CC[A]
    override val name = "Sink"
    override protected def mapperNode(a: A): Node[B] = new AlreadyCompletedNode[A](a)
    private val bld = cbf.newBuilder
    override protected def accumulate(b: collection.Seq[A], isLast: Boolean): collection.Seq[CC[A]] = {
      bld ++= b
      if (isLast) collection.Seq(bld.result()) else collection.Seq.empty
    }
    override protected val next: StageManager[CC[A], CC[A]] =
      new NilStageManager[CC[A]](lazyEmptyResult = cbf.newBuilder.result())
    override protected val throttle: Option[Throttle] = None
    override protected val orderedResults: Option[mutable.Queue[PlaceHolder[B]]] = Some(
      new mutable.Queue[PlaceHolder[B]]())
  }

  // Combines with previous stage to create a StageCreator for draining into a collection.
  class TidyAndComplete[CC[_]] {
    def ::[AA, A](stage: Stage[AA, A])(implicit
        cbf: scala.collection.compat.Factory[A, CC[A]]): StageManagerCreator[AA, CC[A]] =
      stage.feedsTo(new StageManagerCreator[A, CC[A]] {
        override private[AsyncPipeline] def apply(): StageManager[A, CC[A]] = new SinkStageManager[A, CC](cbf)
      })
  }

  def toColl[CC[_]] = new TidyAndComplete[CC]

}
