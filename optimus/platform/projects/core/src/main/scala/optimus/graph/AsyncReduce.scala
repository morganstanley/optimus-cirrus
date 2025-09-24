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
import optimus.platform._
import optimus.platform.PluginHelpers.toNodeFactory

import scala.collection.BuildFrom
import scala.collection.mutable

trait AsyncReduce[A, CC <: Iterable[A]] {
  self: AsyncBase[A, CC] =>

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def associativeReduce(@nodeLift merge: (A, A) => A): Option[A] = associativeReduce$withNode(toNodeFactory(merge))
  def associativeReduce$withNode(merge: (A, A) => Node[A]): Option[A] = associativeReduce$newNode(merge).get
  def associativeReduce$queued(merge: (A, A) => Node[A]): NodeFuture[Option[A]] = associativeReduce$newNode(
    merge).enqueue
  def associativeReduce$newNode(merge: (A, A) => Node[A]): Node[Option[A]] = if (c.isEmpty)
    new AlreadyCompletedNode(None)
  else {
    val ci = c.iterator
    val queue = mutable.Queue.empty[Reduction]

    // Represents a reduction, pending or complete of elements i through n inclusive.  It's not really necessary
    // to track the indices, but it arguably makes this easier to think about.
    // Note that this constructor mutates the input iterator and reduction queue
    class Reduction(i: Int, j: Int, parent: Option[Reduction]) {
      // When complete, this becomes a Left
      private var state: Either[A, (Reduction, Reduction)] =
        if (i == j) Left(ci.next()) // dfs, so leaf nodes will be visited in order
        else {
          val w =
            (j - i - 1) / 2 // the -1 here matches scala .par, at least for n<32, as .par is non-deterministic above that
          Right((new Reduction(i, i + w, Some(this)), new Reduction(i + w + 1, j, Some(this))))
        }

      // Enqueue the reduction if both inputs are ready
      private def maybeEnqueue(): Unit = {
        state match {
          case Right((a, b)) if a.state.isLeft && b.state.isLeft =>
            queue.enqueue(this)
          case _ =>
        }
      }

      // Add initial pairs to queue
      maybeEnqueue()

      def pair = state.right.get match {
        case (r1, r2) => (r1.result, r2.result)
      }
      def result = state.left.get

      // When we complete, have the parent check if both pairs are ready and if so enqueue itself.
      def complete(a: A): Unit = {
        this.state = Left(a)
        parent.foreach(_.maybeEnqueue())
      }

    }

    // Commutative here means we don't care in what order consumeIteration is called, as our iterations
    // track the correct destination for the result.
    val cm = new CommutativeAggregatorNode[(Reduction, A), Option[A]](workMarker, maxConcurrency, -1) {

      // Calling this constructor builds the tree of dependencies of sub-reductions.
      // If all goes well, the root will contain the answer when we're done.
      val root = new Reduction(0, c.size - 1, None)

      override def hasNextIteration: Boolean = queue.nonEmpty

      override def nextIteration: Iteration = {
        val reduction = queue.dequeue()
        val (a1, a2) = reduction.pair
        // The node remembers its Reduction element, so we can complete it and possibly enqueue the parent.
        val node = merge(a1, a2).map((reduction, _))
        new Iteration(node)
      }

      override def consumeIteration(i: Iteration): Unit = {
        val (reduction, a) = i.node.result
        reduction.complete(a)
      }

      override def getFinalResult: Option[A] = Some(root.result)
      override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.AssociativeReduce
    }
    cm
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def commutativeMapReduce[K, V, That](@nodeLift mapper: A => Iterable[(K, V)])(@nodeLift reducer: (V, V) => V)(implicit
      cbf: BuildFrom[CC, (K, V), That]): Option[That] =
    commutativeMapReduce$withNode(toNodeFactory(mapper))(toNodeFactory(reducer))
  def commutativeMapReduce$withNode[K, V, That](mapper: A => Node[Iterable[(K, V)]])(reducer: (V, V) => Node[V])(
      implicit cbf: BuildFrom[CC, (K, V), That]): Option[That] =
    commutativeMapReduce$newNode(mapper)(reducer).get
  def commutativeMapReduce$queued[K, V, That](mapper: A => Node[Iterable[(K, V)]])(reducer: (V, V) => Node[V])(implicit
      cbf: BuildFrom[CC, (K, V), That]): NodeFuture[Option[That]] =
    commutativeMapReduce$newNode(mapper)(reducer).enqueue
  def commutativeMapReduce$newNode[K, V, That](mapper: A => Node[Iterable[(K, V)]])(reducer: (V, V) => Node[V])(implicit
      cbf: BuildFrom[CC, (K, V), That]): Node[Option[That]] = {
    if (c.isEmpty) new AlreadyCompletedNode(None)
    else {
      val cm =
        new CommutativeAggregatorNode[Iterable[(K, V)], Option[That]](workMarker, maxConcurrency, -1) {
          private[this] val reductionQueue = mutable.Queue.empty[(K, V, V)]
          private[this] val accumulator = mutable.HashMap.empty[K, V]
          private[this] val iterator: Iterator[A] = c.iterator

          override def hasNextIteration: Boolean = iterator.hasNext || reductionQueue.nonEmpty

          override def nextIteration: Iteration = {
            val node: Node[Iterable[(K, V)]] = if (reductionQueue.nonEmpty) {
              val (k, v1, v2) = reductionQueue.dequeue()
              reducer(v1, v2).map(v => Seq(k -> v))
            } else { mapper(iterator.next()) }
            new Iteration(node)
          }

          private def processKV(k: K, v: V): Unit = accumulator.updateWith(k) {
            case Some(v2) => reductionQueue.enqueue((k, v, v2)); None
            case None     => Some(v)
          }

          override def consumeIteration(i: Iteration): Unit = i.node.result.foreach { case (k, v) => processKV(k, v) }

          override def getFinalResult: Option[That] = Some(accumulator to cbf.toFactory(c))

          override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.CommutativeMapReduce
        }
      cm
    }
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def accumulate[Z, B](z: Z)(@nodeLift mapper: A => B)(@nodeLift accumulator: (Z, B) => Z): Z =
    accumulate$withNode(z)(toNodeFactory(mapper))(toNodeFactory(accumulator))
  def accumulate$queued[Z, B](z: Z)(mapper: A => Node[B])(accumulator: (Z, B) => Node[Z]): Node[Z] =
    accumulate$newNode(z)(mapper)(accumulator).enqueue
  def accumulate$withNode[Z, B](z: Z)(mapper: A => Node[B])(accumulator: (Z, B) => Node[Z]): Z =
    accumulate$newNode(z)(mapper)(accumulator).get

  private[this] def accumulate$newNode[Z, B](z: Z)(mapper: A => Node[B])(accumulator: (Z, B) => Node[Z]): Node[Z] = {

    // This node will complete with the accumulation so far, plus any queue of unaccumulated mapped values
    val can: Node[(List[B], Z)] =
      new CommutativeAggregatorNode[Unit, (List[B], Z)](workMarker, maxConcurrency, c.size) {
        val can = this
        val i = c.iterator
        var q: List[B] = Nil
        var acc: Z = z

        var accumulating = false

        // Kick of mapper on an element; on completion sweep up any mapped values into accumulant if nobody is doing this already
        class Collect(e: A) extends CompletableNode[Unit] {
          var b: B = _
          var mapNode: Node[B] = _
          override def run(ec: OGSchedulerContext): Unit = {
            mapNode = mapper(e)
            mapNode.attach(scenarioStack)
            ec.enqueue(mapNode)
            mapNode.continueWith(this, ec)
          }
          override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
            combineInfo(child, eq)
            if (child.isDoneWithException)
              completeWithException(child.exception, eq)
            else if (child eq mapNode) {
              b = mapNode.result
              val weShouldAccumulate = can.synchronized {
                if (can.accumulating)
                  false
                else {
                  can.accumulating = true
                  true
                }
              }
              if (weShouldAccumulate) {
                // If we got the lock, process the queue.  Note that we only lock the queue itself briefly.
                val lq = can.synchronized {
                  val lq = b :: q
                  q = Nil
                  lq
                }
                val accNode: Node[Z] = (new asyncSeq(lq)).foldLeft$newNode(acc)(accumulator)
                accNode.attach(scenarioStack())
                eq.enqueue(accNode)
                accNode.continueWith(this, eq)
              } else {
                // Someone is already accumulating - just add to the queue
                can.synchronized {
                  q = b :: q
                }
                completeWithResult((), eq)
              }
            } else {
              val accNode = child.asInstanceOf[Node[Z]]
              can.synchronized {
                can.accumulating = false
                acc = accNode.result
              }
              acc = accNode.result
              completeWithResult((), eq)
            }
          }
        }
        override def hasNextIteration: Boolean = i.hasNext
        override def nextIteration: Iteration = new Iteration(new Collect(i.next()))
        override def consumeIteration(i: Iteration): Unit = {}
        override def getFinalResult = (q, acc)
        override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.Accumulate
      }
    // When the aggregator completes, sweep up any remaining mapped values
    can.flatMap { case (q, r) =>
      new asyncSeq(q).foldLeft$newNode(r)(accumulator)
    }
  }

}
