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

import optimus.platform.PluginHelpers.toNodeFactory
import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.annotations.scenarioIndependentTransparent
import optimus.platform._
import optimus.scalacompat.collection.IterableLike

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.compat._

object AsyncIterator {

  implicit class ToAsyncIteratorOp[A, CI <: Iterable[A]](cc: CI with IterableLike[A, CI]) {
    def ipar = AI(cc, null, Int.MaxValue, new Noop[A])
    def ipar(maxConcurrency: Int) = AI(cc, null, maxConcurrency, new Noop[A])
    def ipar(marker: ProgressMarker, maxConcurrency: Int = Int.MaxValue) = AI(cc, marker, maxConcurrency, new Noop[A])
  }

  private[AsyncIterator] type NI[A] = Node[Iterable[A]]

  // Closure yielding an iterable
  private[AsyncIterator] sealed trait IterableProducer[A, Z]

  private[AsyncIterator] sealed trait Compose[AA, A, Z] extends IterableProducer[AA, Z]

  // A linked list of such closures - outermost is last in the pipeline
  final private[AsyncIterator] case class FlatMapCompose[AA, A, Z](f: A => NI[Z], prev: IterableProducer[AA, A])
      extends Compose[AA, A, Z]
  final private[AsyncIterator] case class MapCompose[AA, A, Z](f: A => Node[Z], prev: IterableProducer[AA, A])
      extends Compose[AA, A, Z]
  final private[AsyncIterator] case class FilterCompose[AA, A, Z](f: A => Node[Boolean], prev: IterableProducer[AA, A])
      extends Compose[AA, A, Z]
  // Boring placeholder for when we create an iterator that doesn't do anything
  final private[AsyncIterator] case class Noop[A]() extends IterableProducer[A, A]

  final case class AI[A, CI <: IterableOnce[A], B](
      c: CI,
      workMarker: ProgressMarker,
      maxConcurrency: Int,
      chain: IterableProducer[A, B]
  ) {

    @nodeSyncLift
    def map[C](@nodeLift f: B => C): AI[A, CI, C] = map$withNode(toNodeFactory(f))
    def map$withNode[C](f: B => Node[C]): AI[A, CI, C] = {
      val newChain: IterableProducer[A, C] = chain match {
        // Might as well fuse with the previous map
        case MapCompose(previousFunction, prevChain) =>
          val composed: Any => Node[C] = { a: Any => previousFunction.asInstanceOf[Any => Node[B]](a).flatMap(f) }
          MapCompose(composed, prevChain)
        case _ =>
          MapCompose(f, chain)
      }
      AI(c, workMarker, maxConcurrency, newChain)
    }

    @nodeSyncLift
    def flatMap[C](@nodeLift f: B => Iterable[C]): AI[A, CI, C] = flatMap$withNode(toNodeFactory(f))
    def flatMap$withNode[C](f: B => Node[Iterable[C]]): AI[A, CI, C] = {
      val newChain: IterableProducer[A, C] = FlatMapCompose(f, chain)
      AI(c, workMarker, maxConcurrency, newChain)
    }

    @nodeSyncLift
    def filter(@nodeLift f: B => Boolean): AI[A, CI, B] = filter$withNode(toNodeFactory(f))
    def filter$withNode(f: B => Node[Boolean]): AI[A, CI, B] = {
      val newChain: IterableProducer[A, B] = chain match {
        case FilterCompose(prevPred, prevChain) =>
          val composed: Any => Node[Boolean] = { a: Any =>
            prevPred(a).flatMap { b =>
              if (b) f.asInstanceOf[Any => Node[Boolean]](a) else new AlreadyCompletedNode(false)
            }
          }
          FilterCompose(composed, prevChain)
        case _ =>
          FilterCompose(f, chain)
      }
      AI(c, workMarker, maxConcurrency, newChain)
    }

    private case class Phase(f: Any => NI[Any], next: Phase) {
      def hasNext: Boolean = next ne null
    }

    private type FA = Any => NI[Any]

    private case class Slot(var a: Any, phase: Phase, var done: Boolean = false)
    //                     null                        false             =>  waiting for node completion
    //                     Iterable[Any]               false             =>  holds result of completion of any phase
    //                     Iterable[B]                 true              =>  results of last phase, to be reaped
    //                     null                        true              =>  reaped
    //                     List[Slot]                  false             =>  slots for the results of the next phase

    // Reverse and type-erase the list of transforms, so we can process them in order.
    @tailrec
    private def reverseTransforms(xf: IterableProducer[Any, Any], next: Phase): Phase = xf match {
      case MapCompose(f, prev)     => reverseTransforms(prev, Phase({ a => f(a).map(Some(_)) }, next))
      case FlatMapCompose(f, prev) => reverseTransforms(prev, Phase(f.asInstanceOf[FA], next))
      case FilterCompose(pred, prev) =>
        reverseTransforms(prev, Phase({ a => pred(a).map(if (_) Some(a) else None) }, next))
      case Noop() => next
    }

    @nodeSync
    @nodeSyncLift
    @scenarioIndependentTransparent
    def to[CC](cbf: Factory[B, CC]): CC = to$withNode(cbf)
    def to$queued[CC](cbf: Factory[B, CC]): Node[CC] = to$newNode(cbf).enqueue
    def to$withNode[CC](cbf: Factory[B, CC]): CC = to$newNode(cbf).get
    def to$newNode[CC](cbf: Factory[B, CC]): Node[CC] = {

      val builder: mutable.Builder[B, CC] = cbf.newBuilder
      val phase0 = reverseTransforms(chain.asInstanceOf[IterableProducer[Any, Any]], null)

      // inputs from the original collection
      val input = c.iterator

      val can: Node[CC] = new CommutativeAggregatorNode[Slot, CC](workMarker, maxConcurrency, -1) {
        // stack of slots with intermediate results,
        private var intermediates: List[Slot] = Nil
        // slots get added to this queue when created from inputs; they will be removed from the queue when the
        // entire subtree is complete
        private val phase0Slots = mutable.Queue.empty[Slot]

        override def hasNextIteration: Boolean = input.hasNext || intermediates.nonEmpty
        override def nextIteration: Iteration = {
          // Priority given to processing intermediates
          val node: Node[Slot] = if (intermediates.nonEmpty) {
            val slot = intermediates.head
            intermediates = intermediates.tail
            // node will complete with the re-used slot, but now holding results - on completion, we will either
            // convert each result to a new Slot for the next phase, or try to reap it
            slot.phase.f(slot.a).map { as: Iterable[Any] =>
              slot.a = as
              slot
            }
          } else if (input.hasNext) {
            // brand new slot for our tree
            val slot = Slot(null, phase0)
            phase0Slots += slot
            phase0.f(input.next()).map { as =>
              slot.a = as
              slot
            }
          } else throw new IllegalStateException("There is a blight in the land!")
          new Iteration(node)
        }

        override def consumeIteration(i: Iteration): Unit = {
          val slot = i.node.result
          if (slot.phase.hasNext) {
            // If there's another phase, create new slots for it
            val as = slot.a.asInstanceOf[Iterable[Any]]
            if (as.isEmpty) {
              // If closure returned an empty iterable, then we consider it already reaped
              slot.a = null
              slot.done = true
            } else {
              val ss: List[Slot] = as.map { a => Slot(a, slot.phase.next) }.toList
              // exiting slot becomes a branch
              slot.a = ss
              slot.done = true
              // and we add the new slots to the intermediates stack
              intermediates = ss ::: intermediates
            }
          } else {
            slot.done = true
            // Pull results from the slot tree until we find some that are still pending
            while (phase0Slots.nonEmpty && reap(phase0Slots.head))
              phase0Slots.dequeue()
          }
        }

        // returns true if we emptied this slot
        def reap(slot: Slot): Boolean = slot.done && ((null == slot.a) || {
          if (!slot.phase.hasNext) {
            // We're at a leaf.  If it's done, feed it to the builder
            builder ++= slot.a.asInstanceOf[Iterable[B]]
            slot.a = null
            true
          } else {
            // We're at a branch - traverse from bottom
            var slots = slot.a.asInstanceOf[List[Slot]]
            while (slots.nonEmpty && reap(slots.head))
              slots = slots.tail
            slots.isEmpty
          }
        })

        override def getFinalResult: CC = builder.result()
        override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.AsyncIterator
      }

      can
    }
  }
}
