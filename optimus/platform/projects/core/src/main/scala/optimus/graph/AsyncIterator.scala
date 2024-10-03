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

import scala.util.Success
import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.compat._
import scala.util.Failure

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

  private trait SlotValue {
    def reapable = false
  }
  // State when the first closure is processing an initial input:
  private final case object InitialState extends SlotValue
  // When the closure completes, it pokes its results into the slot.
  private trait ClosureResult extends SlotValue
  // Normally, that result is is one or more values, e.g. from a flatMap closure,
  // which is either an intermediate or final result, depending on whether there are further phases.
  private final case class IntermediateResults(as: Iterable[Any]) extends ClosureResult
  private final case class FinalResults[B](as: Iterable[B]) extends SlotValue { override def reapable = true }
  // We handle empty (e.g. an empty flatMap or a negative filter) and erroneous results specially.
  private final case object EmptyResults extends ClosureResult { override def reapable = true }
  private final case class ErrorResult(t: Throwable) extends ClosureResult { override def reapable = true }
  // In consumeIteration, each value in an InterableResult is converted into a slot holding a single IntermediateInput
  private final case class IntermediateInput(a: Any) extends SlotValue
  // and we track each of those slots in our result tree
  private final case class FanOut(slots: List[Slot]) extends SlotValue { override def reapable = true }
  // When a node in that tree is finally processed, we mark it as reaped.
  private final case object Reaped extends SlotValue

  private final case class Phase(f: Any => NI[Any], next: Phase) {
    def hasNext: Boolean = next ne null
  }
  private final case class Slot(var slotValue: SlotValue, phase: Phase) {
    def reapable = slotValue.reapable
  }

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

    private type FA = Any => NI[Any]
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
      val phase0: Phase = reverseTransforms(chain.asInstanceOf[IterableProducer[Any, Any]], null)

      // inputs from the original collection
      val input = c.iterator

      val can: Node[CC] = new CommutativeAggregatorNode[Slot, CC](workMarker, maxConcurrency, -1) {
        // stack of slots with intermediate results,
        private var intermediates: List[Slot] = Nil
        // slots get added to this queue when created from inputs; they will be removed from the queue when the
        // entire subtree is complete
        private val phase0Slots = mutable.Queue.empty[Slot]

        override def hasNextIteration: Boolean = input.hasNext || intermediates.nonEmpty

        private def wrappedNode(destinationSlot: Slot, input: Any, closure: Any => NI[Any]): Node[Slot] = {
          // Wrap closure in a NodeTry.  If it completes, the exception it contains is guaranteed RT
          new NodeTryNode(closure(input)).map { nt =>
            destinationSlot.slotValue = nt.toTry match {
              case Success(as) =>
                if (as.isEmpty)
                  EmptyResults
                else if (destinationSlot.phase.hasNext)
                  IntermediateResults(as)
                else
                  FinalResults(as.asInstanceOf[Iterable[B]])
              case Failure(e) =>
                ErrorResult(e)
            }
            destinationSlot
          }

        }

        override def nextIteration: Iteration = {
          // Priority given to processing intermediates
          val node: Node[Slot] = if (intermediates.nonEmpty) {
            // re-use the slot from the intermediate result
            val slot = intermediates.head
            val a = slot.slotValue match {
              case IntermediateInput(a) => a
              case _                    => throw new IllegalStateException("Expected intermediate result")
            }
            intermediates = intermediates.tail
            wrappedNode(slot, a, slot.phase.f)
          } else if (input.hasNext) {
            // brand new slot for our tree
            val slot = Slot(InitialState, phase0)
            phase0Slots += slot
            val a = input.next()
            wrappedNode(slot, a, phase0.f)
          } else throw new IllegalStateException("nextIteration called unexpectedly")
          new Iteration(node)
        }

        override def consumeIteration(i: Iteration): Unit = {
          val slot = i.node.result
          slot.slotValue match {
            case EmptyResults =>
              // Nothing more to be done here
              slot.slotValue = Reaped
            case IntermediateResults(as) =>
              val newSlots: List[Slot] = as.map { a =>
                Slot(IntermediateInput(a), slot.phase.next)
              }.toList
              slot.slotValue = FanOut(newSlots)
              intermediates = newSlots ::: intermediates
            case FinalResults(_) =>
              recursivelyReap()
            case ErrorResult(_) =>
              recursivelyReap()
            case Reaped =>
            // This can happen if the closure completed before a previous consumeIteration that triggered a recursive reap
            case _ =>
              throw new IllegalStateException
          }
        }

        // Collect results in the same order as if this were a normal (non-async) iterator, by traversing
        // the trees of partially completed calculations.
        def recursivelyReap(): Unit = {
          while (phase0Slots.nonEmpty && reap(phase0Slots.head))
            phase0Slots.dequeue()
        }

        // returns true if we emptied this slot
        def reap(slot: Slot): Boolean = (slot.slotValue == Reaped) || (slot.reapable && {
          slot.slotValue match {
            case ErrorResult(t) =>
              throw t
            case FinalResults(bs) =>
              builder ++= bs.asInstanceOf[Iterable[B]] // irritating
              slot.slotValue = Reaped
              true
            case FanOut(ss) =>
              var slots = ss
              while (slots.nonEmpty && reap(slots.head))
                slots = slots.tail
              slots.isEmpty
            case _ =>
              throw new IllegalStateException()
          }
        })

        override def getFinalResult: CC = builder.result()
        override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.AsyncIterator
      }

      can
    }
  }
}
