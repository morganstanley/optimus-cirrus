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
package optimus.platform

import scala.annotation.nowarn
import optimus.core.needsPlugin
import optimus.graph._
import optimus.platform.PluginHelpers.toNodeFactory
import optimus.platform.PluginHelpers.toNodeFQ
import optimus.platform.annotations._
import optimus.platform.internal.PartialFunctionMacro
import optimus.scalacompat.collection._
import optimus.scalacompat.collection.BuildFrom
import optimus.utils.CollectionUtils._

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.mutable.Builder
import org.slf4j.LoggerFactory

sealed trait AsyncImplicits1 {
  @parallelizable
  final implicit def collToAsyncMarker[A, CC <: Iterable[A]](
      c: CC with IterableLike[A, CC]): AsyncIterableMarker[A, CC] =
    new AsyncIterableMarker[A, CC](c)
  // For some reason, scala occasionally needs the more specific implicit.
  @parallelizable
  final implicit def rangeToAsyncMarker(r: Range): AsyncIterableMarker[Int, immutable.IndexedSeq[Int]] =
    new AsyncIterableMarker(r)
  @parallelizable
  final implicit def optionToAsyncMarker[A](o: Option[A]): AsyncOptionMarker[A] = new AsyncOptionMarker[A](o)
}
trait AsyncImplicits extends AsyncImplicits1 {
  // please don't ask me anything about this type signature -- it is what is needed to appease both IJ and scalac
  @parallelizable
  final implicit def mapToAsyncMarker[K, V, CC <: Map[K, V] with MapLike[K, V, CC]](
      m: CC with MapLike[K, V, CC]): AsyncMapMarker[K, V, CC] =
    new AsyncMapMarker[K, V, CC](m)
}
// For optimus projects that don't want to depend on optimus.platform.package
object AsyncImplicits extends AsyncImplicits

trait AsyncCollectionHelpers {

  /**
   * Relax code-reordering criteria within scope. WARNING: Everything in the scope must be referentially transparent, or
   * execution correctness is not guaranteed.
   */
  final def withAdvancedCodeMotion[T](x: T): T = x // lives here, so it's available in core

  /**
   * Plugin generates more elaborate output in this scope
   */
  final def pluginDebug[T](x: T): T = x

  object inlineRT {
    @parallelizable
    @inline def apply[T](x: T): T = x
  }

  object inlineRS {
    @sequential
    @inline def apply[T](x: T): T = x
  }
}

object AsyncCollectionHelpers extends AsyncCollectionHelpers

class AsyncIterableMarker[A, CC <: Iterable[A]](private val c: CC with IterableLike[A, CC]) extends AnyVal {
  def asyncOff: CC with IterableLike[A, CC] = c
  @nowarn("msg=10500 optimus.platform.AsyncInternals.seq")
  def aseq: asyncSeq[A, CC] = AsyncInternals.seq[A, CC](c)
  @nowarn("msg=10500 optimus.platform.AsyncInternals.seq")
  def aseq(marker: ProgressMarker): asyncSeq[A, CC] = AsyncInternals.seq[A, CC](c, marker)
  @nowarn("msg=10500 optimus.platform.AsyncInternals.par")
  def apar: asyncPar[A, CC] = AsyncInternals.par[A, CC](c)
  // reason is documentation only ATM
  @nowarn("msg=10500 optimus.platform.AsyncInternals.par")
  def apar(parallelGraphCallWithoutNodesReason: String): asyncPar[A, CC] = AsyncInternals.par[A, CC](c)
  @nowarn("msg=10500 optimus.platform.AsyncInternals.parImp")
  def apar(maxConcurrency: Int): asyncPar[A, CC] = AsyncInternals.parImp[A, CC](c, maxConcurrency)
  @nowarn("msg=10500 optimus.platform.AsyncInternals.par")
  def apar(marker: ProgressMarker, maxConcurrency: Int = Int.MaxValue): asyncPar[A, CC] =
    AsyncInternals.par[A, CC](c, marker, maxConcurrency)
  @nowarn("msg=10500 optimus.platform.AsyncInternals.parCustom")
  def apar(runtimeChecks: Boolean): asyncPar[A, CC] =
    AsyncInternals.parCustom[A, CC](c, AsyncInternals.DefaultConcurrency, runtimeChecks)
  @nowarn("msg=10500 optimus.platform.AsyncInternals.parCustom")
  def apar(maxConcurrency: Int, runtimeChecks: Boolean): asyncPar[A, CC] =
    AsyncInternals.parCustom[A, CC](c, maxConcurrency, runtimeChecks)
  @nowarn("msg=10500 optimus.platform.AsyncInternals.seq")
  def asyncSequential: asyncSeq[A, Iterable[A]] = AsyncInternals.seq(c)

  def pipeline = new AsyncPipeline(c)

}
class AsyncOptionMarker[A](private val o: Option[A]) extends AnyVal {
  def asyncOff: Option[A] = o
  def async = new OptAsync[A](o)
}
class AsyncMapMarker[K, V, CC <: Map[K, V] with MapLike[K, V, CC]](private val m: CC) extends AnyVal {
  def apar = new asyncParMap[K, V, CC](m)
  def apar(maxConcurrency: Int) = new asyncParMap[K, V, CC](m, maxConcurrency = maxConcurrency)
  def apar(marker: ProgressMarker) = new asyncParMap[K, V, CC](m, workMarker = marker)
  // reason is documentation only ATM
  def apar(parallelGraphCallWithoutNodesReason: String) = new asyncParMap[K, V, CC](m)
  def aseq = new asyncSeqMap[K, V, CC](m)
}

/**
 * This node will node be cached ever. Therefore it's possible to overlay some other execution on the top of it!
 */
class asyncAlwaysUnique extends annotation.StaticAnnotation

// We must not call this object `Async`, because doing so creates a file name clash on Windows where both this and the
// `async` annotation is compiled to `optimus/platform/async.class` (same path in a case-insensitive file system)
object AsyncInternals {

  // noinspection ScalaUnusedSymbol (Suppress for scope of import (if imported) See AutoAsyncComponent.scala)
  object suppressInlining

  final val DefaultConcurrency = Int.MaxValue

  @deprecating("Use inline implicit .apar or .aseq")
  def parCustom[A, CC <: Iterable[A]](
      c: CC with IterableLike[A, CC],
      maxConcurrency: Int = Int.MaxValue,
      runtimeChecks: Boolean = true) = new asyncPar[A, CC](c, null, maxConcurrency, runtimeChecks)
  @deprecating("Use inline implicit .apar or .aseq")
  def par[A, CC <: Iterable[A]](c: CC with IterableLike[A, CC]) = new asyncPar[A, CC](c)
  @deprecating("Use inline implicit .apar or .aseq")
  def parImp[A, CC <: Iterable[A]](c: CC with IterableLike[A, CC], maxConcurrency: Int) =
    new asyncParImp[A, CC](c, maxConcurrency)
  @deprecating("Use inline implicit .apar")
  def par[A, CC <: Iterable[A]](c: CC with IterableLike[A, CC], maxConcurrency: Int) =
    new asyncPar[A, CC](c, null, maxConcurrency)
  @deprecating("Use inline implicit .apar")
  def par[A, CC <: Iterable[A]](c: CC with IterableLike[A, CC], marker: ProgressMarker, maxConcurrency: Int) =
    new asyncPar[A, CC](c, marker, maxConcurrency)

  @deprecating("Use inline implicit .apar or .aseq")
  def seq[A, CC <: Iterable[A]](c: CC with IterableLike[A, CC], marker: ProgressMarker = null) =
    new asyncSeq[A, CC](c, marker)

}

final class OptAsync[A](val c: Option[A]) {

  def collect[B](pf: PartialFunction[A, B]): Option[B] =
    macro optimus.platform.internal.PartialFunctionMacro.collectTransformOpt[A, B]

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def collectPF[B](pf: OptimusPartialFunction[A, B]): Option[B] = collectPF$withNode(pf)
  // noinspection ScalaUnusedSymbol
  def collectPF$queued[B](pf: OptimusPartialFunction[A, B]): NodeFuture[Option[B]] = collectPF$newNode(pf).enqueue
  def collectPF$withNode[B](pf: OptimusPartialFunction[A, B]): Option[B] = collectPF$newNode(pf).get
  private[this] def collectPF$newNode[B](pf: OptimusPartialFunction[A, B]): Node[Option[B]] =
    if (c.isEmpty)
      new AlreadyCompletedNode(None)
    else
      pf.tryApply$newNode(c.get)
}

// Allow runtime suppression of check for possibly infinite stream
private[optimus /*platform*/ ] object AsyncBase {
  private[optimus /*platform*/ ] val log = LoggerFactory.getLogger(this.getClass)
  val streamWarnElements: Int = System.getProperty("optimus.autoasync.streamwarn", "10000").toInt

  // make some reusable nodes for common values
  private[this] def acn[A](a: A) = new AlreadyCompletedNode(a)
  private val trueNode = acn(true)
  private val falseNode = acn(false)
  private val unitNode = acn(())
  private val noneNode = acn(None)
  private val nilNode = acn(Nil)
  private val zeroNode = acn(0)
  private val emptyMapNode = acn(Map.empty)
}

@loom
private[optimus] abstract class AsyncBase[A, CC <: Iterable[A]](
    val c: CC with IterableLike[A, CC],
    val workMarker: ProgressMarker,
    val maxConcurrency: Int,
    runtimeCheck: Boolean)
    extends AsyncReduce[A, CC] {

  import AsyncBase._

  if (streamWarnElements >= 0) {
    def streamWarn(): Unit = {
      val msg =
        s"Found potentially infinite stream with more than $streamWarnElements elements of ${c.head.getClass}"
      log.warn(msg)
      log.debug("Stack", new Exception(msg))
    }
    // We have to leave this apparently undefined to avoid an unused import error. This method
    // is recognized stringly in the staging plugin.
    if (scalaVersionRange("2.13:"): @staged) {
      import scala.collection.LinearSeq
      c.asInstanceOf[Iterable[_]] match {
        case s @ (_: LazyList[_] | _: Stream[_]) =>
          if (s.asInstanceOf[LinearSeq[_]].isDefinedAt(streamWarnElements))
            streamWarn()
        case _ =>
      }
    } else { // LazyList doesn't exist in 2.12
      c match {
        case s: Stream[_] =>
          if (s.isDefinedAt(streamWarnElements))
            streamWarn()
        case _ =>
      }
    }
  }

  protected[this] final def isEmpty: Boolean = c.isEmpty
  protected[this] final def emptyThat[That](cbf: BuildFrom[CC, _, That]): AlreadyCompletedNode[That] = {
    val empty = optimus.scalacompat.collection.empty(cbf, c)
    if (empty.asInstanceOf[AnyRef] eq Nil) nilNode.asInstanceOf[AlreadyCompletedNode[That]]
    else new AlreadyCompletedNode(cbf.newBuilder(c).result())
  }

  // See what goes wrong if we allow Streams through
  // val doSync = runtimeCheck && (c.isInstanceOf[IterableView[_,_]] || c.isInstanceOf[Stream[_]])
  val doSync: Boolean = runtimeCheck && optimus.scalacompat.collection.isView(c)

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def map[B, That](@nodeLift f: A => B)(implicit cbf: BuildFrom[CC, B, That]): That =
    map$withNode[B, That](toNodeFactory(f))
  def map$withNode[B, That](f: A => Node[B])(implicit cbf: BuildFrom[CC, B, That]): That = map$newNode(f).get
  def map$queued[B, That](f: A => Node[B])(implicit cbf: BuildFrom[CC, B, That]): NodeFuture[That] =
    map$newNode(toNodeFQ(f)).enqueue
  final private def map$newNode[B, That](f: A => Node[B])(implicit cbf: BuildFrom[CC, B, That]): Node[That] =
    if (isEmpty) emptyThat(cbf)
    else if (doSync) {
      val builder = cbf.newBuilder(c)
      c.iterator.foreach(x => builder += f(x).get)
      new AlreadyCompletedNode(builder.result())
    } else
      new ConverterSequenceNode[A, B, That](c, f, NodeTaskInfo.Map, workMarker, maxConcurrency) {
        private val builder = cbf.newBuilder(c)
        override def getFinalResult: That = builder.result()
        override def consume(b: B): Unit =
          builder += b
      }
  @nodeSync
  @scenarioIndependentTransparent
  private[optimus] def mapPrelifted[B, That](f: A => Node[B])(implicit cbf: BuildFrom[CC, B, That]): That =
    map$queued(f).get$
  // noinspection ScalaUnusedSymbol
  private[optimus] def mapPrelifted$queued[B, That](f: A => Node[B])(implicit
      cbf: BuildFrom[CC, B, That]): NodeFuture[That] =
    map$queued(f)

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def partitionMap[P, B, That](p: (() => Option[A]) => P)(@nodeLift f: P => B)(implicit
      cbf: BuildFrom[CC, B, That]): That = partitionMap$withNode(p)(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def partitionMap$queued[P, B, That](p: (() => Option[A]) => P)(@nodeLift f: P => Node[B])(implicit
      cbf: BuildFrom[CC, B, That]): Node[That] = partitionMap$newNode(p)(toNodeFQ(f)).enqueue
  def partitionMap$withNode[P, B, That](p: (() => Option[A]) => P)(@nodeLift f: P => Node[B])(implicit
      cbf: BuildFrom[CC, B, That]): That = partitionMap$newNode(p)(f).get
  def partitionMap$newNode[P, B, That](p: (() => Option[A]) => P)(@nodeLift f: P => Node[B])(implicit
      cbf: BuildFrom[CC, B, That]): Node[That] =
    new PartitioningSequenceNode[A, P, B, That](c, p, f, NodeTaskInfo.Map, workMarker, maxConcurrency) {
      private val builder = cbf.newBuilder(c)
      override def getFinalResult: That = builder.result()
      override def consume(b: B): Unit = builder += b
    }

  /**
   * Asynchronously map f over the collection. If an exceptions are thrown, invoke the handler, to log them or perform
   * some other operation for side effects and, optionally, to assemble a string message, but continue evaluating f on
   * the remaining elements. Finally, if any exceptions were thrown, throw a RuntimeException with each of the
   * exceptions attached as suppressed. Note that the handler receives, in addition to the input element, a full
   * NodeResult containing warnings if present.
   */
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def mapWithRethrow[B, That](@nodeLift f: A => B)(
      @nodeLift processExceptions: Iterable[(A, NodeResult[B])] => Option[String])(implicit
      cbf: BuildFrom[CC, B, That]): That = mapWithRethrow$withNode(toNodeFactory(f))(toNodeFactory(processExceptions))
  // noinspection ScalaUnusedSymbol
  def mapWithRethrow$queued[B, That](f: A => Node[B])(
      processExceptions: Iterable[(A, NodeResult[B])] => Node[Option[String]])(implicit
      cbf: BuildFrom[CC, B, That]): Node[That] = mapWithRethrow$newNode(toNodeFQ(f))(processExceptions).enqueue
  def mapWithRethrow$withNode[B, That](f: A => Node[B])(
      processExceptions: Iterable[(A, NodeResult[B])] => Node[Option[String]])(implicit
      cbf: BuildFrom[CC, B, That]): That = mapWithRethrow$newNode(f)(processExceptions).get
  final private[this] def mapWithRethrow$newNode[B, That](f: A => Node[B])(
      processExceptions: Iterable[(A, NodeResult[B])] => Node[Option[String]])(implicit
      cbf: BuildFrom[CC, B, That]): Node[That] = {
    val outer = this.asInstanceOf[AsyncBase[A, Iterable[A]]].map$newNode(a => new NodeResultNode(f(a)))
    val result: Node[That] = new CompletableNodeM[That] {
      override def run(ec: OGSchedulerContext): Unit = {
        outer.attach(scenarioStack())
        ec.enqueue(outer) // Schedule the result node as a continuation of the source node.
        outer.continueWith(this, ec)
      }
      private var exs: Iterable[Throwable] = _
      private var msgNode: Node[Option[String]] = _
      override def onChildCompleted(eq: EvaluationQueue, task: NodeTask): Unit = {
        combineInfo(task, eq)
        if (task.isDoneWithException) completeWithException(task.exception, eq)
        else if (!task.isDoneWithResult) throwNodeCompletionException(task.asInstanceOf[Node[_]])
        else if (task eq outer) {
          // Original map operation just completed...
          val nrs = outer.result
          nrs.flatMap(_.warnings).foreach(attach)
          if (nrs.exists(_.hasException)) {
            // ... with exceptions.  Launch error processor in a child scope (so that exceptions won't propagate out)
            val errata: Iterable[(A, NodeResult[B])] = c.zip(nrs).filter(_._2.hasException)
            exs = errata.map(_._2.exception) // store exceptions so we can attach them later
            msgNode = processExceptions(errata)
            msgNode.attach(scenarioStack.withChildCancellationScope())
            eq.enqueue(msgNode)
            msgNode.continueWith(this, eq)
          } else {
            // .. successfully.  Complete with sequence result.
            val builder = cbf.newBuilder(c)
            nrs.foreach(builder += _.value)
            completeWithResult(builder.result(), eq)
          }
        } else {
          // Error processor just completed.
          assert(task eq msgNode)
          val msg = msgNode.result.getOrElse(s"Exceptions during mapWithRethrow: ${exs.size}/${c.size}")
          val ise = new RuntimeException(msg)
          exs.foreach(ise.addSuppressed(_))
          completeWithException(ise, eq)
        }
      }
    }
    result
  }

  @nodeSync
  @scenarioIndependentTransparent
  private[optimus] def flatMapPrelifted[B, That](f: A => Node[IterableOnce[B]])(implicit
      cbf: BuildFrom[CC, B, That]): That = needsPlugin
  // noinspection ScalaUnusedSymbol
  private[optimus] def flatMapPrelifted$queued[B, That](f: A => Node[IterableOnce[B]])(implicit
      cbf: BuildFrom[CC, B, That]): Node[That] = flatMap$queued(f)

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def flatMap[B, That](@nodeLift f: A => IterableOnce[B])(implicit cbf: BuildFrom[CC, B, That]): That =
    flatMap$newNode(toNodeFactory(f)).get
  def flatMap$queued[B, That](f: A => Node[IterableOnce[B]])(implicit cbf: BuildFrom[CC, B, That]): Node[That] =
    flatMap$newNode(toNodeFQ(f)).enqueue
  def flatMap$queued[B, That](f: A => IterableOnce[B], cbf: BuildFrom[CC, B, That]): NodeFuture[That] =
    flatMap$newNode(toNodeFactory(f))(cbf).enqueue
  def flatMap$withNode[B, That](f: A => Node[IterableOnce[B]])(implicit cbf: BuildFrom[CC, B, That]): That =
    flatMap$newNode(f).get
  final private[optimus] def flatMap$newNode[B, That](f: A => Node[IterableOnce[B]])(implicit
      cbf: BuildFrom[CC, B, That]) =
    if (isEmpty) emptyThat(cbf)
    else if (doSync) {
      val builder = cbf.newBuilder(c)
      c.foreach(x => f(x).get.foreach(y => builder += y))
      new AlreadyCompletedNode(builder.result())
    } else
      new ConverterSequenceNode[A, IterableOnce[B], That](c, f, NodeTaskInfo.FlatMap, workMarker, maxConcurrency) {
        private val builder = cbf.newBuilder(c)
        override def getFinalResult: That = builder.result()
        override def consume(b: IterableOnce[B]): Unit = builder ++= b.seq
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def foreach[U](@nodeLift f: A => U): Unit = foreach$withNode { toNodeFactory(f) }
  // noinspection ScalaUnusedSymbol
  def foreach$queued[U](f: A => Node[U]): Node[Unit] = foreach$newNode[U](toNodeFQ(f)).enqueue
  def foreach$withNode[U](f: A => Node[U]): Unit = foreach$newNode[U](f).get
  final private[this] def foreach$newNode[U](f: A => Node[U]): Node[Unit] =
    if (isEmpty) unitNode
    else if (doSync) new AlreadyCompletedNode[Unit](c.foreach(f(_)))
    else
      new CommutativeAggregatorNode[U, Unit](workMarker, maxConcurrency, c.knownSize) {
        private[this] val i = c.iterator
        override def hasNextIteration: Boolean = i.hasNext
        override def nextIteration: Iteration = new Iteration(f(i.next()))
        override def consumeIteration(i: Iteration): Unit = ()
        override def getFinalResult: Unit = ()
        override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.ForEach
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  @deprecated(
    "foldLeft is inherently sequential. Consider a different combinator, or use .aseq.foldLeft if you " +
      "absolutely must use foldLeft",
    since = "2020-11-02")
  def foldLeft[B](z: B)(@nodeLift op: (B, A) => B): B = foldLeft$withNode(z)(toNodeFactory(op))
  def foldLeft$queued[B](z: B)(op: (B, A) => Node[B]): NodeFuture[B] = foldLeft$newNode(z)(toNodeFQ(op)).enqueue
  def foldLeft$withNode[B](z: B)(op: (B, A) => Node[B]): B = foldLeft$newNode(z)(op).get
  protected def foldLeft$newNode[B](z: B)(op: (B, A) => Node[B]): Node[B] =
    if (isEmpty) new AlreadyCompletedNode(z)
    else if (doSync) new AlreadyCompletedNode(c.foldLeft(z)(op(_, _).get))
    else {
      class FoldLeft(var r: B, val iterator: Iterator[A]) extends SequenceNode[B, B](workMarker, 1, -1) {
        override def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.FoldLeft
        override def getFinalResult: B = r
        override def consumeIteration(i: Iteration): Unit = r = i.node.result
        override def nextIteration = new Iteration(op(r, iterator.next()))
        override def hasNextIteration: Boolean = iterator.hasNext
      }
      new FoldLeft(z, c.iterator)
    }

  /*.map {
    case (q: List[B], r: Z) =>
      new asyncSeq(q).foldLeft$withNode(r)(accumulator) */
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def filter(@nodeLift f: A => Boolean): CC = filter$withNode(toNodeFactory(f))
  def filter$queued(f: A => Node[Boolean]): NodeFuture[CC] = filter$newNode(toNodeFQ(f)).enqueue
  def filter$withNode(f: A => Node[Boolean]): CC = filter$newNode(f).get
  final private[this] def filter$newNode(f: A => Node[Boolean]): Node[CC] =
    if (isEmpty) new AlreadyCompletedNode(c)
    else if (doSync) new AlreadyCompletedNode(c.filter(f(_).get))
    else
      new ConverterSequenceNode2[A, Boolean, CC](c, f, NodeTaskInfo.Filter, workMarker, maxConcurrency) {
        private[this] val builder: mutable.Builder[A, CC] = newBuilder
        override def getFinalResult: CC = builder.result()
        override def consume(b: Boolean, org: A): Unit = if (b) builder += org
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def filterNot(@nodeLift f: A => Boolean): CC = filterNot$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def filterNot$queued(f: A => Node[Boolean]): NodeFuture[CC] = filterNot$newNode(toNodeFQ(f)).enqueue
  def filterNot$withNode(f: A => Node[Boolean]): CC = filterNot$newNode(f).get
  final private[this] def filterNot$newNode(f: A => Node[Boolean]): Node[CC] =
    if (isEmpty) new AlreadyCompletedNode(c)
    else if (doSync) new AlreadyCompletedNode(c.filterNot(f(_).get))
    else
      new ConverterSequenceNode2[A, Boolean, CC](c, f, NodeTaskInfo.Filter, workMarker, maxConcurrency) {
        private[this] val builder = newBuilder
        override def getFinalResult: CC = builder.result()
        override def consume(b: Boolean, org: A): Unit = if (!b) builder += org
      }

  @nodeSync
  @scenarioIndependentTransparent
  private[optimus] def filterPrelifted(f: A => Node[Boolean]): CC = filterPrelifted$queued(f).get$
  private[optimus] def filterPrelifted$queued(f: A => Node[Boolean]): NodeFuture[CC] = filter$queued(f)

  /**
   * flatMap optimized for closures returning Option. This avoids forced conversion of Options to sequences, providing
   * an efficient and somewhat less messy alternative to collect and partial functions. If asyncgraph has trouble with a
   * collect due to non-idempotency of typechecking partial functions, this is the optimal substitute.
   */
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def flatMop[B, That](@nodeLift f: A => Option[B])(implicit cbf: BuildFrom[CC, B, That]): That = flatMop$withNode(
    toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def flatMop$queued[B, That](f: A => Node[Option[B]])(implicit cbf: BuildFrom[CC, B, That]): Node[That] =
    flatMop$newNode(toNodeFQ(f)).enqueue
  // noinspection ScalaUnusedSymbol
  def flatMop$withNode[B, That](f: A => Node[Option[B]])(implicit cbf: BuildFrom[CC, B, That]): That =
    flatMop$newNode(f).get
  final private[this] def flatMop$newNode[B, That](f: A => Node[Option[B]])(implicit cbf: BuildFrom[CC, B, That]) =
    if (isEmpty) emptyThat(cbf)
    else if (doSync) {
      val builder = cbf.newBuilder(c)
      c.foreach(x => f(x).get.foreach(builder += _))
      new AlreadyCompletedNode(builder.result())
    } else
      new ConverterSequenceNode[A, Option[B], That](c, f, NodeTaskInfo.FlatMap, workMarker, maxConcurrency) {
        private val builder = cbf.newBuilder(c)
        override def getFinalResult: That = builder.result()
        override def consume(b: Option[B]): Unit = if (b.isDefined) builder += b.get
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def groupBy[K](@nodeLift f: A => K): Map[K, CC] = groupBy$withNode(toNodeFactory(f))
  def groupBy$queued[K](f: A => Node[K]): Node[Map[K, CC]] = groupBy$newNode(toNodeFQ(f)).enqueue
  def groupBy$queued[K](f: A => K): NodeFuture[Map[K, CC]] = groupBy$newNode(toNodeFactory(f(_))).enqueue
  def groupBy$withNode[K](f: A => Node[K]): immutable.Map[K, CC] = groupBy$newNode(f).get
  final private[this] def groupBy$newNode[K](f: A => Node[K]): Node[Map[K, CC]] =
    if (isEmpty) emptyMapNode.asInstanceOf[AlreadyCompletedNode[Map[K, CC]]]
    else if (doSync) new AlreadyCompletedNode(c.groupBy(f(_).get))
    else
      new ConverterSequenceNode2[A, K, Map[K, CC]](c, f, NodeTaskInfo.GroupBy, workMarker, maxConcurrency) {
        private[this] val builder = mutable.Map.empty[K, Builder[A, CC]]
        override def getFinalResult: Map[K, CC] = {
          // convert those mutable builders back to immutable CCs
          val b = immutable.Map.newBuilder[K, CC]
          for ((k, v) <- builder) b += k -> v.result()
          b.result()
        }
        override def consume(k: K, org: A): Unit =
          builder.getOrElseUpdate(
            k,
            newBuilder) += org // TODO (OPTIMUS-0000): Lots of optimization improvements can be done here
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def groupByStable[K](@nodeLift f: A => K): Seq[(K, Seq[A])] = groupByStable$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def groupByStable$queued[K](f: A => Node[K]): Node[Seq[(K, Seq[A])]] = groupByStable$newNode(toNodeFQ(f)).enqueue
  def groupByStable$withNode[K](f: A => Node[K]): Seq[(K, Seq[A])] = groupByStable$newNode(f).get
  final private[this] def groupByStable$newNode[K](f: A => Node[K]): Node[Seq[(K, Seq[A])]] =
    if (isEmpty) emptyMapNode.asInstanceOf[AlreadyCompletedNode[Seq[(K, Seq[A])]]]
    else if (doSync) new AlreadyCompletedNode(c.groupByStable(f(_).get))
    else
      new ConverterSequenceNode2[A, K, Seq[(K, Seq[A])]](c, f, NodeTaskInfo.GroupBy, workMarker, maxConcurrency) {
        private[this] val builder = mutable.LinkedHashMap.empty[K, Builder[A, Seq[A]]]
        override def getFinalResult: Seq[(K, Seq[A])] = {
          val b = immutable.Vector.newBuilder[(K, Seq[A])]
          b.sizeHint(builder.size)
          for ((k, v) <- builder) b += k -> v.result()
          b.result()
        }
        override def consume(k: K, org: A): Unit =
          builder.getOrElseUpdate(k, Vector.newBuilder) += org
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def partition(@nodeLift f: A => Boolean): (CC, CC) = partition$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def partition$queued(f: A => Node[Boolean]): NodeFuture[(CC, CC)] = partition$newNode(toNodeFQ(f)).enqueue
  def partition$withNode(f: A => Node[Boolean]): (CC, CC) = partition$newNode(f).get
  final private[this] def partition$newNode(f: A => Node[Boolean]) =
    if (isEmpty) new AlreadyCompletedNode(c, c)
    else
      new ConverterSequenceNode2[A, Boolean, (CC, CC)](c, f, NodeTaskInfo.Partition, workMarker, maxConcurrency) {
        val (lBuilder, rBuilder) = (newBuilder, newBuilder)
        override def getFinalResult: (CC, CC) = (lBuilder.result(), rBuilder.result())
        override def consume(b: Boolean, org: A): Unit =
          if (b) { lBuilder += org }
          else { rBuilder += org }
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def forall(@nodeLift f: A => Boolean): Boolean = forall$newNode(toNodeFactory(f)).get
  // noinspection ScalaUnusedSymbol
  def forall$queued(f: A => Node[Boolean]): NodeFuture[Boolean] = forall$newNode(toNodeFQ(f)).enqueue
  def forall$withNode(f: A => Node[Boolean]): Boolean = forall$newNode(f).get
  final private[this] def forall$newNode(f: A => Node[Boolean]) =
    if (isEmpty) trueNode
    else if (doSync) new AlreadyCompletedNode(c.forall(f(_).get))
    else
      new ConverterSequenceNode[A, Boolean, Boolean](c, f, NodeTaskInfo.ForAll, workMarker, maxConcurrency) {
        private[this] var r = true
        override def consume(k: Boolean): Unit = if (!k) r = false // Once 1 was set to false the result is false
        override def stopConsuming: Boolean = !r
        override def getFinalResult: Boolean = r // Short-circuit iterations
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def find(@nodeLift f: A => Boolean): Option[A] = find$withNode(toNodeFactory(f))
  def find$queued(f: A => Node[Boolean]): NodeFuture[Option[A]] = find$newNode(toNodeFQ(f)).enqueue
  def find$withNode(f: A => Node[Boolean]): Option[A] = find$newNode(f).get
  final private[this] def find$newNode(f: A => Node[Boolean]): Node[Option[A]] =
    if (isEmpty) noneNode
    else if (doSync) new AlreadyCompletedNode(c.find(f(_).get))
    else // Predicates may be parallelized, but we will return entry corresponding to the first True.
      new ConverterSequenceNode2[A, Boolean, Option[A]](c, f, NodeTaskInfo.Find, workMarker, maxConcurrency) {
        private[this] var r: Option[A] = None
        override def consume(b: Boolean, a: A): Unit = if (b && r.isEmpty) r = Some(a)
        override def stopConsuming: Boolean = r.nonEmpty // Short-circuit iterations
        override def getFinalResult: Option[A] = r
      }
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def exists(@nodeLift f: A => Boolean): Boolean = exists$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def exists$queued(f: A => Node[Boolean]): NodeFuture[Boolean] = exists$newNode(toNodeFQ(f)).enqueue
  def exists$withNode(f: A => Node[Boolean]): Boolean = exists$newNode(f).get
  final private[this] def exists$newNode(f: A => Node[Boolean]) =
    if (isEmpty) falseNode
    else if (doSync) new AlreadyCompletedNode(c.exists(f(_).get))
    else
      new ConverterSequenceNode[A, Boolean, Boolean](c, f, NodeTaskInfo.Exists, workMarker, maxConcurrency) {
        private[this] var r = false
        override def consume(k: Boolean): Unit = if (k) r = true // Once 1 was set to false the result is false
        override def stopConsuming: Boolean = r // Short-circuit iterations
        override def getFinalResult: Boolean = r
      }

  protected def newBuilder: Builder[A, CC] = {
    // cast required while we have 2.12/2.13 cross building
    optimus.scalacompat.collection.newBuilderFor(c).asInstanceOf[Builder[A, CC]]
  }

  def collect[B, That](pf: PartialFunction[A, B])(implicit cbf: BuildFrom[CC, B, That]): That =
    macro PartialFunctionMacro.collectTransform[CC, A, B, That]
  def collectFirst[B](pf: PartialFunction[A, B]): Option[B] =
    macro PartialFunctionMacro.collectFirstTransform[CC, A, B]

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def collectPF[B, That](pf: OptimusPartialFunction[A, B])(implicit cbf: BuildFrom[CC, B, That]): That =
    collectPF$withNode(pf)
  // noinspection ScalaUnusedSymbol
  def collectPF$queued[B, That](pf: OptimusPartialFunction[A, B])(implicit
      cbf: BuildFrom[CC, B, That]): NodeFuture[That] =
    collectPF$newNode(pf).enqueue
  def collectPF$withNode[B, That](pf: OptimusPartialFunction[A, B])(implicit cbf: BuildFrom[CC, B, That]): That =
    collectPF$newNode(pf).get
  final private[this] def collectPF$newNode[B, That](pf: OptimusPartialFunction[A, B])(implicit
      cbf: BuildFrom[CC, B, That]) =
    new ConverterSequenceNode[A, Option[B], That](
      c,
      a => pf.tryApply$newNode(a),
      NodeTaskInfo.Collect,
      null,
      maxConcurrency) {
      private val builder = cbf.newBuilder(c)
      override def getFinalResult: That = builder.result()
      override def consume(b: Option[B]): Unit = b.foreach { builder += _ }
    }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def collectFirstPF[B](pf: OptimusPartialFunction[A, B]): Option[B] = collectFirstPF$withNode(pf)
  // noinspection ScalaUnusedSymbol
  def collectFirstPF$queued[B](pf: OptimusPartialFunction[A, B]): NodeFuture[Option[B]] =
    collectFirstPF$newNode(pf).enqueue
  def collectFirstPF$withNode[B](pf: OptimusPartialFunction[A, B]): Option[B] =
    collectFirstPF$newNode(pf).get
  final private[this] def collectFirstPF$newNode[B](pf: OptimusPartialFunction[A, B]) =
    if (isEmpty) noneNode
    else
      new ConverterSequenceNode[A, Option[B], Option[B]](
        c,
        pf.tryApply$newNode,
        NodeTaskInfo.CollectFirst,
        workMarker,
        maxConcurrency) {
        var res = Option.empty[B]
        override def getFinalResult: Option[B] = res
        override def consume(b: Option[B]): Unit = if (res.isEmpty) res = b
        override def stopConsuming: Boolean = res.nonEmpty
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def sortBy[B](@nodeLift f: A => B)(implicit ev: CC <:< SeqLike[A, CC], ord: Ordering[B]): CC = sortBy$withNode(
    toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def sortBy$queued[B](f: A => Node[B])(implicit ev: CC <:< SeqLike[A, CC], ord: Ordering[B]): Node[CC] =
    sortBy$newNode(toNodeFQ(f)).enqueue
  // noinspection ScalaUnusedSymbol
  def sortBy$withNode[B](f: A => Node[B])(implicit ev: CC <:< SeqLike[A, CC], ord: Ordering[B]): CC =
    sortBy$newNode(f).get
  final private[this] def sortBy$newNode[B](
      f: A => Node[B]
  )(implicit ev: CC <:< SeqLike[A, CC], ord: Ordering[B]): Node[CC] =
    if (isEmpty) new AlreadyCompletedNode(c)
    else if (doSync) new AlreadyCompletedNode(c.sortBy(f(_).get)(ord))
    else
      new ConverterSequenceNode2[A, B, CC](c, f, NodeTaskInfo.SortBy, workMarker, maxConcurrency) {
        private val sortKeys = mutable.Map[A, B]()
        override def getFinalResult: CC = c.sortBy(sortKeys)(ord)
        override def consume(elem: B, source: A): Unit =
          sortKeys += source -> elem
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def minBy[B: Ordering](@nodeLift f: A => B): A = minBy$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def minBy$queued[B: Ordering](f: A => Node[B]): Node[A] = minBy$newNode(toNodeFQ(f)).enqueue
  def minBy$withNode[B: Ordering](f: A => Node[B]): A = minBy$newNode(f).get
  final private[this] def minBy$newNode[B](f: A => Node[B])(implicit ord: Ordering[B]): Node[A] =
    if (c.isEmpty) new AlreadyFailedNode(new NoSuchElementException("empty.minBy"))
    else if (doSync) new AlreadyCompletedNode(c.minBy(f(_).get)(ord))
    else
      new ConverterSequenceNode2[A, B, A](c, f, NodeTaskInfo.MinBy, workMarker, maxConcurrency) {
        private[this] var first: Boolean = true
        private[this] var minA: A = _
        private[this] var minB: B = _

        override def consume(elem: B, source: A): Unit = {
          if (first) {
            minA = source; minB = elem; first = false
          } else if (ord.lt(elem, minB)) {
            minA = source; minB = elem
          }
        }
        override def getFinalResult: A = minA // must be initialized because we only get here if c.nonEmpty
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def maxBy[B: Ordering](@nodeLift f: A => B): A = maxBy$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def maxBy$queued[B: Ordering](f: A => Node[B]): Node[A] = maxBy$newNode(toNodeFQ(f)).enqueue
  def maxBy$withNode[B: Ordering](f: A => Node[B]): A = maxBy$newNode(f).get
  final private[this] def maxBy$newNode[B](f: A => Node[B])(implicit ord: Ordering[B]): Node[A] =
    if (c.isEmpty) new AlreadyFailedNode(new NoSuchElementException("empty.maxBy"))
    else if (doSync) new AlreadyCompletedNode(c.maxBy(f(_).get)(ord))
    else
      new ConverterSequenceNode2[A, B, A](c, f, NodeTaskInfo.MaxBy, workMarker, maxConcurrency) {
        private[this] var first: Boolean = true
        private[this] var maxA: A = _
        private[this] var maxB: B = _

        override def consume(elem: B, source: A): Unit = {
          if (first) {
            maxA = source; maxB = elem; first = false
          } else if (ord.gt(elem, maxB)) {
            maxA = source; maxB = elem
          }
        }
        override def getFinalResult: A = maxA // must be initialized because we only get here if c.nonEmpty
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def count(@nodeLift p: A => Boolean): Int = count$withNode(toNodeFactory(p))
  // noinspection ScalaUnusedSymbol
  def count$queued(p: A => Node[Boolean]): NodeFuture[Int] = count$newNode(toNodeFQ(p)).enqueue
  def count$withNode(p: A => Node[Boolean]): Int = count$newNode(p).get
  final private[this] def count$newNode(f: A => Node[Boolean]): Node[Int] =
    if (isEmpty) zeroNode
    else if (doSync) new AlreadyCompletedNode(c.count(f(_).get))
    else
      new ConverterSequenceNode[A, Boolean, Int](c, f, NodeTaskInfo.Count, workMarker, maxConcurrency) {
        private[this] var count = 0
        override def consume(elem: Boolean): Unit = { if (elem) count += 1 }
        override def getFinalResult: Int = count
      }
  class AutoAsync[B, That](bf: BuildFrom[CC, B, That]) {
    @nodeSync
    @nodeSyncLift
    @scenarioIndependentTransparent
    final def map(@nodeLift f: A => B): That =
      AsyncBase.this.map$newNode(toNodeFactory(f))(bf).get
    // noinspection ScalaUnusedSymbol
    final def map$queued(f: A => Node[B]): NodeFuture[That] = AsyncBase.this.map$newNode(toNodeFQ(f))(bf).enqueue
    final def map$withNode(f: A => Node[B]): That = AsyncBase.this.map$newNode(f)(bf).get

    @nodeSync
    @nodeSyncLift
    @scenarioIndependentTransparent
    def flatMap(@nodeLift f: A => IterableOnce[B]): That =
      AsyncBase.this.flatMap$newNode(toNodeFactory(f))(bf).get
    // noinspection ScalaUnusedSymbol
    def flatMap$queued(@nodeLift f: A => Node[IterableOnce[B]]): NodeFuture[That] =
      AsyncBase.this.flatMap$newNode(toNodeFQ(f))(bf).enqueue
    def flatMap$withNode(f: A => Node[IterableOnce[B]]): That = AsyncBase.this.flatMap$newNode(f)(bf).get
  }
  def auto[B, That](bf: BuildFrom[CC, B, That]): AutoAsync[B, That] = new AutoAsync[B, That](bf)
}

/** Async versions of some nice map-only methods. */
// Neither of these are auto-asynced right now.
@loom
trait AsyncMapBase[K, V, CC <: Map[K, V] with MapLike[K, V, CC]] { base: AsyncBase[(K, V), CC] =>

  /** Map key-value pairs to new values (same key). Just like [[Map#transform]] but more asyncy. */
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  final def transform[W, That](@nodeLift f: (K, V) => W)(implicit cbf: BuildFrom[CC, (K, W), That]): That =
    transform$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  final def transform$queued[W, That](f: (K, V) => Node[W])(implicit cbf: BuildFrom[CC, (K, W), That]): Node[That] =
    transform$newNode(toNodeFQ(f))(cbf).enqueue
  final def transform$queued[W, That](f: (K, V) => W, cbf: BuildFrom[CC, (K, W), That]): NodeFuture[That] =
    transform$newNode(toNodeFactory(f))(cbf).enqueue
  final def transform$withNode[W, That](f: (K, V) => Node[W])(implicit cbf: BuildFrom[CC, (K, W), That]): That =
    transform$newNode(f)(cbf).get
  private[this] def transform$newNode[W, That](
      f: (K, V) => Node[W],
      snti: SequenceNodeTaskInfo = NodeTaskInfo.Transform)(implicit cbf: BuildFrom[CC, (K, W), That]): Node[That] =
    if (isEmpty) emptyThat(cbf)
    else if (doSync) {
      val builder = cbf.newBuilder(c)
      base.c.foreach { case (k, v) => builder.+=((k, f(k, v).get)) }
      new AlreadyCompletedNode(builder.result())
    } else {
      // this could probably be made a little less allocation-heavy but there's no good way to externally iterate
      // over a Map that doesn't involve tuples in the current API
      val ftupled: ((K, V)) => Node[W] = { case (k, v) => f(k, v) }
      new ConverterSequenceNode2[(K, V), W, That](c, ftupled, snti, workMarker, maxConcurrency) {
        private val bldr = cbf.newBuilder(base.c)
        override def consume(w: W, kv: (K, V)): Unit = { bldr += (kv._1 -> w) }
        override def getFinalResult: That = bldr.result()
      }
    }

  // This is an alternative to mapValues which is strict and async-friendly.
  // It is equivalent to map.apar.transform((_, v) => f(v))
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  final def transformValues[W, That](@nodeLift f: V => W)(implicit cbf: BuildFrom[CC, (K, W), That]): That =
    transformValues$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  final def transformValues$queued[W, That](f: V => Node[W])(implicit cbf: BuildFrom[CC, (K, W), That]): Node[That] =
    transform$newNode((_, v) => f(v), NodeTaskInfo.TransformValues)(cbf).enqueue
  final def transformValues$withNode[W, That](f: V => Node[W])(implicit cbf: BuildFrom[CC, (K, W), That]): That =
    transform$newNode((_, v) => f(v), NodeTaskInfo.TransformValues)(cbf).get

}

class asyncPar[A, CC <: Iterable[A]](
    c: CC with IterableLike[A, CC],
    workMarker: ProgressMarker,
    maxConcurrency: Int,
    runtimeCheck: Boolean = true)
    extends AsyncBase[A, CC](c, workMarker, maxConcurrency, runtimeCheck) {
  if (maxConcurrency < 0) throw new IllegalArgumentException("maxConcurrency must be >= 0")
  def this(c: CC with IterableLike[A, CC]) = this(c, null, AsyncInternals.DefaultConcurrency)
  def this(c: CC with IterableLike[A, CC], runtimeCheck: Boolean) =
    this(c, null, AsyncInternals.DefaultConcurrency, runtimeCheck)

  // implement withFilter to avoid scalac warnings on for-comprehensions with guards.
  // This used returns an asyncPar so that the next operation is asynced as well.
  // This will cause ClassCastExceptions if/when we automatically lift into asyncPar rather than
  // asyncSeq.  Then we need to get FilterMonadic lifted as well.
  // TODO (OPTIMUS-11637): we maybe could make this lazy (as it's supposed to be), but for now this will be ok
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def withFilter(@nodeLift f: A => Boolean): asyncPar[A, CC] = withFilter$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def withFilter$queued(f: A => Node[Boolean]): NodeFuture[asyncPar[A, CC]] = withFilter$newNode(toNodeFQ(f)).enqueue
  def withFilter$withNode(f: A => Node[Boolean]): asyncPar[A, CC] = withFilter$newNode(f).get
  final private[this] def withFilter$newNode(f: A => Node[Boolean]): Node[asyncPar[A, CC]] = {
    def cast(coll: CC) = coll.asInstanceOf[CC with IterableLike[A, CC]]

    if (isEmpty) new AlreadyCompletedNode(this)
    else if (doSync) {
      val filtered: CC = c.filter(f(_).get)
      // Cast is necessary until we figure out this IterableLike stuff
      val v = cast(filtered)
      new AlreadyCompletedNode(new asyncPar[A, CC](v))
    } else
      new ConverterSequenceNode2[A, Boolean, asyncPar[A, CC]](
        c,
        f,
        NodeTaskInfo.WithFilter,
        workMarker,
        maxConcurrency) {
        private val builder = newBuilder
        override def getFinalResult = new asyncPar[A, CC](cast(builder.result()))
        override def consume(b: Boolean, org: A): Unit = if (b) builder += org
      }
  }
}

// Identical to asyncPar above, but with a different name, so we can distinguish implicitly created classes in the plugin.
final class asyncParImp[A, CC <: Iterable[A]](
    c: CC with IterableLike[A, CC],
    maxConcurrency: Int = AsyncInternals.DefaultConcurrency,
    runtimeCheck: Boolean = true)
    extends asyncPar[A, CC](c, null, maxConcurrency, runtimeCheck)

sealed class asyncSeq[A, CC <: Iterable[A]](
    c: CC with IterableLike[A, CC],
    marker: ProgressMarker = null,
    runtimeCheck: Boolean = true)
    extends AsyncBase[A, CC](c, marker, 1, runtimeCheck) {

  // implement withFilter to avoid scalac warnings on for-comprehensions with guards.
  // In the automatic case we need to return something that extends FilterMonadic[A, CC].
  // TODO (OPTIMUS-11637): we maybe could make this lazy (as it's supposed to be), but for now this will be ok
  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def withFilter(@nodeLift f: A => Boolean): CC = withFilter$withNode(toNodeFactory(f))
  // noinspection ScalaUnusedSymbol
  def withFilter$queued(f: A => Node[Boolean]): NodeFuture[CC] = withFilter$newNode(toNodeFQ(f)).enqueue
  def withFilter$withNode(f: A => Node[Boolean]): CC = withFilter$newNode(f).get
  final private[this] def withFilter$newNode(f: A => Node[Boolean]) =
    if (isEmpty) new AlreadyCompletedNode(c)
    else
      new ConverterSequenceNode2[A, Boolean, CC](c, f, NodeTaskInfo.Filter, workMarker, maxConcurrency) {
        private val builder = newBuilder
        override def getFinalResult: CC = builder.result()
        override def consume(b: Boolean, org: A): Unit = if (b) builder += org
      }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  override def foldLeft[B](z: B)(@nodeLift op: (B, A) => B): B = foldLeft$withNode(z)(toNodeFactory(op))
  override def foldLeft$queued[B](z: B)(op: (B, A) => Node[B]): NodeFuture[B] = super.foldLeft$queued(z)(op)
  override def foldLeft$newNode[B](z: B)(op: (B, A) => Node[B]): Node[B] = super.foldLeft$newNode(z)(op)
  override def foldLeft$withNode[B](z: B)(op: (B, A) => Node[B]): B = super.foldLeft$withNode(z)(op)
}

final class asyncParMap[K, V, CC <: Map[K, V] with MapLike[K, V, CC]](
    c: CC with IterableLike[(K, V), CC],
    workMarker: ProgressMarker = null,
    maxConcurrency: Int = AsyncInternals.DefaultConcurrency,
    runtimeCheck: Boolean = true
) extends asyncPar[(K, V), CC](c, workMarker, maxConcurrency, runtimeCheck)
    with AsyncMapBase[K, V, CC]

final class asyncSeqMap[K, V, CC <: Map[K, V] with MapLike[K, V, CC]](
    c: CC with IterableLike[(K, V), CC],
    marker: ProgressMarker = null,
    runtimeCheck: Boolean = true
) extends asyncSeq[(K, V), CC](c, marker, runtimeCheck)
    with AsyncMapBase[K, V, CC]
