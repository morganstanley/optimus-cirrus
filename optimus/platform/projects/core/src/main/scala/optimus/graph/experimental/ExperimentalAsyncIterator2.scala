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
package optimus.graph.experimental

import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.LinkedBlockingQueue
import optimus.platform.EvaluationQueue
import optimus.platform.annotations._
import optimus.platform.util.Log
import optimus.platform.NodeFunction0
import optimus.platform.asNode
import optimus.scalacompat.collection.IterableLike

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import optimus.graph._
import optimus.platform.PluginHelpers.toNodeFactory

trait ExperimentalAsyncIterator2[A] {
  import ExperimentalAsyncIterator2._

  // will ultimately be returned
  private var last: Terminal[A] = _
  private[this] var completelyDone = false

  protected def typ: String
  val id = s"$typ@${System.identityHashCode(this)}"

  // Might return:
  //   AlreadyCompleted(Some(a))
  //   CompletableNode which should not have been started
  //   AlreadyCompleted(None)
  //   AlreadyFailed
  // For the last two, we distinguish between nodes that just happen to be done and nodes actually extend Terminal
  // Contract:
  //   If you pull a next(),
  //   1. If not completed, you must run it
  //   2. If it's empty, but non-terminal, ignore the value
  //   3. If it's terminal, complete
  def next: NO[A]

  def maybeSetLast(n: NO[_]): Terminal[A] = this.synchronized {
    n match {
      case Terminal(t) =>
        if ((last eq null) || (last.isDoneWithResult && t.isDoneWithException))
          last = t.to[A]
      case _ =>
    }
    last
  }

  def buf(ccy: Int, maxBuffered: Int = Int.MaxValue): ExperimentalAsyncIterator2[A] = {
    val in = this
    new ExperimentalAsyncIterator2[A] {
      ai =>
      override protected def typ = "buf"
      var trulyLast: NO[A] = _
      private var maxActuallyOffered = 0
      private var maxActuallyBuffered = 0
      private val offered = new LinkedBlockingQueue[CO[A]]()
      private val preLaunched = new LinkedBlockingQueue[NO[A]]()
      private var barrier: BarrierNode = _

      private class BarrierNode extends CompletableNodeM[Unit] {
        override def run(ec: OGSchedulerContext): Unit = {
          val done = ai.synchronized(ai.barrier eq null)
          if (done) {
            logger.debug(s"$id barrier already gone")
            completeWithResult((), ec)
          } else {
            logger.debug(s"$id barrier waiting, this=${disp(this)}")
          }
        }
        override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
          logger.debug(s"$id barrier completing")
          this.completeWithResult((), eq)
        }
      }

      private val coordinator: ConcurrencyManager = new ConcurrencyManager {
        override def maxConcurrency: Int = {
          // This will slow down the rate of taking,
          // At the same time, we'll give nextIteration a node that just waits until preFetch is reduced
          //
          val ps = ai.synchronized(preLaunched.size)
          if (preLaunched.size > maxBuffered) {
            logger.debug(s"$id-$i preLaunched=$ps => restricting ccy")
            1
          } else ccy
        }

        private class InputLaunchNode(ii: Int, prev: NO[A], preOffer: CO[A]) extends CompletableNodeM[Unit] {
          override def toString = s"$id-it-$ii-" + super.toString
          override def run(ec: OGSchedulerContext): Unit = {
            logger.debug(s"$id-it-$ii this.run: prev=${disp(prev)}")
            prev.attach(scenarioStack())
            ec.enqueue(prev)
            prev.continueWith(this, ec)
          }
          override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
            logger.debug(s"$id-it-$ii completing prev=${disp(prev)} -> o=${disp(preOffer)}")
            // If preOffer is null, this is in the pre-launched queue
            if (preOffer ne null) { //
              logger.debug(s"$id-it-$ii-occ completing from ${disp(prev)}")
              preOffer.attach(scenarioStack())
              eq.enqueue(preOffer) // should be fast
              prev.continueWith(preOffer, eq)
            }
            maybeSetLast(prev)
            completeWithResult((), eq)
          }
        }

        override def run(ec: OGSchedulerContext): Unit = {
          logger.debug(s"$id starting coordinator minRunning=$ccy, maxbuffered=$maxBuffered")
          super.run(ec)
        }
        var i = 0
        var readDone = false
        override protected def sequenceExecutionInfo: SequenceNodeTaskInfo = NodeTaskInfo.Sequence
        override def hasNextIteration: Boolean = ai.synchronized { !readDone }
        override def nextIteration: Iteration = ai.synchronized {
          i += 1
          if (barrier ne null) {
            logger.debug(s"$id-$i returning premade barrier ${disp(barrier)}")
            new Iteration(barrier)
          } else if (preLaunched.size > maxBuffered && (barrier eq null)) {
            logger.debug(s"$id-$i preLaunched=${preLaunched.size}, creating barrier node")
            barrier = new BarrierNode
            new Iteration(barrier)
          } else {
            val consumed = offered.poll()
            val prev = prevUnlessTerminal(in, ai) match {
              case Left(terminal) =>
                logger.debug(s"$id-$i-nextIteration 'preLaunching' terminal ${disp(terminal)}")
                preLaunched.offer(terminal)
                readDone = true
                terminal // this will be boring
              case Right(prev) =>
                if (consumed eq null)
                  preLaunched.offer(prev)
                if (preLaunched.size > maxActuallyBuffered) maxActuallyBuffered = preLaunched.size()
                prev
            }
            logger.debug(s"$id-$i-nextIteration snagged ${disp(prev)}")
            val node = new InputLaunchNode(i, prev, consumed)
            if (logger.isDebugEnabled())
              trackIncomplete(ai, node)
            logger.debug(
              s"nextIteration $id $i returning prev=${disp(prev)} --> iter=${disp(node)} --> offered=${disp(consumed)}")
            new Iteration(node)
          }
        }
        override def consumeIteration(i: Iteration): Unit = {}
        override def getFinalResult: Unit = {
          logger.debug(s"$id getFinalResult maxPreLaunched=$maxActuallyBuffered maxOffered=$maxActuallyOffered")
        }
      }

      val cleanup = new NodeAwaiter {
        override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
          logger.debug(s"$id-cleanup")
          assert(node eq coordinator)
          val os = new ArrayBuffer[CO[A]]()
          ai.synchronized {
            last = terminal()
            preLaunched.offer(last)
            while (!offered.isEmpty) os += offered.remove()
          }
          os.foreach { o =>
            if (o.scenarioStack == null)
              o.attach(node.scenarioStack)
            eq.enqueue(o)
            last.continueWith(o, eq)
          }
        }
      }

      private def processBarrier(eq: EvaluationQueue): Unit = {
        val b = ai.synchronized {
          if (barrier ne null) {
            val tmp = barrier
            barrier = null
            tmp
          } else null
        }
        if (b ne null) {
          logger.debug(s"$id removed barrier, now completing ${disp(b)}")
          b.onChildCompleted(eq, null)
        }
      }

      private class BufferProxy(i: Int, doProcesBarrier: Boolean, completeImmediately: Boolean)
          extends CompletableNodeM[Option[A]] {
        override def toString = s"$id-proxy-$i-$doProcesBarrier-$completeImmediately-" + super.toString
        override def run(ec: OGSchedulerContext): Unit = {
          if (doProcesBarrier) processBarrier((ec))
          if (i == 0) {
            coordinator.attach(scenarioStack)
            ec.enqueue(coordinator)
            coordinator.continueWith(cleanup, ec)
            logger.debug(s"$id enqueued coordinator ${disp(coordinator)}")
          }
          if (completeImmediately)
            completeWithResult(None, ec)
        }
        override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
          val completedPrev = child.asInstanceOf[NO[A]]
          if (isTerminal(completedPrev))
            logger.debug(s"$id completing iteration with terminal")
          this.completeFromNode(completedPrev, eq)
        }
      }

      var i = 0
      override def next: NO[A] = this.synchronized {
        val iSnap = i
        val hadBarrier = barrier ne null
        i += 1
        if (!preLaunched.isEmpty) {
          val pre = preLaunched.remove()
          val ret = if (isTerminal(pre)) {
            trulyLast = pre
            assert(preLaunched.isEmpty || isTerminal(preLaunched.peek()))
            if (hadBarrier) {
              val proxy = new BufferProxy(iSnap, hadBarrier, true)
              preLaunched.offer(pre) // put back the terminal
              proxy
            } else
              pre
          } else {
            assert(offered.isEmpty)
            pre
          }
          logger.debug(s"$id returning prelaunched  ${disp(ret)}")
          ret
        } else if (trulyLast ne null) {
          logger.debug(s"$id returning terminal ${disp(trulyLast)}")
          trulyLast
        } else {
          val proxy = new BufferProxy(iSnap, hadBarrier, false)
          trackIncomplete(ai, proxy)
          logger.debug(s"$id returning proxy ${disp(proxy)}")
          offered.offer(proxy)
          if (offered.size() > maxActuallyOffered) maxActuallyOffered = offered.size()
          proxy
        }
      }
    }
  }

  @nodeSyncLift
  def map[B](@nodeLift f: A => B)(): ExperimentalAsyncIterator2[B] = map$withNode(toNodeFactory(f))
  def map$withNode[B](f: A => Node[B]): ExperimentalAsyncIterator2[B] = {
    val in = this
    // keep track of all requested nodes, and set definitivelyDone when they've all completed, at least one of them done
    new ExperimentalAsyncIterator2[B] {
      ai =>
      override def typ = "map"
      override def next: NO[B] = this.synchronized {
        prevUnlessTerminal(in, ai) match {
          case Left(terminal) => terminal
          case Right(inputNode) =>
            val y = mapOptionNode(id, inputNode, f)
            y
        }
      }
    }
  }

  @nodeSyncLift
  def flatMap0[B](@nodeLift f: A => Iterable[B])(): ExperimentalAsyncIterator2[B] = flatMap0$withNode(toNodeFactory(f))
  def flatMap0$withNode[B](f: A => Node[Iterable[B]]): ExperimentalAsyncIterator2[B] = concat0(this.map$withNode(f))
  @nodeSyncLift
  def samplingPartition[B](@nodeLift f: NodeFunction0[Option[A]] => Option[B]): ExperimentalAsyncIterator2[B] =
    samplingPartition$withNode(toNodeFactory(f))
  def samplingPartition$withNode[B](
      @nodeLift f: NodeFunction0[Option[A]] => Node[Option[B]]): ExperimentalAsyncIterator2[B] = {
    val in = this
    new ExperimentalAsyncIterator2[B] {
      ai =>
      override def typ = "samplingPartition"
      var last: NO[B] = _
      val supplier = asNode.apply$withNode[Option[A]] { () =>
        ai.synchronized {
          prevUnlessTerminal(in, ai) match {
            case Left(term: Terminal[B]) =>
              last = term
              terminal[A]()
            case Right(prev) => prev
          }
        }
      }
      override def next: NO[B] = this.synchronized {
        if (last ne null) last
        else f(supplier)
      }
    }
  }

  @nodeSync
  @nodeSyncLift
  @scenarioIndependentTransparent
  def force(maxCcy: Int = 1): Iterable[A] = force$newNode(maxCcy).get
  def force$queued(maxCcy: Int): NodeFuture[Iterable[A]] = force$newNode(maxCcy).enqueue
  def force$withNode(maxCcy: Int): Iterable[A] = force$newNode(maxCcy).get
  def force$newNode(maxCcy: Int): Node[Iterable[A]] = {
    val in = if (maxCcy <= 1) this else this.buf(maxCcy)
    val as = new ArrayBuffer[A]
    val node = new CompletableNodeM[Iterable[A]] {
      override def toString: String = "force-" + super.toString
      override def run(ec: OGSchedulerContext): Unit = {
        onChildCompleted(ec, null)
      }
      override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
        occ(eq, child)
        @tailrec def occ(eq: EvaluationQueue, child: NodeTask): Unit = {
          val node = child.asInstanceOf[NO[A]]
          logger.debug(s"$id occ ${disp(this)} completed by ${disp(node)}")
          val stop =
            if (node eq null)
              false
            else if (node.isDoneWithException) {
              completeWithException(node.exception, eq)
              true
            } else if (node.result.isEmpty) {
              completeWithResult(as, eq)
              true
            } else {
              as += node.result.get
              false
            }
          if (!stop)
            prevUnlessTerminal(in, null) match {
              case Left(done) =>
                if (done.isDoneWithException)
                  completeWithException(done.exception, eq)
                else {
                  completeWithResult(as, eq)
                  if (done.result.isEmpty)
                    logger.debug(s"$id occ caught terminal ${disp(done)}")
                }
              case Right(next) =>
                logger.debug(s"$id fetched and enqueuing ${disp(next)}")
                next.attach(scenarioStack())
                eq.enqueue(next)
                if (!next.tryAddToWaiterList(this))
                  occ(eq, next)
            }
        }
      }

    }
    logger.debug(s"force returning ${disp(node)}")
    trackIncomplete(this, node)
    node
  }

}

trait AsyncIteratorConsumer[A] {}

object ExperimentalAsyncIterator2 extends Log {

  private def logger = log
  private type NO[X] = Node[Option[X]]
  private type CO[X] = CompletableNodeM[Option[X]]

  trait Terminal[X] extends Node[Option[X]] {
    override def toString = "TERMINAL:" + super.toString
    def to[A]: Terminal[A] = this.asInstanceOf[Terminal[A]]
  }

  object Terminal {
    def unapply(node: NO[_]): Option[Terminal[_]] = {
      if (isTerminal(node)) Some(node.asInstanceOf[Terminal[_]])
      else if (node.isDoneWithException) Some(terminal(node.exception()))
      else None
    }
  }

  class TerminalCompletedNode[A] extends AlreadyCompletedNode(None) with Terminal[A]
  class TerminalFailedNode[A](e: Throwable) extends AlreadyFailedNode(e) with Terminal[A]

  private def some[X](x: X) = new AlreadyCompletedNode[Option[X]](Some(x))
  private def terminal[X](): Terminal[X] = new TerminalCompletedNode
  private def terminal[X](e: Throwable): Terminal[X] = new TerminalFailedNode(e)
  private def isTerminal(n: NO[_]) = n.isInstanceOf[Terminal[_]]

  private val timer = new Timer
  private def trackIncomplete(in: ExperimentalAsyncIterator2[_], node: Node[_]): Unit = {
    val task = new TimerTask {
      override def run(): Unit = {
        if (!node.isDone) {
          log.warn(s"Horrible! $in ${disp(node)}")
        }

      }
    }
    timer.schedule(task, 10000L)
  }

  private def mapOptionNode[A, B](id: String, outer: NO[A], f: A => Node[B]): CO[B] = {
    val node = new CompletableNodeM[Option[B]] {
      override def toString = s"$id-mapOptionNode-" + super.toString
      override def run(ec: OGSchedulerContext): Unit = {
        logger.debug(s"$id run outer=${disp(outer)} this=${disp(this)}")
        outer.attach(scenarioStack())
        ec.enqueue(outer)
        outer.continueWith(this, ec)
      }
      var bNode: Node[B] = _
      override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
        logger.debug(s"$id occ child=${disp(child.asInstanceOf[NO[_]])} bnode=${disp(bNode)}")
        combineInfo(child, eq)
        if (child.isDoneWithException)
          completeWithException(child.exception(), eq)
        else if (!child.isDoneWithResult)
          throwNodeCompletionException(child.asInstanceOf[Node[_]])
        else if (child eq outer) {
          if (outer.result.isEmpty) completeWithResult(None, eq)
          else {
            bNode = f(outer.result.get)
            bNode.attach(scenarioStack())
            eq.enqueue(bNode)
            bNode.continueWith(this, eq)
            logger.debug(s"mapOptionNode onCC outer=${disp(outer)} this=${disp(this)} bNode=${disp(bNode)}")
          }
        } else {
          assert(child eq bNode)
          logger.debug(s"$id completing Some(${bNode.result} -> ${disp(this)}")
          completeWithResult(Some(bNode.result), eq)
        }
      }
    }
    node
  }

  def from[A, CC <: Iterable[A]](c: CC with IterableLike[A, CC]): ExperimentalAsyncIterator2[A] =
    new ExperimentalAsyncIterator2[A] {
      def typ = "from"
      val i = c.iterator
      // note: it's ok if this turns out to be untrue
      override def next: NO[A] = {
        this.synchronized {
          if (i.hasNext)
            some(i.next())
          else
            terminal[A]()
        }
      }
    }

  def fromNodes[A](nodes: Iterable[NO[A]]): ExperimentalAsyncIterator2[A] = new ExperimentalAsyncIterator2[A] {
    def typ = "fromNodes"
    val i = nodes.iterator
    override def next: NO[A] = this.synchronized {
      if (!i.hasNext) terminal()
      else i.next()
    }
  }

  def compose[A] = new AsyncIteratorComposer[A, A](identity)

  def disp(cn: Node[_]) = {
    if (cn eq null) "null"
    else {
      val id = s"$cn@${System.identityHashCode(cn)}"
      if (cn.isDoneWithResult) s"$id:${cn.result}"
      else if (cn.isDoneWithException) s"$id:${cn.exception()}"
      else s"$id:notdone"
    }
  }

  def concat0[A](in: ExperimentalAsyncIterator2[Iterable[A]]): ExperimentalAsyncIterator2[A] =
    new ExperimentalAsyncIterator2[A] {
      def typ = "concat0"
      var last: NO[A] = null
      val offered = new LinkedBlockingQueue[CO[A]]()
      val launched = new LinkedBlockingQueue[NO[Iterable[A]]]()
      val ready = new ArrayBuffer[A]()
      def sweep(eq: EvaluationQueue): Unit = {
        val finish = new ArrayBuffer[(A, CO[A])]
        this.synchronized {
          logger.debug(s"$id-sweep running offered=${offered.size} launched=${launched.size} ready=${ready.size}")
          while (!launched.isEmpty && launched.peek().isDone) {
            val child = launched.remove()
            child match {
              case Terminal(t) =>
                logger.debug(s"$id-sweep setting terminal ${disp(t)}")
                last = t.to[A]
              case _ =>
                logger.debug(s"$id-sweep adding ${child.result}")
                child.result.foreach { as =>
                  ready ++= as
                }
            }
          }
          var i = 0
          while (!offered.isEmpty && i < ready.length) {
            finish += ((ready(i), offered.remove()))
            i += 1
          }
          ready.remove(0, i)
        }
        finish.foreach { case (a, o) =>
          logger.debug(s"$id-sweep completing ${disp(o)} with $a")
          o.completeWithResult(Some(a), eq)
        }
        if (last ne null) {
          logger.debug(s"$id-sweep completing remaining ${offered.size} with ${disp(last)}")
          while (!offered.isEmpty) offered.remove().completeFromNode(last, eq)
        }
      }
      var i = 0
      override def next: NO[A] = this.synchronized {
        if (last ne null) {
          if (!ready.isEmpty) {
            val ret = new AlreadyCompletedNode(Some(ready(0)))
            ready.remove(0)
            ret
          } else last
        } else {
          i += 1
          val prev = in.next
          val iSnap = i
          val proxy = new CompletableNodeM[Option[A]] {
            override def run(ec: OGSchedulerContext): Unit = {
              logger.debug(s"$id-proxy-$iSnap launching ${disp(prev)}")
              prev.attach(scenarioStack())
              ec.enqueue(prev)
              prev.continueWith(this, ec)
            }
            override def onChildCompleted(eq: EvaluationQueue, child: NodeTask): Unit = {
              logger.debug(s"$id-proxy-$iSnap completing with ${disp(child.asInstanceOf[Node[_]])}")
              sweep(eq)
            }
          }
          launched.offer(prev)
          offered.offer(proxy)
          proxy
        }
      }
    }

  // Swallows empty
  final protected def prevUnlessTerminal[P, A](
      in: ExperimentalAsyncIterator2[P],
      to: ExperimentalAsyncIterator2[A]): Either[Terminal[A], NO[P]] = {
    @tailrec
    def loop(in: ExperimentalAsyncIterator2[P]): Either[Terminal[A], NO[P]] = {
      in.next match {
        case Terminal(t) =>
          if (to ne null) {
            if ((to.last eq null) || (to.last.isDoneWithResult && t.isDoneWithException))
              to.last = t.to[A]
            Left(to.last)
          } else Left(t.to[A])
        case n if n.isDoneWithResult && n.result.isEmpty =>
          // Skip over empty but not terminal results
          loop(in)
        case n =>
          Right(n)
      }
    }
    logger.debug(s"$to pulling from ${in.id}")
    if (to ne null)
      to.synchronized(loop(in))
    else
      loop(in)
  }
}

class AsyncIteratorComposer[A, B](f: ExperimentalAsyncIterator2[A] => ExperimentalAsyncIterator2[B]) {
  @nodeSync
  @nodeSyncLift
  def map[C](@nodeLift g: B => C): AsyncIteratorComposer[A, C] = map$withNode(toNodeFactory(g))
  def map$withNode[C](g: B => Node[C]): AsyncIteratorComposer[A, C] =
    new AsyncIteratorComposer[A, C](a => f(a).map$withNode(g))
  def iterator[CC <: Iterable[A]](c: CC with IterableLike[A, CC]): ExperimentalAsyncIterator2[B] = {
    val it = ExperimentalAsyncIterator2.from[A, CC](c)
    f(it)
  }

}
