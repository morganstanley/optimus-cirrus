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

import optimus.graph.CompletableNodeM
import optimus.graph.NodeTask
import optimus.platform.annotations.nodeSync
import optimus.graph.Node
import optimus.graph.AlreadyCompletedNode
import optimus.graph.AlreadyFailedNode
import optimus.graph.CompletableNode
import optimus.graph.NodeFuture
import optimus.graph.NodeTaskInfo
import optimus.graph.OGSchedulerContext

import scala.annotation.nowarn

/**
 * Optimus version partialFunction rewrite, to make partial function calls(apply/isDefindAt) can be asynced.
 *
 * We don't extend from AbstractPartialFunction, because for the OptimusPartialFunction, we only support a limited
 * method set.
 *
 * We don't have the $_newNode implementation, because that needs plugin to transform. The implementation details is in
 * OptimusPartialFunctionMarco.scala
 *
 * We don't extends NodeFunction1 until there is such requirement
 */
// The `Unit` value is deprecated in 2.12 (by AnnotatingComponent). In 2.13 it's `@compileTimeOnly`, so the `@nowarn`
// here can be removed after the migration.
@nowarn("msg=10500 scala.Unit")
trait OptimusPartialFunction[
    @specialized(scala.Int, scala.Long, scala.Float, scala.Double, scala.AnyRef) -T1,
    @specialized(scala.Unit, scala.Boolean, scala.Int, scala.Float, scala.Long, scala.Double, scala.AnyRef) +R] {
  // extends NodeFunction1[T1, R] {

  //  @nodeSync
  //  def applyOrElse[A1 <: T1, B1 >: R](x: A1, default: NodeFunction1[A1, B1]): B1 = applyOrElse$_newNode[A1, B1](x, default).get
  //  def applyOrElse$queued[A1 <: T1, B1 >: R](x: A1, default: NodeFunction1[A1, B1]): Node[B1] = applyOrElse$_newNode[A1, B1](x, default).enqueue
  //  def applyOrElse$withNode[A1 <: T1, B1 >: R](x: A1, default: NodeFunction1[A1, B1]): B1 = applyOrElse$_newNode[A1, B1](x, default).get
  //  protected def applyOrElse$_newNode[A1 <: T1, B1 >: R](x: A1, default: NodeFunction1[A1, B1]): Node[B1]

  @nodeSync
  final def apply(x: T1): R = apply$_newNode(x).get
  final def apply$queued(x: T1): NodeFuture[R] = apply$_newNode(x).enqueue
  final def apply$withNode(x: T1): R = apply$_newNode(x).get
  private[platform] def apply$_newNode(x: T1): Node[R]

  @nodeSync
  final def isDefinedAt(x: T1): Boolean = isDefinedAt$_newNode(x).get
  final def isDefinedAt$queued(x: T1): NodeFuture[Boolean] = isDefinedAt$_newNode(x).enqueue
  final def isDefinedAt$withNode(x: T1): Boolean = isDefinedAt$_newNode(x).get
  protected def isDefinedAt$_newNode(x: T1): Node[Boolean]

  def tryApply$newNode(x: T1): Node[Option[R]] = {
    val pf = OptimusPartialFunction.this
    new CompletableNodeM[Option[R]] {
      var notKnownDefined = true

      override def executionInfo: NodeTaskInfo = NodeTaskInfo.PartialFunction
      override def run(ec: OGSchedulerContext): Unit = {
        val n2 = pf.isDefinedAt$queued(x)
        n2.continueWith(this, ec)
      }
      override def onChildCompleted(eq: EvaluationQueue, node: NodeTask): Unit = {
        this.combineInfo(node, eq)
        if (node.isDoneWithException)
          this.completeWithException(node.exception, eq)
        else if (notKnownDefined) {
          if (node.resultObject().asInstanceOf[Boolean]) {
            notKnownDefined = false
            val n3 = pf.apply$_newNode(x)
            if (!n3.isDone) {
              n3.attach(this.scenarioStack())
              eq.enqueue(this, n3)
            }
            n3.continueWith(this, eq)
          } else {
            this.completeWithResult(None, eq)
          }
        } else {
          val b = node.asInstanceOf[Node[R]].result
          this.completeWithResult(Some(b), eq)
        }
      }
    }
  }

  // n.b. *not* @nodeSync because this just returns a new OptimusPartialFunction; it doesn't execute anything
  def orElse[A1 <: T1, B1 >: R](that: OptimusPartialFunction[A1, B1]): OptimusPartialFunction[A1, B1] =
    new OrElsePartialFunction(this, that)

  //  def andThen[C](k: NodeFunction1[R, C]): OptimusPartialFunction[T1, C]
  //  def lift: NodeFunction1[T1, OptAsync[R]] // TODO (OPTIMUS-0000): !!! - What's better here? OptAsync or Option?
  //  def runWith[U](action: NodeFunction1[R, U]): NodeFunction1[T1, Boolean]
}

object OptimusPartialFunction {

  private[this] val empty_opf: OptimusPartialFunction[Any, Nothing] = new OptimusPartialFunction[Any, Nothing] {
    final override def apply$_newNode(x: Any): Node[Nothing] = new AlreadyFailedNode(new MatchError(x))
    final override protected def isDefinedAt$_newNode(x: Any): Node[Boolean] = new AlreadyCompletedNode(false)
  }

  def empty[A, B]: OptimusPartialFunction[A, B] = empty_opf
}

private class OrElsePartialFunction[-A, +B](left: OptimusPartialFunction[A, B], right: OptimusPartialFunction[A, B])
    extends OptimusPartialFunction[A, B] {
  override private[platform] def apply$_newNode(x: A): Node[B] = new CompletableNode[B] {
    private var state: Int = _
    private var child: NodeFuture[_] = _
    override def executionInfo(): NodeTaskInfo = NodeTaskInfo.OrElse
    override def run(ec: OGSchedulerContext): Unit = state match {
      case 0 =>
        state = 1
        child = left.isDefinedAt$queued(x)
        child.continueWith(this, ec)
      // was waiting for left.isDefinedAt
      case 1 =>
        val c = child.asInstanceOf[CompletableNode[Boolean]]
        combineInfo(c, ec)
        if (c.isDoneWithException) completeWithException(c.exception(), ec)
        else if (c.result) {
          // left isDefinedAt x, so schedule left.apply
          state = 3
          child = left.apply$queued(x)
          child.continueWith(this, ec)
        } else {
          // left was not defined at x, so schedule right.isDefined
          state = 2
          child = right.isDefinedAt$queued(x)
          child.continueWith(this, ec)
        }
      // was waiting for right.isDefinedAt
      case 2 =>
        val c = child.asInstanceOf[CompletableNode[Boolean]]
        combineInfo(c, ec)
        if (c.isDoneWithException) completeWithException(c.exception(), ec)
        else if (c.result) {
          // right isDefinedAt x, so schedule right.apply
          state = 3
          child = right.apply$queued(x)
          child.continueWith(this, ec)
        } else {
          // neither left nor right isDefinedAt x
          completeWithException(new MatchError(x), ec)
        }
      // was waiting for left.apply or right.apply
      case 3 => completeFromNode(child.asInstanceOf[CompletableNode[B]], ec)
    }
  }
  override protected def isDefinedAt$_newNode(x: A): Node[Boolean] = new CompletableNode[Boolean] {
    private var state: Int = _
    private var child: CompletableNode[Boolean] = _
    override def executionInfo(): NodeTaskInfo = NodeTaskInfo.OrElse
    override def run(ec: OGSchedulerContext): Unit = state match {
      case 0 =>
        state = 1
        child = left.isDefinedAt$queued(x).asInstanceOf[CompletableNode[Boolean]]
        child.continueWith(this, ec)
      // was waiting for left isDefinedAt
      case 1 =>
        // left isDefinedAt returned true or failed - either way we're done
        if (child.isDoneWithException || child.result) completeFromNode(child, ec)
        else {
          // left.isDefinedAt returned false but still need to combineInfo from left
          combineInfo(child, ec)
          // now try right.isDefinedAt
          state = 2
          child = right.isDefinedAt$queued(x).asInstanceOf[CompletableNode[Boolean]]
          child.continueWith(this, ec)
        }
      // was waiting for right.isDefinedAt
      case 2 =>
        // whatever happened (true/false/exception), this is our result
        completeFromNode(child, ec)
    }
  }
}
