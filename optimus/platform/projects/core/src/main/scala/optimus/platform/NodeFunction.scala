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
import optimus.graph.AlreadyCompletedNode
import optimus.graph.Node
import optimus.graph.NodeClsIDSupport
import optimus.graph.OGSchedulerContext
import optimus.graph.UnsafeInternal
import optimus.graph.profiled.NodeDelegate
import optimus.platform.PluginHelpers._
import optimus.platform.annotations.captureByValue
import optimus.platform.annotations.nodeLift
import optimus.platform.annotations.nodeLiftByName
import optimus.platform.annotations.nodeSync
import optimus.platform.annotations.nodeSyncLift
import optimus.platform.annotations.withNodeClassID
import optimus.platform.asNodeInnards.NodeFunction1ImplSS

/*
  asAsync { ... } lifts N-ary closures to AsyncFunctionN
  asNode  { ... } lifts N-ary closures to NodeFunctionN

  AsyncFunctionN.apply is @impure
  NodeFunctionN.apply  is assumed pure
 */

trait AsyncFunction0[+R] extends Serializable {
  @impure
  @nodeSync
  def apply(): R
  @impure
  def apply$queued(): Node[R]
}

trait AsyncFunction1[-T1, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1): R
  @impure
  def apply$queued(v1: T1): Node[R]
}
case object AsyncFunction1 {
  // no sense in creating many of these
  private val _identity: AsyncFunction1[AnyRef, AnyRef] = new AsyncFunction1[AnyRef, AnyRef] {
    // no actual reason to submit the node if we do end up here
    @nodeSync def apply(v1: AnyRef): AnyRef = v1
    def apply$queued(v1: AnyRef) = new AlreadyCompletedNode(v1)
  }
  def identity[T]: AsyncFunction1[T, T] = _identity.asInstanceOf[AsyncFunction1[T, T]]
}

trait AsyncFunction2[-T1, -T2, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1, v2: T2): R
  @impure
  def apply$queued(v1: T1, v2: T2): Node[R]
}

trait AsyncFunction3[-T1, -T2, -T3, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3): R
  @impure
  def apply$queued(v1: T1, v2: T2, v3: T3): Node[R]
}

trait AsyncFunction4[-T1, -T2, -T3, -T4, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4): R
  @impure
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4): Node[R]
}

trait AsyncFunction5[-T1, -T2, -T3, -T4, -T5, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): R
  @impure
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): Node[R]
}

trait AsyncFunction6[-T1, -T2, -T3, -T4, -T5, -T6, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): R
  @impure
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): Node[R]
}

trait AsyncFunction7[-T1, -T2, -T3, -T4, -T5, -T6, -T7, +R] extends Serializable {
  @nodeSync
  @impure
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): R
  @impure
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): Node[R]
}

trait AsyncFunction8[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): R
  @impure
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): Node[R]
}

trait AsyncFunction9[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, -T9, +R] extends Serializable {
  @impure
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): R
  @impure
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): Node[R]
}

trait HasNewNode[+R] {
  // We need the default because of NodeFunction extensions outside of platform where it might not be
  // possible to return an unqueued node.
  def apply$newNode(): Node[R]
}

trait NodeFunction0[+R] extends AsyncFunction0[R] {
  @nodeSync
  def apply(): R
  def apply$queued(): Node[R]
}

trait NodeFunction0NN[+R] extends NodeFunction0[R] with HasNewNode[R]

trait NodeFunction1[-T1, +R] extends AsyncFunction1[T1, R] {
  @nodeSync
  def apply(v1: T1): R
  def apply$queued(v1: T1): Node[R]
}

trait HasFastNodeGenerator[-T1, +R] {
  private[optimus] def nodeGenerator: AnyRef
}

case object NodeFunction1 {
  private[optimus] def nodeGenerator[T1, R](nf: NodeFunction1[T1, R]): AnyRef =
    nf match {
      case n: HasFastNodeGenerator[_, _] => n.nodeGenerator
      case _ =>
        (v: T1) =>
          new NodeDelegate[R] {
            override protected def childNode: Node[R] = nf.apply$queued(v)
          }
    }

  // no sense in creating many of these
  private val _identity: NodeFunction1[AnyRef, AnyRef] = new NodeFunction1[AnyRef, AnyRef] {
    // no actual reason to submit the node if we do end up here
    @nodeSync def apply(v1: AnyRef): AnyRef = v1
    def apply$queued(v1: AnyRef) = new AlreadyCompletedNode(v1)
  }
  def identity[T]: NodeFunction1[T, T] = _identity.asInstanceOf[NodeFunction1[T, T]]
}

trait NodeFunction2[-T1, -T2, +R] extends AsyncFunction2[T1, T2, R] {
  @nodeSync
  def apply(v1: T1, v2: T2): R
  def apply$queued(v1: T1, v2: T2): Node[R]
}

trait NodeFunction3[-T1, -T2, -T3, +R] extends AsyncFunction3[T1, T2, T3, R] {
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3): R
  def apply$queued(v1: T1, v2: T2, v3: T3): Node[R]
}

trait NodeFunction4[-T1, -T2, -T3, -T4, +R] extends AsyncFunction4[T1, T2, T3, T4, R] {
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4): R
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4): Node[R]
}

trait NodeFunction5[-T1, -T2, -T3, -T4, -T5, +R] extends AsyncFunction5[T1, T2, T3, T4, T5, R] {
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): R
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): Node[R]
}

trait NodeFunction6[-T1, -T2, -T3, -T4, -T5, -T6, +R] extends AsyncFunction6[T1, T2, T3, T4, T5, T6, R] {
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): R
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): Node[R]
}

trait NodeFunction7[-T1, -T2, -T3, -T4, -T5, -T6, -T7, +R] extends AsyncFunction7[T1, T2, T3, T4, T5, T6, T7, R] {
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): R
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): Node[R]
}

trait NodeFunction8[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, +R]
    extends AsyncFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] {
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): R
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): Node[R]
}

trait NodeFunction9[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, -T9, +R]
    extends AsyncFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] {
  @nodeSync
  def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): R
  def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): Node[R]

}

object asAsyncInnards {

  private[optimus /*platform*/ ] final class AsyncFunction0Impl[+R](val vn: () => Node[R]) extends AsyncFunction0[R] {
    @impure
    @nodeSync
    def apply(): R = vn().get
    @impure
    def apply$queued(): Node[R] = vn().enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction0Impl[_] => NodeClsIDSupport.equals(vn, that.vn)
      case _                           => false
    }
    @transient private[this] var cachedHashCode: Int = 0
    // noinspection HashCodeUsesVar
    override def hashCode(): Int = {
      if (cachedHashCode == 0)
        cachedHashCode = NodeClsIDSupport.hashCode(vn)
      cachedHashCode
    }
  }

  @nodeSyncLift
  @nodeLiftByName
  def apply[T1, R](@nodeLift @withNodeClassID f: T1 => R): AsyncFunction1[T1, R] = optimus.core.needsPlugin
  def apply$withNode[T1, R](vn: T1 => Node[R]): AsyncFunction1[T1, R] = new AsyncFunction1Impl(vn)

  private[optimus /*platform*/ ] final class AsyncFunction1Impl[-T1, +R](val vn: T1 => Node[R])
      extends AsyncFunction1[T1, R] {
    @impure
    @nodeSync
    def apply(v1: T1): R = vn(v1).get
    @impure
    def apply$queued(v1: T1): Node[R] = vn(v1).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction1Impl[_, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                              => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction2Impl[-T1, -T2, +R](val vn: (T1, T2) => Node[R])
      extends AsyncFunction2[T1, T2, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2): R = vn(v1, v2).get
    @impure
    def apply$queued(v1: T1, v2: T2): Node[R] = vn(v1, v2).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction2Impl[_, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                 => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction3Impl[-T1, -T2, -T3, +R](val vn: (T1, T2, T3) => Node[R])
      extends AsyncFunction3[T1, T2, T3, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3): R = vn(v1, v2, v3).get
    @impure
    def apply$queued(v1: T1, v2: T2, v3: T3): Node[R] = vn(v1, v2, v3).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction3Impl[_, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                    => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction4Impl[-T1, -T2, -T3, -T4, +R](
      val vn: (T1, T2, T3, T4) => Node[R])
      extends AsyncFunction4[T1, T2, T3, T4, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4): R = vn(v1, v2, v3, v4).get
    @impure
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4): Node[R] = vn(v1, v2, v3, v4).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction4Impl[_, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                       => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction5Impl[-T1, -T2, -T3, -T4, -T5, +R](
      val vn: (T1, T2, T3, T4, T5) => Node[R])
      extends AsyncFunction5[T1, T2, T3, T4, T5, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): R = vn(v1, v2, v3, v4, v5).get
    @impure
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): Node[R] = vn(v1, v2, v3, v4, v5).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction5Impl[_, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                          => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction6Impl[-T1, -T2, -T3, -T4, -T5, -T6, +R](
      val vn: (T1, T2, T3, T4, T5, T6) => Node[R])
      extends AsyncFunction6[T1, T2, T3, T4, T5, T6, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): R = vn(v1, v2, v3, v4, v5, v6).get
    @impure
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): Node[R] = vn(v1, v2, v3, v4, v5, v6).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction6Impl[_, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                             => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction7Impl[-T1, -T2, -T3, -T4, -T5, -T6, -T7, +R](
      val vn: (T1, T2, T3, T4, T5, T6, T7) => Node[R])
      extends AsyncFunction7[T1, T2, T3, T4, T5, T6, T7, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): R = vn(v1, v2, v3, v4, v5, v6, v7).get
    @impure
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): Node[R] =
      vn(v1, v2, v3, v4, v5, v6, v7).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction7Impl[_, _, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                                => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction8Impl[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, +R](
      val vn: (T1, T2, T3, T4, T5, T6, T7, T8) => Node[R])
      extends AsyncFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): R =
      vn(v1, v2, v3, v4, v5, v6, v7, v8).get
    @impure
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): Node[R] =
      vn(v1, v2, v3, v4, v5, v6, v7, v8).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction8Impl[_, _, _, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                                   => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class AsyncFunction9Impl[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, -T9, +R](
      val vn: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => Node[R])
      extends AsyncFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] {
    @impure
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): R =
      vn(v1, v2, v3, v4, v5, v6, v7, v8, v9).get
    @impure
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): Node[R] =
      vn(v1, v2, v3, v4, v5, v6, v7, v8, v9).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: AsyncFunction9Impl[_, _, _, _, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                                      => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }
}

object asNodeInnards {

  @nodeSyncLift
  @nodeLiftByName
  implicit def liftAndEnqueue[R](@nodeLift @nodeLiftByName r: => R): Node[R] = optimus.core.needsPlugin
  implicit def liftAndEnqueue$withNode[R](r: Node[R]): Node[R] = r.enqueue

  // used for both by-name and nilary cases to save typing
  private[optimus /*platform*/ ] sealed abstract class NodeFunction0ImplBase[+R](private val equalikey: AnyRef)
      extends NodeFunction0NN[R] {
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction0ImplBase[_] =>
        NodeClsIDSupport.equals(this.equalikey, that.equalikey)
      case _ => false
    }
    @transient private[this] var cachedHashCode: Int = 0
    // noinspection HashCodeUsesVar
    override def hashCode(): Int = {
      if (cachedHashCode == 0) {
        cachedHashCode = NodeClsIDSupport.hashCode(equalikey)
        if (cachedHashCode == 0) cachedHashCode = -1
      }
      cachedHashCode
    }
  }

  private[optimus /*platform*/ ] final class NodeFunctionByNameImpl[+R](val template: Node[R])
      extends NodeFunction0ImplBase[R](template) {
    @nodeSync
    def apply(): R = apply$queued().get
    def apply$queued(): Node[R] = apply$newNode().enqueue
    @nowarn("msg=10500 optimus.graph.UnsafeInternal.clone")
    override def apply$newNode(): Node[R] = template match {
      case acn: AlreadyCompletedNode[R] => acn
      case _                            => UnsafeInternal.clone(template)
    }
  }

  private[optimus /*platform*/ ] final class NodeFunction0Impl[+R](val vn: () => Node[R])
      extends NodeFunction0ImplBase[R](vn) {
    @nodeSync
    def apply(): R = vn().get
    def apply$queued(): Node[R] = vn().enqueue
    def apply$newNode(): Node[R] = vn()
  }

  private[optimus /*platform*/ ] final class NodeFunction1Impl[-T1, +R](val vn: T1 => Node[R])
      extends NodeFunction1[T1, R]
      with HasFastNodeGenerator[T1, R] {
    @nodeSync
    def apply(v1: T1): R = vn(v1).get
    def apply$queued(v1: T1): Node[R] = vn(v1).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction1Impl[_, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                             => false
    }

    override private[optimus] def nodeGenerator: AnyRef = vn
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  /**
   * Freezes the scenario stack arguments (i.e. cacheID and inputs params) <br> Notes:
   *   1. Cancellation Scope is explicitly re-queries to avoid an issue where some mapping is produced and cached in a
   *      CS that was cancelled later, the view is considered to be re-usable!
   *   1. Dependency mask is poisoned. This could result in a huge under-reuse under XSFT Experimental to reduce memory
   *      usage
   */
  private[optimus /*platform*/ ] final class NodeFunction1ImplSS[-T1, +R](val vn: T1 => Node[R])
      extends NodeFunction1[T1, R]
      with HasFastNodeGenerator[T1, R] {
    private val ss = EvaluationContext.poisonTweakDependencyMask().scenarioStack()
    private def node(v: T1): Node[R] = {
      val node = vn(v)
      node.attach(ss.withCancellationScopeRaw(EvaluationContext.cancelScope))
      node
    }

    @nodeSync
    def apply(v1: T1): R = node(v1).get
    def apply$queued(v1: T1): Node[R] = node(v1).enqueueAttached
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction1ImplSS[_, _] => NodeClsIDSupport.equals(vn, that.vn) && (ss._cacheID eq that.ss._cacheID)
      case _                               => false
    }

    override private[optimus] def nodeGenerator: AnyRef = vn
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction2Impl[-T1, -T2, +R](val vn: (T1, T2) => Node[R])
      extends NodeFunction2[T1, T2, R] {
    @nodeSync
    def apply(v1: T1, v2: T2): R = vn(v1, v2).get
    def apply$queued(v1: T1, v2: T2): Node[R] = vn(v1, v2).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction2Impl[_, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction3Impl[-T1, -T2, -T3, +R](val vn: (T1, T2, T3) => Node[R])
      extends NodeFunction3[T1, T2, T3, R] {
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3): R = vn(v1, v2, v3).get
    def apply$queued(v1: T1, v2: T2, v3: T3): Node[R] = vn(v1, v2, v3).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction3Impl[_, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                   => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction4Impl[-T1, -T2, -T3, -T4, +R](
      val vn: (T1, T2, T3, T4) => Node[R])
      extends NodeFunction4[T1, T2, T3, T4, R] {
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4): R = vn(v1, v2, v3, v4).get
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4): Node[R] = vn(v1, v2, v3, v4).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction4Impl[_, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                      => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction5Impl[-T1, -T2, -T3, -T4, -T5, +R](
      val vn: (T1, T2, T3, T4, T5) => Node[R])
      extends NodeFunction5[T1, T2, T3, T4, T5, R] {
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): R = vn(v1, v2, v3, v4, v5).get
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5): Node[R] = vn(v1, v2, v3, v4, v5).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction5Impl[_, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                         => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction6Impl[-T1, -T2, -T3, -T4, -T5, -T6, +R](
      val vn: (T1, T2, T3, T4, T5, T6) => Node[R])
      extends NodeFunction6[T1, T2, T3, T4, T5, T6, R] {
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): R = vn(v1, v2, v3, v4, v5, v6).get
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6): Node[R] = vn(v1, v2, v3, v4, v5, v6).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction6Impl[_, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                            => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction7Impl[-T1, -T2, -T3, -T4, -T5, -T6, -T7, +R](
      val vn: (T1, T2, T3, T4, T5, T6, T7) => Node[R])
      extends NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R] {
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): R = vn(v1, v2, v3, v4, v5, v6, v7).get
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7): Node[R] =
      vn(v1, v2, v3, v4, v5, v6, v7).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction7Impl[_, _, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                               => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction8Impl[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, +R](
      val vn: (T1, T2, T3, T4, T5, T6, T7, T8) => Node[R])
      extends NodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] {
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): R =
      vn(v1, v2, v3, v4, v5, v6, v7, v8).get
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8): Node[R] =
      vn(v1, v2, v3, v4, v5, v6, v7, v8).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction8Impl[_, _, _, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                                  => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }

  private[optimus /*platform*/ ] final class NodeFunction9Impl[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, -T9, +R](
      val vn: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => Node[R])
      extends NodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] {
    @nodeSync
    def apply(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): R =
      vn(v1, v2, v3, v4, v5, v6, v7, v8, v9).get
    def apply$queued(v1: T1, v2: T2, v3: T3, v4: T4, v5: T5, v6: T6, v7: T7, v8: T8, v9: T9): Node[R] =
      vn(v1, v2, v3, v4, v5, v6, v7, v8, v9).enqueue
    override def equals(o: Any): Boolean = o match {
      case that: NodeFunction9Impl[_, _, _, _, _, _, _, _, _, _] => NodeClsIDSupport.equals(vn, that.vn)
      case _                                                     => false
    }
    override def hashCode(): Int = NodeClsIDSupport.hashCode(vn)
  }
}

sealed trait asNodeInnards {
  import asNodeInnards._
  def applyNImpl(
      c: scala.reflect.macros.blackbox.Context
  )(f: c.Tree): c.Tree = {
    import c.universe._
    c.macroApplication match {
      case Apply(TypeApply(Select(qual /*asNode|asAsync*/, /*applyN*/ _), _), _) =>
        q"$qual.apply($f)"
    }
  }

  // noinspection ScalaUnusedSymbol (used in the matching withNode)
  // Lift a by-name argument to a NodeFunction0, which will need to be deref'd manually with ().
  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply0[R](@nodeLift @nodeLiftByName @withNodeClassID @captureByValue f: => R): NodeFunction0NN[R] =
    (f _).asInstanceOf[NodeFunction0NN[R]]

  @captureByValue
  def apply0$withNode[R](@captureByValue templ: Node[R]): NodeFunction0NN[R] = new NodeFunctionByNameImpl[R](templ)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[R](@nodeLift @withNodeClassID @captureByValue f: () => R): NodeFunction0NN[R] =
    f.asInstanceOf[NodeFunction0NN[R]]
  @captureByValue
  def apply$withNode[R](@captureByValue vn: () => Node[R]): NodeFunction0NN[R] = new NodeFunction0Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, R](@nodeLift @withNodeClassID @captureByValue f: T1 => R): NodeFunction1[T1, R] =
    f.asInstanceOf[NodeFunction1[T1, R]]
  @captureByValue
  def apply$withNode[T1, R](@captureByValue vn: T1 => Node[R]): NodeFunction1[T1, R] = new NodeFunction1Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, R](@nodeLift @withNodeClassID @captureByValue f: (T1, T2) => R): NodeFunction2[T1, T2, R] =
    f.asInstanceOf[NodeFunction2[T1, T2, R]]
  @captureByValue
  def apply$withNode[T1, T2, R](@captureByValue vn: (T1, T2) => Node[R]): NodeFunction2[T1, T2, R] =
    new NodeFunction2Impl(vn)
  def apply2[T1, T2, R](f: Function2[T1, T2, R]): NodeFunction2[T1, T2, R] =
    macro asNode.applyNImpl

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3) => R): NodeFunction3[T1, T2, T3, R] =
    f.asInstanceOf[NodeFunction3[T1, T2, T3, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, R](@captureByValue vn: (T1, T2, T3) => Node[R]): NodeFunction3[T1, T2, T3, R] =
    new NodeFunction3Impl(vn)
  def apply3[T1, T2, T3, R](f: Function3[T1, T2, T3, R]): NodeFunction3[T1, T2, T3, R] =
    macro asNode.applyNImpl

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4) => R): NodeFunction4[T1, T2, T3, T4, R] =
    f.asInstanceOf[NodeFunction4[T1, T2, T3, T4, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, R](
      @captureByValue vn: (T1, T2, T3, T4) => Node[R]): NodeFunction4[T1, T2, T3, T4, R] =
    new NodeFunction4Impl(vn)
  def apply4[T1, T2, T3, T4, R](f: Function4[T1, T2, T3, T4, R]): NodeFunction4[T1, T2, T3, T4, R] =
    macro asNode.applyNImpl

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5) => R): NodeFunction5[T1, T2, T3, T4, T5, R] =
    f.asInstanceOf[NodeFunction5[T1, T2, T3, T4, T5, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, R](
      @captureByValue vn: (T1, T2, T3, T4, T5) => Node[R]): NodeFunction5[T1, T2, T3, T4, T5, R] =
    new NodeFunction5Impl(vn)
  def apply5[T1, T2, T3, T4, T5, R](f: Function5[T1, T2, T3, T4, T5, R]): NodeFunction5[T1, T2, T3, T4, T5, R] =
    macro asNode.applyNImpl

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, R](@nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6) => R)
      : NodeFunction6[T1, T2, T3, T4, T5, T6, R] =
    f.asInstanceOf[NodeFunction6[T1, T2, T3, T4, T5, T6, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, R](
      @captureByValue vn: (T1, T2, T3, T4, T5, T6) => Node[R]): NodeFunction6[T1, T2, T3, T4, T5, T6, R] =
    new NodeFunction6Impl(vn)
  def apply6[T1, T2, T3, T4, T5, T6, R](
      f: Function6[T1, T2, T3, T4, T5, T6, R]): NodeFunction6[T1, T2, T3, T4, T5, T6, R] =
    macro asNode.applyNImpl

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, T7, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6, T7) => R)
      : NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R] =
    f.asInstanceOf[NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, T7, R](
      @captureByValue vn: (T1, T2, T3, T4, T5, T6, T7) => Node[R]): NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R] =
    new NodeFunction7Impl(vn)
  def apply7[T1, T2, T3, T4, T5, T6, T7, R](
      f: Function7[T1, T2, T3, T4, T5, T6, T7, R]): NodeFunction7[T1, T2, T3, T4, T5, T6, T7, R] =
    macro asNode.applyNImpl

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, T7, T8, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6, T7, T8) => R)
      : NodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] =
    f.asInstanceOf[NodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, T7, T8, R](@captureByValue vn: (T1, T2, T3, T4, T5, T6, T7, T8) => Node[R])
      : NodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] =
    new NodeFunction8Impl(vn)
  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R)
      : NodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] =
    f.asInstanceOf[NodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      @captureByValue vn: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => Node[R])
      : NodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] =
    new NodeFunction9Impl(vn)

}

sealed trait asAsyncInnards {
  import asAsyncInnards._

  // noinspection ScalaUnusedSymbol (used in the matching withNode)
  // Lift a by-name argument to a AsyncFunction0, which will need to be deref'd manually with ().
  // Not generally useful outside of macros or plugin
  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply0[R](@nodeLift @nodeLiftByName @captureByValue f: => R): AsyncFunction0[R] =
    (f _).asInstanceOf[AsyncFunction0[R]]
  @captureByValue
  @nowarn("msg=10500 optimus.graph.UnsafeInternal.clone")
  def apply0$withNode[R](@captureByValue vn: Node[R]): AsyncFunction0[R] = vn match {
    case acn: AlreadyCompletedNode[R] => new AsyncFunction0Impl(() => acn)
    case _                            => new AsyncFunction0Impl(() => UnsafeInternal.clone(vn))
  }

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[R](@nodeLift @withNodeClassID @captureByValue f: () => R): AsyncFunction0[R] =
    f.asInstanceOf[AsyncFunction0[R]]
  @captureByValue
  def apply$withNode[R](@captureByValue vn: () => Node[R]): AsyncFunction0[R] = new AsyncFunction0Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, R](@nodeLift @withNodeClassID @captureByValue f: T1 => R): AsyncFunction1[T1, R] =
    f.asInstanceOf[AsyncFunction1[T1, R]]
  @captureByValue
  def apply$withNode[T1, R](@captureByValue vn: T1 => Node[R]): AsyncFunction1[T1, R] = new AsyncFunction1Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, R](@nodeLift @withNodeClassID @captureByValue f: (T1, T2) => R): AsyncFunction2[T1, T2, R] =
    f.asInstanceOf[AsyncFunction2[T1, T2, R]]
  @captureByValue
  def apply$withNode[T1, T2, R](@captureByValue vn: (T1, T2) => Node[R]): AsyncFunction2[T1, T2, R] =
    new AsyncFunction2Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3) => R): AsyncFunction3[T1, T2, T3, R] =
    f.asInstanceOf[AsyncFunction3[T1, T2, T3, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, R](@captureByValue vn: (T1, T2, T3) => Node[R]): AsyncFunction3[T1, T2, T3, R] =
    new AsyncFunction3Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4) => R): AsyncFunction4[T1, T2, T3, T4, R] =
    f.asInstanceOf[AsyncFunction4[T1, T2, T3, T4, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, R](
      @captureByValue vn: (T1, T2, T3, T4) => Node[R]): AsyncFunction4[T1, T2, T3, T4, R] =
    new AsyncFunction4Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5) => R): AsyncFunction5[T1, T2, T3, T4, T5, R] =
    f.asInstanceOf[AsyncFunction5[T1, T2, T3, T4, T5, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, R](
      @captureByValue vn: (T1, T2, T3, T4, T5) => Node[R]): AsyncFunction5[T1, T2, T3, T4, T5, R] =
    new AsyncFunction5Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, R](@nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6) => R)
      : AsyncFunction6[T1, T2, T3, T4, T5, T6, R] =
    f.asInstanceOf[AsyncFunction6[T1, T2, T3, T4, T5, T6, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, R](
      @captureByValue vn: (T1, T2, T3, T4, T5, T6) => Node[R]): AsyncFunction6[T1, T2, T3, T4, T5, T6, R] =
    new AsyncFunction6Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, T7, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6, T7) => R)
      : AsyncFunction7[T1, T2, T3, T4, T5, T6, T7, R] =
    f.asInstanceOf[AsyncFunction7[T1, T2, T3, T4, T5, T6, T7, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, T7, R](
      @captureByValue vn: (T1, T2, T3, T4, T5, T6, T7) => Node[R]): AsyncFunction7[T1, T2, T3, T4, T5, T6, T7, R] =
    new AsyncFunction7Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, T7, T8, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6, T7, T8) => R)
      : AsyncFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] =
    f.asInstanceOf[AsyncFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, T7, T8, R](@captureByValue vn: (T1, T2, T3, T4, T5, T6, T7, T8) => Node[R])
      : AsyncFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R] =
    new AsyncFunction8Impl(vn)

  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      @nodeLift @withNodeClassID @captureByValue f: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R)
      : AsyncFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] =
    f.asInstanceOf[AsyncFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]]
  @captureByValue
  def apply$withNode[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      @captureByValue vn: (T1, T2, T3, T4, T5, T6, T7, T8, T9) => Node[R])
      : AsyncFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R] =
    new AsyncFunction9Impl(vn)

}

object asNode extends asNodeInnards {

  /** Experimental to reduce memory / scenarioStack allocations */
  def withCurrentScenarioStack[T1, R](vn: T1 => Node[R]): NodeFunction1[T1, R] = new NodeFunction1ImplSS(vn)

}
object asAsync extends asAsyncInnards

object asyncLazyWithAnyRuntimeEnv {

  @async implicit def derefLazy[R](l: Lazy[R]): R = l.deref
  def derefLazy$queued[R](l: Lazy[R]): Node[R] = l.deref$queued

  final class Lazy[R] private[asyncLazyWithAnyRuntimeEnv] (v: Node[R]) {
    @nodeSync
    def apply(): R = {
      ensureAttached
      v.get
    }
    def apply$queued(): Node[R] = {
      val ec = ensureAttached
      if (!v.isDone)
        ec.enqueueDirect(v)
      v
    }
    @nodeSync def deref: R = apply()
    def deref$queued: Node[R] = apply$queued()

    private def ensureAttached: OGSchedulerContext = {
      val ec = OGSchedulerContext.current()
      v.synchronized {
        // allowIllegalOverrideOfInitialRuntimeEnvironment HACK!!!!!
        if (v.scenarioStack eq null) v.attach(ec.scenarioStack.initialRuntimeScenarioStack(initialTime = null))
      }
      ec
    }

  }
  @nodeSyncLift
  @nodeLiftByName @captureByValue
  def apply[R](@nodeLiftByName @nodeLift @captureByValue v: => R): Lazy[R] = new Lazy[R](toNode(v _))
  @captureByValue
  def apply$withNode[R](@captureByValue v: Node[R]) = new Lazy[R](v)
}
