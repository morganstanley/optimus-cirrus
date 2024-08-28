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
package optimus.graph.loom

import optimus.graph.{CompletableNode, Node, OGSchedulerContext}

/**
 * Wrapper to force Scala not to optimize the following expression:
 * {{{
 *   def foo(f: => T) = asAsync{ () => f }
 * }}}
 * Scala would not create a lambda `() => f`, but just reuse `f`
 */
class AsNodeFactoryPlain1[T1, R](val f: Function1[T1, R]) extends (T1 => Node[R]) {
  override def apply(v1: T1): Node[R] = new AsNodePlain[R](f(v1))
}

/* Wrapper for backward compatibility and nice stack element */
class AsNodeFactory1[T1, R](private val l: LNodeFunction1[T1, R]) extends (T1 => Node[R]) with LNodeClsID {
  override def apply(v1: T1): Node[R] = l.apply$newNode(v1)
  override def _clsID(): Int = l._clsID()
  override def stackTraceElem(): StackTraceElement = l.asInstanceOf[LNodeClsID].stackTraceElem
  override def hashCode(): Int = l.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case np: AsNodeFactory1[_, _] => l.equals(np.l)
    case _                        => false
  }
}

/* Wrapper for backward compatibility and nice stack element */
class AsNodeFactory2[T1, T2, R](private val l: LNodeFunction2[T1, T2, R])
    extends ((T1, T2) => Node[R])
    with LNodeClsID {
  override def apply(v1: T1, v2: T2): Node[R] = l.apply$newNode(v1, v2)
  override def _clsID(): Int = l._clsID()
  override def stackTraceElem(): StackTraceElement = l.asInstanceOf[LNodeClsID].stackTraceElem
  override def hashCode(): Int = l.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case np: AsNodeFactory2[_, _, _] => l.equals(np.l)
    case _                           => false
  }
}

class AsNodePlain[R](l: => R) extends CompletableNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l, ec)
}

/** A generic node that wraps a function */
abstract class AsNode[R] extends CompletableNode[R] with LNodeClsID {
  protected def l: LNodeFunction[R]
  override def _clsID: Int = l._clsID()
  override def getProfileId: Int = l.getProfileId
  override def hashCode(): Int = l.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case anode: AsNode[R @unchecked] => l.equals(anode.l)
    case _                           => false
  }
}

class AsNode0[R](override val l: LNodeFunction0[R]) extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(), ec)
}
class AsNode1[T1, R](override val l: LNodeFunction1[T1, R], v1: T1) extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1), ec)
}
class AsNode2[T1, T2, R](override val l: LNodeFunction2[T1, T2, R], v1: T1, v2: T2) extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2), ec)
}
class AsNode3[T1, T2, T3, R](override val l: LNodeFunction3[T1, T2, T3, R], v1: T1, v2: T2, v3: T3) extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2, v3), ec)
}
class AsNode4[T1, T2, T3, T4, R](override val l: LNodeFunction4[T1, T2, T3, T4, R], v1: T1, v2: T2, v3: T3, v4: T4)
    extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2, v3, v4), ec)
}
class AsNode5[T1, T2, T3, T4, T5, R](
    override val l: LNodeFunction5[T1, T2, T3, T4, T5, R],
    v1: T1,
    v2: T2,
    v3: T3,
    v4: T4,
    v5: T5)
    extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2, v3, v4, v5), ec)
}
class AsNode6[T1, T2, T3, T4, T5, T6, R](
    override val l: LNodeFunction6[T1, T2, T3, T4, T5, T6, R],
    v1: T1,
    v2: T2,
    v3: T3,
    v4: T4,
    v5: T5,
    v6: T6)
    extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2, v3, v4, v5, v6), ec)
}
class AsNode7[T1, T2, T3, T4, T5, T6, T7, R](
    override val l: LNodeFunction7[T1, T2, T3, T4, T5, T6, T7, R],
    v1: T1,
    v2: T2,
    v3: T3,
    v4: T4,
    v5: T5,
    v6: T6,
    v7: T7)
    extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2, v3, v4, v5, v6, v7), ec)
}
class AsNode8[T1, T2, T3, T4, T5, T6, T7, T8, R](
    override val l: LNodeFunction8[T1, T2, T3, T4, T5, T6, T7, T8, R],
    v1: T1,
    v2: T2,
    v3: T3,
    v4: T4,
    v5: T5,
    v6: T6,
    v7: T7,
    v8: T8)
    extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2, v3, v4, v5, v6, v7, v8), ec)
}
class AsNode9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
    override val l: LNodeFunction9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R],
    v1: T1,
    v2: T2,
    v3: T3,
    v4: T4,
    v5: T5,
    v6: T6,
    v7: T7,
    v8: T8,
    v9: T9)
    extends AsNode[R] {
  override def run(ec: OGSchedulerContext): Unit = completeWithResult(l(v1, v2, v3, v4, v5, v6, v7, v8, v9), ec)
}
