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
package optimus.platform.relational.asm

import optimus.graph.AlreadyCompletedNode
import optimus.graph.Node
import optimus.platform.Lambda1
import optimus.platform.Lambda2

@FunctionalInterface
trait Func0[+R] {
  def apply(): R
}

@FunctionalInterface
trait Func1[-T, +R] {
  def apply(t: T): R
}

@FunctionalInterface
trait Func2[-T1, -T2, +R] {
  def apply(t1: T1, t2: T2): R
}

@FunctionalInterface
trait Func3[-T1, -T2, -T3, +R] {
  def apply(t1: T1, t2: T2, t3: T3): R
}

@FunctionalInterface
trait Func4[-T1, -T2, -T3, -T4, +R] {
  def apply(t1: T1, t2: T2, t3: T3, t4: T4): R
}

@FunctionalInterface
trait Func5[-T1, -T2, -T3, -T4, -T5, +R] {
  def apply(t1: T1, t2: T2, t3: T3, t4: T4, t5: T5): R
}

@FunctionalInterface
trait Func6[-T1, -T2, -T3, -T4, -T5, -T6, +R] {
  def apply(t1: T1, t2: T2, t3: T3, t4: T4, t5: T5, t6: T6): R
}

@FunctionalInterface
trait Func7[-T1, -T2, -T3, -T4, -T5, -T6, -T7, +R] {
  def apply(t1: T1, t2: T2, t3: T3, t4: T4, t5: T5, t6: T6, t7: T7): R
}

@FunctionalInterface
trait Func8[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, +R] {
  def apply(t1: T1, t2: T2, t3: T3, t4: T4, t5: T5, t6: T6, t7: T7, t8: T8): R
}

@FunctionalInterface
trait Func9[-T1, -T2, -T3, -T4, -T5, -T6, -T7, -T8, -T9, +R] {
  def apply(t1: T1, t2: T2, t3: T3, t4: T4, t5: T5, t6: T6, t7: T7, t8: T8, t9: T9): R
}

trait LiftedFunction

/**
 * Please do not call the methods here since they are used by the LambdaCompiler to bridge the gaps between ASM and
 * scala toolbox.
 */
object Func {
  import FuncUtils._

  def asFunction[R](f: Func0[R]): () => R = { () =>
    f()
  }

  def wrap[R](f: Func0[Node[R]]): () => Node[R] = {
    new LF0(() => Continuation.wrap(() => f()))
  }

  def asFunction[T, R](f: Func1[T, R]): T => R = { t =>
    f(t)
  }

  def wrap[T, R](f: Func1[T, Node[R]]): T => Node[R] = {
    new LF1(t => Continuation.wrap(() => f(t)))
  }

  def asFunction[T1, T2, R](f: Func2[T1, T2, R]): (T1, T2) => R = { (t1, t2) =>
    f(t1, t2)
  }

  def wrap[T1, T2, R](f: Func2[T1, T2, Node[R]]): (T1, T2) => Node[R] = {
    new LF2((t1, t2) => Continuation.wrap(() => f(t1, t2)))
  }

  def asFunction[T1, T2, T3, R](f: Func3[T1, T2, T3, R]): (T1, T2, T3) => R = { (t1, t2, t3) =>
    f(t1, t2, t3)
  }

  def wrap[T1, T2, T3, R](f: Func3[T1, T2, T3, Node[R]]): (T1, T2, T3) => Node[R] = {
    new LF3((t1, t2, t3) => Continuation.wrap(() => f(t1, t2, t3)))
  }

  def asFunction[T1, T2, T3, T4, R](f: Func4[T1, T2, T3, T4, R]): (T1, T2, T3, T4) => R = { (t1, t2, t3, t4) =>
    f(t1, t2, t3, t4)
  }

  def wrap[T1, T2, T3, T4, R](f: Func4[T1, T2, T3, T4, Node[R]]): (T1, T2, T3, T4) => Node[R] = {
    new LF4((t1, t2, t3, t4) => Continuation.wrap(() => f(t1, t2, t3, t4)))
  }

  def asFunction[T1, T2, T3, T4, T5, R](f: Func5[T1, T2, T3, T4, T5, R]): (T1, T2, T3, T4, T5) => R = {
    (t1, t2, t3, t4, t5) =>
      f(t1, t2, t3, t4, t5)
  }

  def wrap[T1, T2, T3, T4, T5, R](f: Func5[T1, T2, T3, T4, T5, Node[R]]): (T1, T2, T3, T4, T5) => Node[R] = {
    new LF5((t1, t2, t3, t4, t5) => Continuation.wrap(() => f(t1, t2, t3, t4, t5)))
  }

  def asFunction[T1, T2, T3, T4, T5, T6, R](f: Func6[T1, T2, T3, T4, T5, T6, R]): (T1, T2, T3, T4, T5, T6) => R = {
    (t1, t2, t3, t4, t5, t6) =>
      f(t1, t2, t3, t4, t5, t6)
  }

  def wrap[T1, T2, T3, T4, T5, T6, R](
      f: Func6[T1, T2, T3, T4, T5, T6, Node[R]]): (T1, T2, T3, T4, T5, T6) => Node[R] = {
    new LF6((t1, t2, t3, t4, t5, t6) => Continuation.wrap(() => f(t1, t2, t3, t4, t5, t6)))
  }

  def asFunction[T1, T2, T3, T4, T5, T6, T7, R](
      f: Func7[T1, T2, T3, T4, T5, T6, T7, R]): (T1, T2, T3, T4, T5, T6, T7) => R = { (t1, t2, t3, t4, t5, t6, t7) =>
    f(t1, t2, t3, t4, t5, t6, t7)
  }

  def wrap[T1, T2, T3, T4, T5, T6, T7, R](
      f: Func7[T1, T2, T3, T4, T5, T6, T7, Node[R]]): (T1, T2, T3, T4, T5, T6, T7) => Node[R] = {
    new LF7((t1, t2, t3, t4, t5, t6, t7) => Continuation.wrap(() => f(t1, t2, t3, t4, t5, t6, t7)))
  }

  def asFunction[T1, T2, T3, T4, T5, T6, T7, T8, R](
      f: Func8[T1, T2, T3, T4, T5, T6, T7, T8, R]): (T1, T2, T3, T4, T5, T6, T7, T8) => R = {
    (t1, t2, t3, t4, t5, t6, t7, t8) =>
      f(t1, t2, t3, t4, t5, t6, t7, t8)
  }

  def wrap[T1, T2, T3, T4, T5, T6, T7, T8, R](
      f: Func8[T1, T2, T3, T4, T5, T6, T7, T8, Node[R]]): (T1, T2, T3, T4, T5, T6, T7, T8) => Node[R] = {
    new LF8((t1, t2, t3, t4, t5, t6, t7, t8) => Continuation.wrap(() => f(t1, t2, t3, t4, t5, t6, t7, t8)))
  }

  def asFunction[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      f: Func9[T1, T2, T3, T4, T5, T6, T7, T8, T9, R]): (T1, T2, T3, T4, T5, T6, T7, T8, T9) => R = {
    (t1, t2, t3, t4, t5, t6, t7, t8, t9) =>
      f(t1, t2, t3, t4, t5, t6, t7, t8, t9)
  }

  def wrap[T1, T2, T3, T4, T5, T6, T7, T8, T9, R](
      f: Func9[T1, T2, T3, T4, T5, T6, T7, T8, T9, Node[R]]): (T1, T2, T3, T4, T5, T6, T7, T8, T9) => Node[R] = {
    new LF9((t1, t2, t3, t4, t5, t6, t7, t8, t9) => Continuation.wrap(() => f(t1, t2, t3, t4, t5, t6, t7, t8, t9)))
  }

  def liftNode[R](f: Function0[_]): () => Node[R] = {
    f match {
      case _: LiftedFunction => f.asInstanceOf[() => Node[R]]
      case _                 => new LF0(() => new AlreadyCompletedNode(f().asInstanceOf[R]))
    }
  }

  def liftNode[T, R](f: Function1[T, _]): T => Node[R] = {
    f match {
      case _: LiftedFunction => f.asInstanceOf[T => Node[R]]
      case _                 => new LF1(t => new AlreadyCompletedNode(f(t).asInstanceOf[R]))
    }
  }

  def liftNode[T1, T2, R](f: Function2[T1, T2, _]): (T1, T2) => Node[R] = {
    f match {
      case _: LiftedFunction => f.asInstanceOf[(T1, T2) => Node[R]]
      case _                 => new LF2((t1, t2) => new AlreadyCompletedNode(f(t1, t2).asInstanceOf[R]))
    }
  }

  def asEither[R](f: Function0[_]): Either[() => Node[R], () => R] = {
    f match {
      case l: LiftedFunction => Left(f.asInstanceOf[() => Node[R]])
      case _                 => Right(f.asInstanceOf[() => R])
    }
  }

  def asEither[T, R](f: Function1[T, _]): Either[T => Node[R], T => R] = {
    f match {
      case l: LiftedFunction => Left(f.asInstanceOf[T => Node[R]])
      case _                 => Right(f.asInstanceOf[T => R])
    }
  }

  def asEither[T1, T2, R](f: Function2[T1, T2, _]): Either[(T1, T2) => Node[R], (T1, T2) => R] = {
    f match {
      case l: LiftedFunction => Left(f.asInstanceOf[(T1, T2) => Node[R]])
      case _                 => Right(f.asInstanceOf[(T1, T2) => R])
    }
  }

  def asLambda1[T, R](f: Function1[T, _]): Lambda1[T, R] = {
    asEither[T, R](f) match {
      case Left(x)  => Lambda1(None, Some(x), None)
      case Right(x) => Lambda1(Some(x), None, None)
    }
  }

  def asLambda2[T1, T2, R](f: Function2[T1, T2, _]): Lambda2[T1, T2, R] = {
    asEither[T1, T2, R](f) match {
      case Left(x)  => Lambda2(None, Some(x), None)
      case Right(x) => Lambda2(Some(x), None, None)
    }
  }
}
