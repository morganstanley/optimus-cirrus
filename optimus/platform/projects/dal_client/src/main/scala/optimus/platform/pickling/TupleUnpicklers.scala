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
package optimus.platform.pickling

import optimus.platform._

trait TupleUnpicklers {
  def tuple2Unpickler[A: Manifest, B: Manifest](ua: Unpickler[A], ub: Unpickler[B]): Unpickler[(A, B)] =
    new Tuple2Unpickler[A, B](ua, ub)
  def tuple3Unpickler[A: Manifest, B: Manifest, C: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C]): Unpickler[(A, B, C)] =
    new Tuple3Unpickler[A, B, C](ua, ub, uc)
  def tuple4Unpickler[A: Manifest, B: Manifest, C: Manifest, D: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C],
      ud: Unpickler[D]): Unpickler[(A, B, C, D)] =
    new Tuple4Unpickler[A, B, C, D](ua, ub, uc, ud)
  def tuple5Unpickler[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C],
      ud: Unpickler[D],
      ue: Unpickler[E]): Unpickler[(A, B, C, D, E)] =
    new Tuple5Unpickler[A, B, C, D, E](ua, ub, uc, ud, ue)
  def tuple6Unpickler[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest, F: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C],
      ud: Unpickler[D],
      ue: Unpickler[E],
      uf: Unpickler[F]): Unpickler[(A, B, C, D, E, F)] =
    new Tuple6Unpickler[A, B, C, D, E, F](ua, ub, uc, ud, ue, uf)
  def tuple7Unpickler[A: Manifest, B: Manifest, C: Manifest, D: Manifest, E: Manifest, F: Manifest, G: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C],
      ud: Unpickler[D],
      ue: Unpickler[E],
      uf: Unpickler[F],
      ug: Unpickler[G]): Unpickler[(A, B, C, D, E, F, G)] =
    new Tuple7Unpickler[A, B, C, D, E, F, G](ua, ub, uc, ud, ue, uf, ug)
  def tuple8Unpickler[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C],
      ud: Unpickler[D],
      ue: Unpickler[E],
      uf: Unpickler[F],
      ug: Unpickler[G],
      uh: Unpickler[H]): Unpickler[(A, B, C, D, E, F, G, H)] =
    new Tuple8Unpickler[A, B, C, D, E, F, G, H](ua, ub, uc, ud, ue, uf, ug, uh)
  def tuple9Unpickler[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest,
      I: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C],
      ud: Unpickler[D],
      ue: Unpickler[E],
      uf: Unpickler[F],
      ug: Unpickler[G],
      uh: Unpickler[H],
      ui: Unpickler[I]): Unpickler[(A, B, C, D, E, F, G, H, I)] =
    new Tuple9Unpickler[A, B, C, D, E, F, G, H, I](ua, ub, uc, ud, ue, uf, ug, uh, ui)
  def tuple10Unpickler[
      A: Manifest,
      B: Manifest,
      C: Manifest,
      D: Manifest,
      E: Manifest,
      F: Manifest,
      G: Manifest,
      H: Manifest,
      I: Manifest,
      J: Manifest](
      ua: Unpickler[A],
      ub: Unpickler[B],
      uc: Unpickler[C],
      ud: Unpickler[D],
      ue: Unpickler[E],
      uf: Unpickler[F],
      ug: Unpickler[G],
      uh: Unpickler[H],
      ui: Unpickler[I],
      uj: Unpickler[J]): Unpickler[(A, B, C, D, E, F, G, H, I, J)] =
    new Tuple10Unpickler[A, B, C, D, E, F, G, H, I, J](ua, ub, uc, ud, ue, uf, ug, uh, ui, uj)
}

class Tuple2Unpickler[T1: Manifest, T2: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2])
    extends Unpickler[(T1, T2)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2) = pickled match {
    case t: Tuple2[_, _] => {
      (t1Unpickler.unpickle(t._1, ctxt), t2Unpickler.unpickle(t._2, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 2)
        error(pickled)
      else
        (t1Unpickler.unpickle(s(0), ctxt), t2Unpickler.unpickle(s(1), ctxt))
    }
    case _ => error(pickled)
  }

  // NB: Unwind (T1, T2).  Should return nothing and not be a @node (waiting on FSM fixes)
  @node private def error(pickled: Any): (T1, T2) = {
    throw new UnexpectedPickledTypeException(implicitly[Manifest[(T1, T2)]], pickled.getClass)
  }
}

class Tuple3Unpickler[T1: Manifest, T2: Manifest, T3: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3])
    extends Unpickler[(T1, T2, T3)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3) = pickled match {
    case t: Tuple3[_, _, _] => {
      (t1Unpickler.unpickle(t._1, ctxt), t2Unpickler.unpickle(t._2, ctxt), t3Unpickler.unpickle(t._3, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 3)
        error(pickled)
      else
        (t1Unpickler.unpickle(s(0), ctxt), t2Unpickler.unpickle(s(1), ctxt), t3Unpickler.unpickle(s(2), ctxt))
    }
    case _ => error(pickled)
  }

  @node private def error(pickled: Any): (T1, T2, T3) = {
    throw new UnexpectedPickledTypeException(implicitly[Manifest[(T1, T2, T3)]], pickled.getClass)
  }
}

class Tuple4Unpickler[T1: Manifest, T2: Manifest, T3: Manifest, T4: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3],
    private[optimus] val t4Unpickler: Unpickler[T4])
    extends Unpickler[(T1, T2, T3, T4)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3, T4) = pickled match {
    case t: Tuple4[_, _, _, _] => {
      (
        t1Unpickler.unpickle(t._1, ctxt),
        t2Unpickler.unpickle(t._2, ctxt),
        t3Unpickler.unpickle(t._3, ctxt),
        t4Unpickler.unpickle(t._4, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 4)
        error(pickled)
      else
        (
          t1Unpickler.unpickle(s(0), ctxt),
          t2Unpickler.unpickle(s(1), ctxt),
          t3Unpickler.unpickle(s(2), ctxt),
          t4Unpickler.unpickle(s(3), ctxt))
    }
    case _ => error(pickled)
  }

  @node private def error(pickled: Any): (T1, T2, T3, T4) = {
    throw new UnexpectedPickledTypeException(implicitly[Manifest[(T1, T2, T3, T4)]], pickled.getClass)
  }
}

class Tuple5Unpickler[T1: Manifest, T2: Manifest, T3: Manifest, T4: Manifest, T5: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3],
    private[optimus] val t4Unpickler: Unpickler[T4],
    private[optimus] val t5Unpickler: Unpickler[T5])
    extends Unpickler[(T1, T2, T3, T4, T5)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3, T4, T5) = pickled match {
    case t: Tuple5[_, _, _, _, _] => {
      (
        t1Unpickler.unpickle(t._1, ctxt),
        t2Unpickler.unpickle(t._2, ctxt),
        t3Unpickler.unpickle(t._3, ctxt),
        t4Unpickler.unpickle(t._4, ctxt),
        t5Unpickler.unpickle(t._5, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 5)
        error(pickled)
      else
        (
          t1Unpickler.unpickle(s(0), ctxt),
          t2Unpickler.unpickle(s(1), ctxt),
          t3Unpickler.unpickle(s(2), ctxt),
          t4Unpickler.unpickle(s(3), ctxt),
          t5Unpickler.unpickle(s(4), ctxt))
    }
    case _ => error(pickled)
  }

  @node private def error(pickled: Any): (T1, T2, T3, T4, T5) = {
    throw new UnexpectedPickledTypeException(implicitly[Manifest[(T1, T2, T3, T4, T5)]], pickled.getClass)
  }
}

class Tuple6Unpickler[T1: Manifest, T2: Manifest, T3: Manifest, T4: Manifest, T5: Manifest, T6: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3],
    private[optimus] val t4Unpickler: Unpickler[T4],
    private[optimus] val t5Unpickler: Unpickler[T5],
    private[optimus] val t6Unpickler: Unpickler[T6])
    extends Unpickler[(T1, T2, T3, T4, T5, T6)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3, T4, T5, T6) = pickled match {
    case t: Tuple6[_, _, _, _, _, _] => {
      (
        t1Unpickler.unpickle(t._1, ctxt),
        t2Unpickler.unpickle(t._2, ctxt),
        t3Unpickler.unpickle(t._3, ctxt),
        t4Unpickler.unpickle(t._4, ctxt),
        t5Unpickler.unpickle(t._5, ctxt),
        t6Unpickler.unpickle(t._6, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 6)
        error(pickled)
      else
        (
          t1Unpickler.unpickle(s(0), ctxt),
          t2Unpickler.unpickle(s(1), ctxt),
          t3Unpickler.unpickle(s(2), ctxt),
          t4Unpickler.unpickle(s(3), ctxt),
          t5Unpickler.unpickle(s(4), ctxt),
          t6Unpickler.unpickle(s(5), ctxt))
    }
    case _ => error(pickled)
  }

  @node private def error(pickled: Any): (T1, T2, T3, T4, T5, T6) = {
    throw new UnexpectedPickledTypeException(implicitly[Manifest[(T1, T2, T3, T4, T5, T6)]], pickled.getClass)
  }
}

class Tuple7Unpickler[T1: Manifest, T2: Manifest, T3: Manifest, T4: Manifest, T5: Manifest, T6: Manifest, T7: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3],
    private[optimus] val t4Unpickler: Unpickler[T4],
    private[optimus] val t5Unpickler: Unpickler[T5],
    private[optimus] val t6Unpickler: Unpickler[T6],
    private[optimus] val t7Unpickler: Unpickler[T7])
    extends Unpickler[(T1, T2, T3, T4, T5, T6, T7)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3, T4, T5, T6, T7) = pickled match {
    case t: Tuple7[_, _, _, _, _, _, _] => {
      (
        t1Unpickler.unpickle(t._1, ctxt),
        t2Unpickler.unpickle(t._2, ctxt),
        t3Unpickler.unpickle(t._3, ctxt),
        t4Unpickler.unpickle(t._4, ctxt),
        t5Unpickler.unpickle(t._5, ctxt),
        t6Unpickler.unpickle(t._6, ctxt),
        t7Unpickler.unpickle(t._7, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 7)
        error(pickled)
      else
        (
          t1Unpickler.unpickle(s(0), ctxt),
          t2Unpickler.unpickle(s(1), ctxt),
          t3Unpickler.unpickle(s(2), ctxt),
          t4Unpickler.unpickle(s(3), ctxt),
          t5Unpickler.unpickle(s(4), ctxt),
          t6Unpickler.unpickle(s(5), ctxt),
          t7Unpickler.unpickle(s(6), ctxt))
    }
    case _ => error(pickled)
  }

  @node private def error(pickled: Any): (T1, T2, T3, T4, T5, T6, T7) = {
    throw new UnexpectedPickledTypeException(implicitly[Manifest[(T1, T2, T3, T4, T5, T6, T7)]], pickled.getClass)
  }
}

class Tuple8Unpickler[
    T1: Manifest,
    T2: Manifest,
    T3: Manifest,
    T4: Manifest,
    T5: Manifest,
    T6: Manifest,
    T7: Manifest,
    T8: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3],
    private[optimus] val t4Unpickler: Unpickler[T4],
    private[optimus] val t5Unpickler: Unpickler[T5],
    private[optimus] val t6Unpickler: Unpickler[T6],
    private[optimus] val t7Unpickler: Unpickler[T7],
    private[optimus] val t8Unpickler: Unpickler[T8])
    extends Unpickler[(T1, T2, T3, T4, T5, T6, T7, T8)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3, T4, T5, T6, T7, T8) = pickled match {
    case t: Tuple8[_, _, _, _, _, _, _, _] => {
      (
        t1Unpickler.unpickle(t._1, ctxt),
        t2Unpickler.unpickle(t._2, ctxt),
        t3Unpickler.unpickle(t._3, ctxt),
        t4Unpickler.unpickle(t._4, ctxt),
        t5Unpickler.unpickle(t._5, ctxt),
        t6Unpickler.unpickle(t._6, ctxt),
        t7Unpickler.unpickle(t._7, ctxt),
        t8Unpickler.unpickle(t._8, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 8)
        error(pickled)
      else
        (
          t1Unpickler.unpickle(s(0), ctxt),
          t2Unpickler.unpickle(s(1), ctxt),
          t3Unpickler.unpickle(s(2), ctxt),
          t4Unpickler.unpickle(s(3), ctxt),
          t5Unpickler.unpickle(s(4), ctxt),
          t6Unpickler.unpickle(s(5), ctxt),
          t7Unpickler.unpickle(s(6), ctxt),
          t8Unpickler.unpickle(s(7), ctxt))
    }
    case _ => error(pickled)
  }

  @node private def error(pickled: Any): (T1, T2, T3, T4, T5, T6, T7, T8) = {
    throw new UnexpectedPickledTypeException(implicitly[Manifest[(T1, T2, T3, T4, T5, T6, T7, T8)]], pickled.getClass)
  }
}

class Tuple9Unpickler[
    T1: Manifest,
    T2: Manifest,
    T3: Manifest,
    T4: Manifest,
    T5: Manifest,
    T6: Manifest,
    T7: Manifest,
    T8: Manifest,
    T9: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3],
    private[optimus] val t4Unpickler: Unpickler[T4],
    private[optimus] val t5Unpickler: Unpickler[T5],
    private[optimus] val t6Unpickler: Unpickler[T6],
    private[optimus] val t7Unpickler: Unpickler[T7],
    private[optimus] val t8Unpickler: Unpickler[T8],
    private[optimus] val t9Unpickler: Unpickler[T9])
    extends Unpickler[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3, T4, T5, T6, T7, T8, T9) = pickled match {
    case t: Tuple9[_, _, _, _, _, _, _, _, _] => {
      (
        t1Unpickler.unpickle(t._1, ctxt),
        t2Unpickler.unpickle(t._2, ctxt),
        t3Unpickler.unpickle(t._3, ctxt),
        t4Unpickler.unpickle(t._4, ctxt),
        t5Unpickler.unpickle(t._5, ctxt),
        t6Unpickler.unpickle(t._6, ctxt),
        t7Unpickler.unpickle(t._7, ctxt),
        t8Unpickler.unpickle(t._8, ctxt),
        t9Unpickler.unpickle(t._9, ctxt))
    }
    case s: collection.Seq[_] => {
      if (s.size != 9)
        error(pickled)
      else
        (
          t1Unpickler.unpickle(s(0), ctxt),
          t2Unpickler.unpickle(s(1), ctxt),
          t3Unpickler.unpickle(s(2), ctxt),
          t4Unpickler.unpickle(s(3), ctxt),
          t5Unpickler.unpickle(s(4), ctxt),
          t6Unpickler.unpickle(s(5), ctxt),
          t7Unpickler.unpickle(s(6), ctxt),
          t8Unpickler.unpickle(s(7), ctxt),
          t9Unpickler.unpickle(s(8), ctxt))
    }
    case _ => error(pickled)
  }

  @node private def error(pickled: Any): (T1, T2, T3, T4, T5, T6, T7, T8, T9) = {
    throw new UnexpectedPickledTypeException(
      implicitly[Manifest[(T1, T2, T3, T4, T5, T6, T7, T8, T9)]],
      pickled.getClass)
  }
}

class Tuple10Unpickler[
    T1: Manifest,
    T2: Manifest,
    T3: Manifest,
    T4: Manifest,
    T5: Manifest,
    T6: Manifest,
    T7: Manifest,
    T8: Manifest,
    T9: Manifest,
    T10: Manifest](
    private[optimus] val t1Unpickler: Unpickler[T1],
    private[optimus] val t2Unpickler: Unpickler[T2],
    private[optimus] val t3Unpickler: Unpickler[T3],
    private[optimus] val t4Unpickler: Unpickler[T4],
    private[optimus] val t5Unpickler: Unpickler[T5],
    private[optimus] val t6Unpickler: Unpickler[T6],
    private[optimus] val t7Unpickler: Unpickler[T7],
    private[optimus] val t8Unpickler: Unpickler[T8],
    private[optimus] val t9Unpickler: Unpickler[T9],
    private[optimus] val t10Unpickler: Unpickler[T10])
    extends Unpickler[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] {
  @node def unpickle(pickled: Any, ctxt: PickledInputStream): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) =
    pickled match {
      case t: Tuple10[_, _, _, _, _, _, _, _, _, _] => {
        (
          t1Unpickler.unpickle(t._1, ctxt),
          t2Unpickler.unpickle(t._2, ctxt),
          t3Unpickler.unpickle(t._3, ctxt),
          t4Unpickler.unpickle(t._4, ctxt),
          t5Unpickler.unpickle(t._5, ctxt),
          t6Unpickler.unpickle(t._6, ctxt),
          t7Unpickler.unpickle(t._7, ctxt),
          t8Unpickler.unpickle(t._8, ctxt),
          t9Unpickler.unpickle(t._9, ctxt),
          t10Unpickler.unpickle(t._10, ctxt))
      }
      case s: collection.Seq[_] => {
        if (s.size != 10)
          error(pickled)
        else
          (
            t1Unpickler.unpickle(s(0), ctxt),
            t2Unpickler.unpickle(s(1), ctxt),
            t3Unpickler.unpickle(s(2), ctxt),
            t4Unpickler.unpickle(s(3), ctxt),
            t5Unpickler.unpickle(s(4), ctxt),
            t6Unpickler.unpickle(s(5), ctxt),
            t7Unpickler.unpickle(s(6), ctxt),
            t8Unpickler.unpickle(s(7), ctxt),
            t9Unpickler.unpickle(s(8), ctxt),
            t10Unpickler.unpickle(s(9), ctxt))
      }
      case _ => error(pickled)
    }

  @node private def error(pickled: Any): (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10) = {
    throw new UnexpectedPickledTypeException(
      implicitly[Manifest[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)]],
      pickled.getClass)
  }
}
