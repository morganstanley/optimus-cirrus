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

import scala.collection.{GenTraversableOnce, IterableLike}
import scala.collection.mutable.{Builder => MBuilder}
import scala.collection.generic.{CanBuildFrom, GenericCompanion}

/**
 * An alternative implementation of Set, which is covariant in the element type.
 *
 * @note
 *   Contravariant methods such as contains are provided through the implicit CovariantSetContra class.
 *
 * @tparam A
 *   The type of elements within this set
 */
trait CovariantSet[+A] extends Iterable[A] with CovariantSetLike[A, CovariantSet[A]]

object CovariantSet extends GenericCompanion[CovariantSet] {
  override def empty[A]: CovariantSet[A] = EmptySet
  override def newBuilder[A]: MBuilder[A, CovariantSet[A]] = CovariantSetLike.newBuilder(empty)

  /**
   * Provides contravariant methods which otherwise would not be possible on a covariant set
   *
   * @note
   *   This technique allows us to provide type-safe functions which would otherwise be contravariant in the element
   *   type, such as contains(x).
   */
  implicit class CovariantSetContra[-A](s: CovariantSet[A]) extends (A => Boolean) {
    def contains(x: A): Boolean = s.hasElem(x)
    def apply(x: A): Boolean = contains(x)
  }

  private abstract class AbstractSet[A]
      extends CovariantSet[A]
      with CovariantSetLike[A, CovariantSet[A]]
      with Serializable {
    override def empty: CovariantSet[A] = EmptySet
    override def companion = CovariantSet
  }

  private class EmptySet extends AbstractSet[Nothing] {
    def hasElem(elem: Any) = false
    def iterator: Iterator[Nothing] = Iterator.empty
    override def size = 0

    def -[B >: Nothing](elem: B): CovariantSet[Nothing] = this
    def +[B >: Nothing](elem: B): CovariantSet[B] = new Set1(elem)
  }

  private class Set1[A](elem1: A) extends AbstractSet[A] {
    def hasElem(elem: Any) = elem == elem1
    def iterator: Iterator[A] = Iterator(elem1)
    override def size = 1
    def -[B >: A](elem: B): CovariantSet[A] = if (elem == elem1) empty else this
    def +[B >: A](elem: B): CovariantSet[B] = if (elem == elem1) this else new Set2(elem1, elem)
  }

  private class Set2[A](elem1: A, elem2: A) extends AbstractSet[A] {
    def hasElem(elem: Any) = elem == elem1 || elem == elem2
    def iterator: Iterator[A] = Iterator(elem1, elem2)
    override def size = 2
    def -[B >: A](elem: B): CovariantSet[A] = {
      if (elem == elem1)
        new Set1(elem2)
      else if (elem == elem2)
        new Set1(elem1)
      else
        this
    }
    def +[B >: A](elem: B): CovariantSet[B] = new SetN[B](this.asCovariantSet[Any].toSet + elem)
  }

  private class SetN[A](underlying: Set[Any]) extends AbstractSet[A] {
    def hasElem(elem: Any) = underlying.contains(elem)
    def iterator: Iterator[A] = underlying.iterator map (_.asInstanceOf[A]) // This is the price we pay for covariance
    override def size = underlying.size
    def -[B >: A](elem: B): CovariantSet[A] = new SetN[A](underlying - elem)
    def +[B >: A](elem: B): CovariantSet[B] = new SetN[B](underlying + elem)
  }

  private object EmptySet extends EmptySet

  class CovariantSetCanBuildFrom[A] extends CanBuildFrom[Coll, A, CovariantSet[A]] {
    def apply(from: Coll) = CovariantSet.newBuilder[A]
    def apply() = CovariantSet.newBuilder[A]
  }

  implicit def cbf[A]: CanBuildFrom[Coll, A, CovariantSet[A]] = new CovariantSetCanBuildFrom[A]
}

/** A template trait for covariant sets */
trait CovariantSetLike[+A, +This <: CovariantSet[A] with CovariantSetLike[A, This]]
    extends IterableLike[A, This]
    with Equals {
  self: This =>

  /** The empty set of the same type as this set */
  def empty: This

  /** Creates a new set with an additional element, unless the element is already present */
  def +[B >: A](elem: B): CovariantSet[B]

  /** Creates a new set with a given element removed from this set */
  def -[B >: A](elem: B): This

  /** Tests whether the set contains a given element - usually called through CovariantSetContra.contains() */
  protected[optimus /*platform*/ ] def hasElem(
      elem: Any): Boolean // TODO: Is there a better way to do this which doesn't require iterating over the entire set?

  /** Creates a new set by adding all the elements contained in another collection to this set */
  def ++[B >: A](xs: GenTraversableOnce[B]): CovariantSet[B] = xs.foldLeft(this.asCovariantSet[B])(_ + _)

  /** Creates a new set from this set with some elements removed */
  def -[B >: A](elem1: B, elem2: B, elems: B*): This = this - elem1 - elem2 -- elems

  /** Creates a new set from this set by removing all elements of another collection */
  def --[B >: A](xs: GenTraversableOnce[B]): This = xs.foldLeft(self)(_ - _)

  /** Tests whether this set is a subset of another set */
  def subsetOf[B >: A](that: CovariantSet[B]): Boolean = this forall that

  /** Computes the intersection between this set and another set */
  def intersect[B >: A](that: CovariantSet[B]): This = this filter that

  /** Computes the intersection between this set and another set */
  def &[B >: A](that: CovariantSet[B]): This = this intersect that

  /** Computes the union of this set and another set */
  def union[B >: A](that: CovariantSet[B]): CovariantSet[B] = this.asCovariantSet[B] ++ that

  /** Computes the union of this set and another set */
  def |[B >: A](that: CovariantSet[B]): CovariantSet[B] = this union that

  /** Computes the difference between this set and another set */
  def diff[B >: A](that: CovariantSet[B]): This = this -- that

  /** Computes the difference between this set and another set */
  def &~[B >: A](that: CovariantSet[B]): This = this diff that

  def asCovariantSet[B >: A]: CovariantSet[B] = this

  //  TODO (OPTIMUS-20898): Covariant.toSeq should not return Stream which is the default impl
  override def toSeq: scala.collection.Seq[A] = toList

  override def equals(that: Any): Boolean = that match {
    case that: CovariantSet[_] =>
      (this eq that) ||
      (that canEqual this) &&
      (this.size == that.size) &&
      (try that subsetOf this
      catch { case ex: ClassCastException => false })
    case _ => false
  }
  private final val setSeed = "CovariantSet".hashCode
  override def hashCode() = scala.util.hashing.MurmurHash3.unorderedHash(seq, setSeed)

  override def stringPrefix: String = "CovariantSet"
  override def toString = super[IterableLike].toString

  override protected[this] def newBuilder: MBuilder[A, This] = CovariantSetLike.newBuilder(empty)
}

object CovariantSetLike {
  private[optimus] def newBuilder[A, Coll <: CovariantSet[A] with CovariantSetLike[A, Coll]](
      empty: Coll): MBuilder[A, Coll] = new CovariantSetBuilder[A, Coll](empty)

  private class CovariantSetBuilder[A, Coll <: CovariantSet[A] with CovariantSetLike[A, Coll]](empty: Coll)
      extends MBuilder[A, Coll] {
    protected var elems: Coll = empty
    def +=(elem: A): this.type = {
      elems =
        (elems + elem).asInstanceOf[Coll] // This cast is a known problem - e.g. see Scala's MapBuilder implementation
      this
    }
    def clear(): Unit = { elems = empty }
    def result(): Coll = elems
  }
}
