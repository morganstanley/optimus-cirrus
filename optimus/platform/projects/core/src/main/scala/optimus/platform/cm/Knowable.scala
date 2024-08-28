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
package optimus.platform.cm

/**
 * Knowables represent values which at a given point in time may be known, unknown or known to be not applicable, using
 * [[optimus.platform.cm.core.Known]], [[optimus.platform.cm.core.Unknown]] and
 * [[optimus.platform.cm.core.NotApplicable]] respectively.
 *
 * `Knowable` supports most of the functionality of [[scala.Option]]
 *
 * @tparam A
 *   type of the knowable value.
 */
sealed abstract class Knowable[+A] {

  /** Returns false if the value is known, otherwise true */
  def isEmpty: Boolean

  /** Returns true if the value is known, otherwise false */
  def isDefined: Boolean = !isEmpty

  /**
   * Returns the option's value.
   * @note
   *   The knowable must be nonEmpty.
   * @throws Predef.NoSuchElementException
   *   if the knowable is empty.
   */
  def get: A

  @inline final def getOrElse[B >: A](default: => B): B = {
    if (isEmpty) default else this.get
  }

  @inline final def orNull[A1 >: A](implicit ev: Null <:< A1): A1 = this getOrElse ev(null)

  @inline final def orElse[B >: A](alternative: => Knowable[B]): Knowable[B] = {
    if (isEmpty) alternative else this
  }

  @inline final def map[B](f: A => B): Knowable[B] = {
    if (isEmpty) as[B] else Known(f(this.get))
  }

  @inline final def flatMap[B](f: A => Knowable[B]): Knowable[B] = {
    if (isEmpty) as[B] else f(this.get)
  }

  @inline def iterator: Iterator[A] = {
    if (isEmpty) Iterator.empty else Iterator.single(this.get)
  }

  @inline final def toList: List[A] = {
    if (isEmpty) Nil else new ::(this.get, Nil)
  }

  @inline final def toOption: Option[A] = {
    if (isEmpty) None else Some(this.get)
  }

  /**
   * Returns true if the knowable is defined, false otherwise.
   * @note
   *   Implemented here to avoid the implicit conversion to Iterable.
   */
  @inline final def nonEmpty = isDefined

  @inline final def exists(p: A => Boolean): Boolean = !isEmpty && p(this.get)

  @inline final def forall(p: A => Boolean): Boolean = isEmpty || p(this.get)

  @inline final def foreach[U](f: A => U): Unit = {
    if (!isEmpty) f(this.get)
  }

  protected def as[B]: Knowable[B]
}

trait KnowableLowPriority {
  implicit def knowableToIterable[A](k: Knowable[A]): Iterable[A] = k.toList
}

object Knowable extends KnowableLowPriority {
  implicit def toOption[A](k: Knowable[A]): Option[A] = k.toOption
}

/** Represents a known value for a [[optimus.platform.cm.core.Knowable]] */
final case class Known[+T](x: T) extends Knowable[T] {
  def isEmpty = false

  def get = x

  protected def as[B]: Knowable[B] =
    throw new ClassCastException(
      "Invalid attempt to cast Known[T] - should only be applied to Unknown or NotApplicable")
}

/** Common base type for values which are not Known */
sealed abstract class Absent[+T] extends Knowable[T] {
  def isEmpty = true

  def get = throw new NoSuchElementException("Unknown.get")
}

/** Indicates that a value is currently unknown */
final case class Unknown[+T]() extends Absent[T] {
  protected def as[B]: Knowable[B] = Unknown[B]()
}

/** Indicates that the value is not applicable */
case object NotApplicable extends Absent[Nothing] {
  protected def as[B]: Knowable[B] = this
}
