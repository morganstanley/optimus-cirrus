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

import java.util.Arrays

import scala.collection.immutable
import scala.collection.mutable
import scala.collection.generic.CanBuildFrom
import scala.collection.generic.GenericTraversableTemplate
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3
import scala.collection.compat.Factory

class ImmutableArray[A] private (private val as: Array[A])
    extends immutable.IndexedSeq[A]
    with GenericTraversableTemplate[A, collection.IndexedSeq]
    with Serializable {
  override def apply(idx: Int): A = as.apply(idx)
  override def length: Int = as.length

  override def equals(o: Any): Boolean = o match {
    case that: ImmutableArray[_] if this.as.getClass == that.as.getClass =>
      (as: Array[_], that.as) match {
        case (as: Array[AnyRef], bs: Array[AnyRef])   => Arrays.equals(as, bs)
        case (as: Array[Int], bs: Array[Int])         => Arrays.equals(as, bs)
        case (as: Array[Double], bs: Array[Double])   => Arrays.equals(as, bs)
        case (as: Array[Long], bs: Array[Long])       => Arrays.equals(as, bs)
        case (as: Array[Float], bs: Array[Float])     => Arrays.equals(as, bs)
        case (as: Array[Char], bs: Array[Char])       => Arrays.equals(as, bs)
        case (as: Array[Byte], bs: Array[Byte])       => Arrays.equals(as, bs)
        case (as: Array[Short], bs: Array[Short])     => Arrays.equals(as, bs)
        case (as: Array[Boolean], bs: Array[Boolean]) => Arrays.equals(as, bs)
        case _                                        => false
      }
    case _ => false
  }
  override lazy val hashCode = {
    (as: Array[_]) match {
      case as: Array[AnyRef]  => Arrays.hashCode(as)
      case as: Array[Int]     => Arrays.hashCode(as)
      case as: Array[Double]  => Arrays.hashCode(as)
      case as: Array[Long]    => Arrays.hashCode(as)
      case as: Array[Float]   => Arrays.hashCode(as)
      case as: Array[Char]    => Arrays.hashCode(as)
      case as: Array[Byte]    => Arrays.hashCode(as)
      case as: Array[Short]   => Arrays.hashCode(as)
      case as: Array[Boolean] => Arrays.hashCode(as)
      case _                  => MurmurHash3.arrayHash(as)
    }
  }

  private[optimus] def rawArray = as
  def copyUnderlying: Array[Byte] = {
    val out = new Array[Byte](as.length)
    Array.copy(rawArray, 0, out, 0, rawArray.length)
    out
  }
}

object ImmutableArray {
  def apply[A: ClassTag](): ImmutableArray[A] = empty[A]
  def empty[A: ClassTag]: ImmutableArray[A] = new ImmutableArray(Array.empty[A])

  // The call to .clone() is needed here to ensure that arrays passed into .apply with :_* will not share a reference
  // with the underlying array in the returned ImmutableArray instance. See ImmutableArrayTests::immutableArrayVarargs.
  def apply[A: ClassTag](as: A*): ImmutableArray[A] = new ImmutableArray(as.toArray.clone)

  def apply[A](as: Array[A]): ImmutableArray[A] = new ImmutableArray(as.clone)

  def wrapped[A](as: Array[A]): ImmutableArray[A] = new ImmutableArray(as)

  implicit def canBuildFrom[T: ClassTag, Node[_]]: CanBuildFrom[ImmutableArray[Node[T]], T, ImmutableArray[T]] = {
    new CanBuildFrom[ImmutableArray[Node[T]], T, ImmutableArray[T]] {
      override def apply(from: ImmutableArray[Node[T]]): mutable.Builder[T, ImmutableArray[T]] = null
      override def apply(): mutable.Builder[T, ImmutableArray[T]] = {
        new mutable.Builder[T, ImmutableArray[T]] {
          private val buf = mutable.ArrayBuffer.empty[T]
          // Using ImmutableArray.wrapped here as a reference to the array should never escape
          override def result(): ImmutableArray[T] = ImmutableArray.wrapped[T](buf.toArray)
          override def clear(): Unit = buf.clear()
          override def +=(e: T): this.type = { buf += e; this }
        }
      }
    }
  }

  // support `coll.apar.map(f)(ImmutableArray.breakOut)
  def breakOut[T: ClassTag, Node[_]]: CanBuildFrom[ImmutableArray[Node[T]], T, ImmutableArray[T]] =
    canBuildFrom[T, Node]

  // support `coll.to(ImmutableArray)`
  implicit def factoryToCBF[T: ClassTag, Node[_]](
      factory: ImmutableArray.type
  ): CanBuildFrom[ImmutableArray[Node[T]], T, ImmutableArray[T]] = canBuildFrom
}
