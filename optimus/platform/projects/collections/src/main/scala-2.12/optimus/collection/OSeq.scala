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
package optimus.collection

import scala.annotation.compileTimeOnly
import scala.collection.AbstractSeq
import scala.reflect.ClassTag

object OSeq {
  def emptySeq[T: ClassTag]: OSeq[T] = {
    getCompanion[T].emptyOS[Nothing]
  }

  @compileTimeOnly("Use OSeq.emptySeq[T] instead, as of Scala 2.13 OSeq.empty cannot accept a ClassTag")
  def empty[A]: OptimusSeq[A] = throw new UnsupportedOperationException()

  // optimised apply methods to avoid array copying
  def apply[T: ClassTag](): OSeq[T] = {
    emptySeq[T]
  }
  def apply(d1: Double): OptimusDoubleSeq = OptimusDoubleSeq.apply(d1)
  def apply(d1: Double, d2: Double): OptimusDoubleSeq = OptimusDoubleSeq.apply(d1, d2)
  def apply(d1: Double, d2: Double, d3: Double): OptimusDoubleSeq = OptimusDoubleSeq.apply(d1, d2, d3)
  def apply(d1: Double, d2: Double, d3: Double, d4: Double): OptimusDoubleSeq = OptimusDoubleSeq.apply(d1, d2, d3, d4)

  //  def apply[T](p1: T): OptimusSeq[T] = OptimusSeq.apply(p1)
  def apply[T](p1: T, p2: T): OptimusSeq[T] = OptimusSeq.apply(p1, p2)
  def apply[T](p1: T, p2: T, p3: T): OptimusSeq[T] = OptimusSeq.apply(p1, p2, p3)
  def apply[T](p1: T, p2: T, p3: T, p4: T): OptimusSeq[T] = OptimusSeq.apply(p1, p2, p3, p4)

  private def getCompanion[T: ClassTag]: OSeqCompanion[T] = {
    val clz = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]
    if (clz.isPrimitive) {
      clz match {
        case java.lang.Double.TYPE => OptimusDoubleSeq.asInstanceOf[OSeqCompanion[T]]
        // TODO (OPTIMUS-45858): Other Data Types to be implemented for pattern matching in Collections
        case _ => OptimusSeq.asInstanceOf[OSeqCompanion[T]]
      }
    } else OptimusSeq.asInstanceOf[OSeqCompanion[T]]
  }
  def tabulate[T: ClassTag](size: Int)(elemFn: OSeqTabulate[T]): OSeq[T] = {
    getCompanion[T].tabulateOS[T](size)(elemFn)
  }
  def fill[T: ClassTag](size: Int)(elem: T): OSeq[T] = {
    getCompanion[T].fillOS[T](size)(elem)
  }
}
abstract class OSeq[+T] private[collection] ()
    extends AbstractSeq[T]
    with IndexedSeq[T]
    with scala.collection.immutable.IndexedSeq[T]
    with Serializable {

  override final def size = length
  override def unzip[A1, A2](implicit asPair: T => (A1, A2)): (OptimusSeq[A1], OptimusSeq[A2]) = {
    if (isEmpty) {
      (OptimusSeq.empty, OptimusSeq.empty)
    } else {
      val aBuilder = OptimusSeq.borrowBuilder[A1]
      val bBuilder = OptimusSeq.borrowBuilder[A2]
      try {
        val size = length
        var i = 0
        while (i < size) {
          val (a, b) = asPair(apply(i))
          aBuilder += a
          bBuilder += b
          i += 1
        }
        (aBuilder.result(), bBuilder.result())
      } finally {
        aBuilder.returnBorrowed()
        bBuilder.returnBorrowed()
      }
    }
  }
  override def unzip3[A1, A2, A3](implicit
      asTriple: T => (A1, A2, A3)): (OptimusSeq[A1], OptimusSeq[A2], OptimusSeq[A3]) = {
    if (isEmpty) {
      (OptimusSeq.empty, OptimusSeq.empty, OptimusSeq.empty)
    } else {
      val aBuilder = OptimusSeq.borrowBuilder[A1]
      val bBuilder = OptimusSeq.borrowBuilder[A2]
      val cBuilder = OptimusSeq.borrowBuilder[A3]
      try {
        val size = length
        var i = 0
        while (i < size) {
          val (a, b, c) = asTriple(apply(i))
          aBuilder += a
          bBuilder += b
          cBuilder += c
          i += 1
        }
        (aBuilder.result(), bBuilder.result(), cBuilder.result())
      } finally {
        aBuilder.returnBorrowed()
        bBuilder.returnBorrowed()
        cBuilder.returnBorrowed()
      }
    }
  }
  ///// extension methods
  /** similar to .zipWithIndex.foldLeft or foldLeft, but without the tupling */
  def foldLeftWithIndex[B, R, T1 >: T](z: R)(f: OSeqFoldLeftWithIndex[R, T1]): R = {
    val size = length
    var i = 0
    var acc = z
    while (i < size) {
      acc = f(i, acc, apply(i))
      i += 1
    }
    acc
  }
}
private[collection] abstract class OSeqImpl {
  def minArraySize: Int
  def maxArraySize: Int
}
//Function1 is missing a specialisation for [Int, AnyRef]
trait OSeqTabulate[@specialized(scala.AnyRef, scala.Double) +T] {
  def apply(index: Int): T
}
trait OSeqFoldLeftWithIndex[R, T] {
  def apply(index: Int, resultFromPrevious: R, valueAtIndex: T): R
}
