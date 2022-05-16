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

import java.io.ObjectStreamException
import java.lang.Double.doubleToLongBits
import java.util
import java.util.Arrays

import scala.annotation.switch
import scala.collection.IndexedSeqView
import scala.collection.IterableFactoryDefaults
import scala.collection.IterableOnce
import scala.collection.immutable.ArraySeq
import scala.collection.immutable.StrictOptimizedSeqOps
import scala.collection.mutable
import scala.collection.{immutable, mutable}
import scala.collection.mutable.WrappedArray
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

object OptimusDoubleSeq
    extends OSeqCompanion[Double]
    with scala.collection.SpecificIterableFactory[Double, OptimusDoubleSeq] {
  def from(t: IterableOnce[Double]): OptimusDoubleSeq = {
    t match {
      case os: OptimusDoubleSeq => os
      case _                    => withSharedBuilder(_ ++= t)
    }
  }

  override def newBuilder: mutable.Builder[Double, OptimusDoubleSeq] = genCBF.newBuilder()
  override def fromSpecific(it: IterableOnce[Double]): OptimusDoubleSeq = {
    if (it.knownSize == 0) empty
    else {
      val builder = genCBF.newBuilder()
      builder ++= it
      builder.result()
    }
  }

  @inline private def withSharedBuilder(fn: OptimusBuilder[Double, OptimusDoubleSeq] => Unit): OptimusDoubleSeq = {
    val b = borrowBuilder
    try {
      fn(b)
      b.result()
    } finally b.returnBorrowed()
  }

  private val emptyHolder = new OptimusDoubleSeq(Array.emptyDoubleArray)

  def empty: OptimusDoubleSeq = emptyHolder
  override def apply(elems: Double*): OptimusDoubleSeq =
    /* we have to copy the array to protect from code like
    val data = Array(1d, 2d, 3d, 4d, 5d, 6d, 7d, 8d, 9d, 10d)
    val exposed = OptimusDoubleSeq(data :_*)
     */
    if (elems.isEmpty) empty
    else {
      elems match {
        case as: ArraySeq.ofDouble =>
          apply(as.unsafeArray)
        case _ =>
          apply(elems.toArray)
      }
    }
  def apply(elems: Array[Double]): OptimusDoubleSeq =
    if (elems.length == 0) empty else new OptimusDoubleSeq(elems.clone())

  private[collection] def borrowBuilder[A]: OptimusDoubleBuilder[OptimusDoubleSeq] = genCBF.borrowBuilder()

  // support `coll.apar.map(f)(OptimusDoubleSeq.breakOut)
  def breakOut[T]: OptimusCanBuildFrom[Seq[Double], Double, OptimusDoubleSeq, Double] = genCBF
  // support `collection.to(OptimusDoubleSeq)`
  implicit def factoryToCBF(
      facotry: OptimusDoubleSeq.type
  ): OptimusCanBuildFrom[Seq[Double], Double, OptimusDoubleSeq, Double] = genCBF
  private[collection] object genCBF
      extends OptimusCanBuildFrom[Seq[Double], Double, OptimusDoubleSeq, Double](
        "OptimusDoubleSeq.genCBF",
        OptimusDoubleSeq.empty,
        OptimusDoubleSeqSettings
      ) {

    override type BuilderType = OptimusDoubleBuilder[OptimusDoubleSeq]

    override protected def makeBuilder(): OptimusDoubleBuilder[OptimusDoubleSeq] =
      new Builder1D

    final class Builder1D private[OptimusDoubleSeq] ()
        extends OptimusDoubleBuilder[OptimusDoubleSeq]
        with OptimusBuilderImpl[Double, OptimusDoubleSeq] {

      override protected var elems: Array[Double] = _
      protected final def capacity: Int = if (elems eq null) 0 else elems.length
      override protected def newArray(size: Int): Array[Double] = new Array[Double](size)
      override protected def copyToArray(): Array[Double] = util.Arrays.copyOf(elems, elemsIndex)

      override def addOne(elem: Double): this.type = {
        prepareForAdditional(1)
        elems(elemsIndex) = elem
        elemsIndex += 1
        this
      }

      override def addAll(xs: IterableOnce[Double]): this.type = xs match {
        case xs: ArraySeq.ofDouble =>
          copyFromArray(xs.unsafeArray, 0, xs.length)
          this
        case xs: OptimusDoubleSeq =>
          flushToArrays()
          addArray(xs.data)
          this
        case _ =>
          super.addAll(xs)
      }

      override def result() = {
        val res: OptimusDoubleSeq = if (arrays eq null) {
          (elemsIndex: @switch) match {
            case 0 => empty
            case _ =>
              if (capacity == elemsIndex) {
                val res = unsafeFromArray(elems)
                elems = null
                res
              } else unsafeFromArray(copyToArray())
          }
        } else {
          var size = elemsIndex
          var ai = 0
          while (ai < arrays.length && (arrays(ai) ne null)) {
            size += arrays(ai).length
            ai += 1
          }
          val newArray = new Array[Double](size)
          var oi = 0
          ai = 0
          while (ai < arrays.length && (arrays(ai) ne null)) {
            System.arraycopy(arrays(ai), 0, newArray, oi, arrays(ai).length)
            oi += arrays(ai).length
            ai += 1
          }
          if ((elems ne null) && elemsIndex > 0) {
            System.arraycopy(elems, 0, newArray, oi, elemsIndex)
            oi += elemsIndex
          }
          assert(oi == size)
          unsafeFromArray(newArray)
        }
        returnElemsArrayIfAppropriate()
        res
      }

      override def toString = "OptimusDoubleSeqBuilder"
    }
  }

  def tabulate(size: Int)(elemFn: OSeqTabulate[Double]): OptimusDoubleSeq =
    if (size <= 0) empty
    else {
      val res = new Array[Double](size)
      var i = 0
      while (i < size) {
        res(i) = elemFn(i)
        i += 1
      }
      unsafeFromArray(res)
    }
  def fill(size: Int)(elem: Double): OptimusDoubleSeq = {
    if (size <= 0) empty
    else {
      val res = new Array[Double](size)
      Arrays.fill(res, elem)
      unsafeFromArray(res)
    }
  }
  // internal methods
  private[collection] def unsafeFromArray(elems: Array[Double]): OptimusDoubleSeq =
    if (elems.length == 0) empty else new OptimusDoubleSeq(elems)

  // optimised apply methods to avoid array copying
  def apply(): OptimusDoubleSeq = empty
  def apply(d1: Double): OptimusDoubleSeq =
    unsafeFromArray(Array(d1))
  def apply(d1: Double, d2: Double): OptimusDoubleSeq =
    unsafeFromArray(Array(d1, d2))
  def apply(d1: Double, d2: Double, d3: Double): OptimusDoubleSeq =
    unsafeFromArray(Array(d1, d2, d3))
  def apply(d1: Double, d2: Double, d3: Double, d4: Double): OptimusDoubleSeq =
    unsafeFromArray(Array(d1, d2, d3, d4))
  // OSeqCompanion methods
  override private[collection] def emptyOS[A <: Double]: OSeq[A] =
    empty.asInstanceOf[OSeq[A]]
  override private[collection] def tabulateOS[A <: Double](size: Int)(elemFn: OSeqTabulate[A]): OSeq[A] =
    tabulate(size)(elemFn).asInstanceOf[OSeq[A]]
  override private[collection] def fillOS[A <: Double](size: Int)(elem: A): OSeq[A] =
    fill(size)(elem).asInstanceOf[OSeq[A]]
}
object OptimusDoubleSeqSettings extends OSeqImpl {
  override def minArraySize: Int = 10000
  override def maxArraySize: Int = 10000
}

/**
 * A wrapper for [[Array]]s of primitive [[Double]] values. This optimises memory and CPU use by not boxing when not
 * needed It exhibits efficient indexing compared to the default IndexSeq ([[Vector]]). It is optimus friendly with a
 * lazily cached hashcode
 */
final class OptimusDoubleSeq private (private val data: Array[Double])
    extends OSeq[Double]
    with immutable.IndexedSeqOps[Double, OSeq, OptimusDoubleSeq]
    with StrictOptimizedSeqOps[Double, OSeq, OptimusDoubleSeq]
    with IterableFactoryDefaults[Double, OSeq] {

  import OptimusDoubleSeq.unsafeFromArray

  override protected def fromSpecific(coll: IterableOnce[Double]): OptimusDoubleSeq =
    OptimusDoubleSeq.fromSpecific(coll)
  override protected def newSpecificBuilder: mutable.Builder[Double, OptimusDoubleSeq] = OptimusDoubleSeq.newBuilder
  override def empty: OptimusDoubleSeq = OptimusDoubleSeq.empty
  override def length: Int = data.length
  override def apply(idx: Int): Double = data(idx)
  override def equals(other: Any): Boolean = other match {
    case that: OptimusDoubleSeq =>
      (this eq that) || (
        data.length == that.data.length &&
          this.hashCode == that.hashCode && Arrays.equals(this.data, that.data)
      )
    case _ => super.equals(other)
  }
  @volatile private[this] var _knownHashCode: Boolean = _
  private[this] var _hashCode: Int = _
  override def hashCode: Int =
    if (_knownHashCode) _hashCode
    else {
      // use seqSeed so that hashCode is consistent with Seq[Double]'s
      val hashCode = MurmurHash3.arrayHash(data, MurmurHash3.seqSeed)
      // note - this is order important
      // hashcode must be written before known
      _hashCode = hashCode
      // volatile write, forces the _hashcode to be visible
      _knownHashCode = true
      hashCode
    }
  override def sameElements[B >: Double](other: IterableOnce[B]): Boolean = {
    other match {
      case that: OptimusDoubleSeq =>
        this.size == that.size && this.hashCode == that.hashCode &&
        (java.util.Arrays.equals(this.data, that.data))
      case _ =>
        super.sameElements(other)
    }
  }

  /**
   * @inheritdoc
   * optimised to avoid boxing where the result is a [[Double]]
   */
  override def sum[B >: Double](implicit num: Numeric[B]): B =
    if (num ne Numeric.DoubleIsFractional) {
      super.sum(num)
    } else {
      var result = 0d
      var index = 0
      while (index < data.length) {
        result += data(index)
        index += 1
      }
      result
    }

  /**
   * @inheritdoc
   * optimised to avoid boxing where the result is a [[Double]]
   */
  override def product[B >: Double](implicit num: Numeric[B]): B =
    if (num ne Numeric.DoubleIsFractional) {
      super.product(num)
    } else {
      var result = 1d
      var index = 0
      while (index < data.length) {
        result *= data(index)
        index += 1
      }
      result
    }

  /**
   * @inheritdoc
   * optimised to avoid boxing where the result is a [[OptimusDoubleSeq]] optimised to zero allocation and return this
   * if the result would == this to save memory and later hashcode
   */
  def map[B](f: Double => Double): OptimusDoubleSeq = {
    val f1 = f.asInstanceOf[Double => Double]
    var newData: Array[Double] = null
    var i = 0
    while (i < data.length) {
      val existing = data(i)
      val result = f1(existing)
      if ((newData eq null) && doubleToLongBits(existing) != doubleToLongBits(result)) {
        newData = new Array[Double](data.length)
        System.arraycopy(data, 0, newData, 0, i)
      }
      if (newData ne null)
        newData(i) = result
      i += 1
    }
    if (newData eq null) this
    else unsafeFromArray(newData)
  }
  override def map[B](f: Double => B): OSeq[B] = {
    super.map(f)
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying when new data is needed, and this/empty to save memory and later hashcode
   * for known results
   */
  override def slice(from: Int, until: Int): OptimusDoubleSeq = {
    val lo = Math.min(Math.max(from, 0), data.length)
    val hi = Math.min(Math.max(until, lo), data.length)
    if (lo == hi) OptimusDoubleSeq.empty
    else if (lo == 0 && hi == data.length) this
    else unsafeFromArray(Arrays.copyOfRange(data, lo, hi))
  }

  override def take(n: Int): OptimusDoubleSeq = {
    slice(0, n)
  }

  override def drop(n: Int): OptimusDoubleSeq = {
    slice(n, length)
  }

  override def takeRight(n: Int): OptimusDoubleSeq = {
    slice(length - n, length)
  }

  override def dropRight(n: Int): OptimusDoubleSeq = {
    slice(0, length - n)
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying when new data is needed, and this/empty to save memory and later hashcode
   * for known results
   */
  override def appendedAll[B >: Double](that: IterableOnce[B]): OSeq[B] = {
    def append(that: Array[Double]): OSeq[B] = {
      val newLength = data.length + that.length
      val newData = Arrays.copyOf(data, newLength)
      System.arraycopy(that, 0, newData, data.length, that.length)
      unsafeFromArray(newData)
    }

    that match {
      case it: Iterable[B] if it.isEmpty => this
      case ods: OptimusDoubleSeq         => append(ods.data)
      case asod: ArraySeq.ofDouble       => append(asod.unsafeArray)
      case _                             =>
        // TODO: Could use a OptimusDoubleSeq builder optimistically assuming that that contains Doubles,
        //       and fall back to an OptimusSeq builder if an element of a different type is detected.
        //       Same applies for
        super.appendedAll(that)
    }
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying when new data is needed, and this/empty to save memory and later hashcode
   * for known results
   */
  override def prependedAll[B >: Double](that: IterableOnce[B]): OSeq[B] = {
    def prepend(that: Array[Double]): OSeq[B] = {
      val newLength = data.length + that.length
      val newData = Arrays.copyOf(that, newLength)
      System.arraycopy(data, 0, newData, that.length, data.length)
      unsafeFromArray(newData)
    }

    that match {
      case it: Iterable[B] if it.isEmpty => this
      case ods: OptimusDoubleSeq         => prepend(ods.data)
      case asod: ArraySeq.ofDouble       => prepend(asod.unsafeArray)
      case _                             => super.prependedAll(that)
    }
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying
   */
  override def appended[B >: Double](that: B): OSeq[B] = {
    that match {
      case d: Double =>
        val newData = Arrays.copyOf(data, data.length + 1)
        newData(data.length) = d
        unsafeFromArray(newData).asInstanceOf[OptimusSeq[B]]
      case _ =>
        super.appended(that)
    }
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying
   */
  override def prepended[B >: Double](that: B): OSeq[B] = {
    that match {
      case _: Double =>
        val newData = new Array[Double](data.length + 1)
        // do th arraycopy first as JVM elides the clear
        System.arraycopy(data, 0, newData, 1, data.length)
        newData(0) = that.asInstanceOf[Double]
        unsafeFromArray(newData).asInstanceOf[OptimusSeq[B]]
      case _ =>
        super.prepended(that)
    }
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying
   */
  override def padTo[B >: Double](len: Int, elem: B): OSeq[B] = {
    if (length <= data.length) this.asInstanceOf[OptimusSeq[B]]
    else {
      elem match {
        case d: Double =>
          val newData = Arrays.copyOf(data, len)
          Arrays.fill(newData, data.length, len, d)
          unsafeFromArray(newData).asInstanceOf[OptimusSeq[B]]
        case _ =>
          super.padTo(len, elem)
      }
    }
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying
   */
  override def copyToArray[B >: Double](xs: Array[B], start: Int, len: Int): Int = {
    val n = OptimusSeq.elemsToCopyToArray(data.length, xs.length, start, len)
    if (xs.getClass.getComponentType == classOf[Double]) {
      System.arraycopy(data, 0, xs, start, n)
      n
    } else super.copyToArray(xs, start, len)
  }

  /**
   * @inheritdoc
   * optimised to avoid boxing, if the function is explicitly [[Double]]
   */
  override def foreach[U](f: Double => U): Unit = {
    var i = 0
    val len = length
    while (i < len) {
      f(this(i))
      i += 1
    }
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying
   */
  override def toArray[B >: Double](implicit B: ClassTag[B]): Array[B] = {
    if (B.runtimeClass == classOf[Double]) {
      // here we know that B =:= Double

      // if length is 0, we can share
      (if (data.length == 0) data
       else data.clone()).asInstanceOf[Array[B]]
    } else super.toArray
  }

  override def head: Double = data.length match {
    case 0 => throw new NoSuchElementException("OptimusDoubleSeq.head")
    case _ => data(0)
  }
  override def last: Double = data.length match {
    case 0 => throw new NoSuchElementException("OptimusDoubleSeq.last")
    case n => data(n - 1)
  }
  override def init: OptimusDoubleSeq = data.length match {
    case 0 => throw new UnsupportedOperationException("OptimusDoubleSeq.init")
    case 1 => OptimusDoubleSeq.empty
    case _ => unsafeFromArray(Arrays.copyOf(data, data.length - 1))
  }
  override def tail: OptimusDoubleSeq = data.length match {
    case 0 => throw new UnsupportedOperationException("OptimusDoubleSeq.tail")
    case 1 => OptimusDoubleSeq.empty
    case _ => unsafeFromArray(Arrays.copyOfRange(data, 1, data.length))
  }

  override final def filter(f: Double => Boolean): OptimusDoubleSeq = filter0(f, true)
  override final def filterNot(f: Double => Boolean): OptimusDoubleSeq = filter0(f, false)
  private def filter0(f: Double => Boolean, include: Boolean): OptimusDoubleSeq = {
    val builder = OptimusDoubleSeq.borrowBuilder
    try {
      var in = 0
      val length = size
      while (in < length) {
        val value = apply(in)
        if (include == f(value)) {
          builder.addOne(value)
        }
        in += 1
      }
      if (builder.resultSize == length) this
      else builder.result()
    } finally builder.returnBorrowed()

  }
  // non collection methods
  def zipWith(other: OptimusDoubleSeq)(f: (Double, Double) => Double): OptimusDoubleSeq = {
    if (this.isEmpty || other.isEmpty) OptimusDoubleSeq.empty
    else {
      val thisData = this.data
      val thatData = other.data
      var i = 0
      val res = new Array[Double](Math.min(thisData.length, thatData.length))
      while (i < res.length) {
        res(i) = f(thisData(i), thatData(i))
        i += 1
      }
      unsafeFromArray(res)
    }
  }

  def mapWithIndexDouble(f: (Double /*value*/, Int /*index*/ ) => Double /*result*/ ): OptimusDoubleSeq =
    if (isEmpty) OptimusDoubleSeq.empty
    else {
      var newData: Array[Double] = null
      var i = 0
      while (i < data.length) {
        val existing = data(i)
        val transformed = f(existing, i)
        if (transformed != existing && (newData eq null)) {
          newData = new Array[Double](data.length)
          System.arraycopy(data, 0, newData, 0, i)
        }
        if (newData ne null)
          newData(i) = transformed
        i += 1
      }
      if (newData eq null) this
      else unsafeFromArray(newData)
    }

  /**
   * a bit like a .zipWithIndex.foldLeft or just foldLeft, but without the boxing and the intermediate collections
   * Implementation note - we use FoldToDoubleWithIndex rather than a scala function type as Function3 is not
   * \@specialised
   */
  def foldLeftToDoubleWithIndex(initial: Double)(f: OptimusDoubleSeqFoldLeftWithIndex): Double = {
    var res = initial
    var i = 0
    while (i < data.length) {
      res = f(i, res, data(i))
      i += 1
    }
    res
  }

  def multiplyEach(v: Double): OptimusDoubleSeq = {
    if (v == 1.0d) this else map(_ * v)
  }
  def multiplyEach(v: OptimusDoubleSeq): OptimusDoubleSeq = {
    require(v.length == length)
    zipWith(v)(_ * _)
  }
  def divideEach(v: Double): OptimusDoubleSeq = {
    if (v == 1.0d) this else map(_ / v)
  }
  def divideEach(v: OptimusDoubleSeq): OptimusDoubleSeq = {
    require(v.length == length)
    zipWith(v)(_ / _)
  }
  def addEach(v: Double): OptimusDoubleSeq = {
    if (v == 0.0d) this else map(_ + v)
  }
  def addEach(v: OptimusDoubleSeq): OptimusDoubleSeq = {
    require(v.length == length)
    zipWith(v)(_ + _)
  }
  def subtractEach(v: Double): OptimusDoubleSeq = {
    if (v == 0.0d) this else map(_ - v)
  }
  def subtractEach(v: OptimusDoubleSeq): OptimusDoubleSeq = {
    require(v.length == length)
    zipWith(v)(_ - _)
  }

  def abs(): OptimusDoubleSeq = map { Math.abs }

  def sumOfSquares: Double = {
    foldLeftToDoubleWithIndex(0.0d)((_, previous, thisValue) => previous + thisValue * thisValue)
  }

  def addAll(that1: IterableOnce[Double]): OptimusDoubleSeq = {
    if (that1.knownSize == 0) this
    else
      OptimusDoubleSeq.withSharedBuilder { builder =>
        builder.addAll(this)
        builder.addAll(that1)
      }
  }
  def addAll(that1: IterableOnce[Double], that2: IterableOnce[Double]): OptimusDoubleSeq = {
    if (that1.knownSize == 0) this
    else
      OptimusDoubleSeq.withSharedBuilder { builder =>
        builder.addAll(this)
        builder.addAll(that1)
        builder.addAll(that2)
      }
  }
  def addAll(
      that1: IterableOnce[Double],
      that2: IterableOnce[Double],
      that3: IterableOnce[Double]): OptimusDoubleSeq = {
    if (that1.knownSize == 0) this
    else
      OptimusDoubleSeq.withSharedBuilder { builder =>
        builder.addAll(this)
        builder.addAll(that1)
        builder.addAll(that2)
        builder.addAll(that3)
      }
  }
  def addAll(
      that1: IterableOnce[Double],
      that2: IterableOnce[Double],
      that3: IterableOnce[Double],
      that4: IterableOnce[Double]): OptimusDoubleSeq = {
    if (that1.knownSize == 0) this
    else
      OptimusDoubleSeq.withSharedBuilder { builder =>
        builder.addAll(this)
        builder.addAll(that1)
        builder.addAll(that2)
        builder.addAll(that3)
        builder.addAll(that4)
      }
  }
  def addAll(
      that1: IterableOnce[Double],
      that2: IterableOnce[Double],
      that3: IterableOnce[Double],
      that4: IterableOnce[Double],
      rest: IterableOnce[Double]*): OptimusDoubleSeq = {
    if (
      that1.knownSize == 0 && that2.knownSize == 0 && that3.knownSize == 0 && that4.knownSize == 0 && rest.forall(
        _.knownSize == 0)
    ) this
    else
      OptimusDoubleSeq.withSharedBuilder { builder =>
        builder.addAll(this)
        builder.addAll(that1)
        builder.addAll(that2)
        builder.addAll(that3)
        builder.addAll(that4)
        rest foreach (builder.addAll(_))
      }
  }

  override protected[this] def className: String = "OptimusDoubleSeq"

  // noinspection ScalaUnusedSymbol
  @throws[ObjectStreamException]("supposedly needed by serialization")
  private[this] def readResolve(): AnyRef =
    if (data.isEmpty) empty else this
}
trait OptimusDoubleSeqFoldLeftWithIndex {
  def apply(index: Int, resultFromPrevious: Double, valueAtIndex: Double): Double
}
