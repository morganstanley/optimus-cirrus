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

import java.util
import java.util.Arrays

import scala.annotation.switch
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.AbstractIterator
import scala.collection.IterableFactoryDefaults
import scala.collection.SeqFactory
import scala.collection.IterableOnce
import scala.collection.immutable
import scala.collection.mutable
import scala.runtime.AbstractFunction1
import scala.util.hashing.MurmurHash3

/**
 * A companion object used to create instances of `OptimusSeq`.
 */
object OptimusSeq extends SeqFactory[OptimusSeq] with OSeqCompanion[Any] {
  def from[T](t: IterableOnce[T]): OptimusSeq[T] = {
    t match {
      case os: OptimusSeq[T] => os
      case _                 => withSharedBuilder[T](_ ++= t)
    }
  }

  @inline private[collection] def withSharedBuilder[Elem](
      fn: OptimusBuilder[Elem, OptimusSeq[Elem]] => Unit): OptimusSeq[Elem] = {
    val b = borrowBuilder[Elem]
    try {
      fn(b)
      b.result()
    } finally b.returnBorrowed()
  }

  // This is reused for all calls to empty.
  private[collection] val EMPTY = OptimusSeqEmpty
  def empty[T]: OptimusSeq[T] = EMPTY

  def fromArray[T](elems: Array[T]): OptimusSeq[T] = (elems.length: @switch) match {
    case 0 => EMPTY
    case 1 => apply(elems(0))
    case 2 => apply(elems(0), elems(1))
    case 3 => apply(elems(0), elems(1), elems(2))
    case 4 => apply(elems(0), elems(1), elems(2), elems(3))
    case _ =>
      OptimusSeq.withSharedBuilder[T] { b =>
        b.addFromArray(elems, 0, elems.length)
      }
  }

  def apply[T](p1: T, p2: T, p3: T, p4: T, elems: T*): OptimusSeq[T] = {
    val size = elems.size + 4
    if (size <= OptimusSeqImpl.maxArraySize) {
      val copy = new Array[Any](size)
      copy(0) = p1
      copy(1) = p2
      copy(2) = p3
      copy(3) = p4
      elems.copyToArray(copy, 4)
      unsafeFromAnyArray(copy)
    } else
      OptimusSeq.withSharedBuilder[T] { b =>
        b += p1
        b += p2
        b += p3
        b += p4
        b ++= elems
      }
  }
  def tabulate[T](size: Int)(elemFn: OSeqTabulate[T]): OptimusSeq[T] = (size: @switch) match {
    case 0 => EMPTY
    case 1 => apply(elemFn(0))
    case 2 => apply(elemFn(0), elemFn(1))
    case 3 => apply(elemFn(0), elemFn(1), elemFn(2))
    case 4 => apply(elemFn(0), elemFn(1), elemFn(2), elemFn(3))
    case _ =>
      if (size < 0) EMPTY
      else if (size < OptimusSeqImpl.maxArraySize) {
        val res = new Array[Any](size)
        var i = 0
        while (i < size) {
          res(i) = elemFn(i)
          i += 1
        }
        unsafeFromAnyArray(res)
      } else {
        val arraysCount = (size - 1) / OptimusSeqImpl.maxArraySize + 1
        val arrays = new Array[Array[AnyRef]](arraysCount)
        var outIdx, i, j = 0
        while (i < size) {
          val thisSize = Math.min(OptimusSeqImpl.maxArraySize, size - i)
          val array = new Array[Any](thisSize)
          arrays(outIdx) = array.asInstanceOf[Array[AnyRef]]
          j = 0
          while (j < thisSize) {
            array(j) = elemFn(i)
            i += 1
            j += 1
          }
          outIdx += 1
        }
        OptimusArraysSeq[T](arrays, null)
      }
  }

  def fill[T](size: Int)(elem: T): OptimusSeq[T] = (size: @switch) match {
    case 0 => EMPTY
    case 1 => apply(elem)
    case 2 => apply(elem, elem)
    case 3 => apply(elem, elem, elem)
    case 4 => apply(elem, elem, elem, elem)
    case _ =>
      if (size < 0) EMPTY
      else if (size < OptimusSeqImpl.maxArraySize) {
        val res = new Array[AnyRef](size)
        util.Arrays.fill(res, elem.asInstanceOf[AnyRef])
        unsafeFromArray(res)
      } else {
        val arraysCount = (size - 1) / OptimusSeqImpl.maxArraySize + 1
        val first = new Array[AnyRef](OptimusSeqImpl.maxArraySize)
        util.Arrays.fill(first, elem.asInstanceOf[AnyRef])
        val arrays = new Array[Array[AnyRef]](arraysCount)
        util.Arrays.fill(arrays.asInstanceOf[Array[AnyRef]], first)

        val lastSize = size - (arraysCount - 1) * OptimusSeqImpl.maxArraySize
        if (lastSize != OptimusSeqImpl.maxArraySize)
          arrays(arraysCount - 1) = util.Arrays.copyOf(first, lastSize)
        OptimusArraysSeq[T](arrays, null)
      }
  }

  def newBuilder[A]: OptimusBuilder[A, OptimusSeq[A]] = canBuildFrom[A].newBuilder()
  private[collection] def borrowBuilder[A]: OptimusBuilder[A, OptimusSeq[A]] = canBuildFrom[A].borrowBuilder()
  private def canBuildFrom[T]: OptimusCanBuildFrom[OptimusSeq[_], T, OptimusSeq[T], AnyRef] =
    genCBF.asInstanceOf[OptimusCanBuildFrom[OptimusSeq[_], T, OptimusSeq[T], AnyRef]]
  // support `coll.apar.map(f)(OptimusSeq.breakOut)
  def breakOut[T]: OptimusCanBuildFrom[OptimusSeq[_], T, OptimusSeq[T], AnyRef] = canBuildFrom
  // support `coll.to(OptimusSeq)`
  implicit def factoryToCBF[T](factory: OptimusSeq.type): OptimusCanBuildFrom[OptimusSeq[_], T, OptimusSeq[T], AnyRef] =
    canBuildFrom
  private[collection] object genCBF
      extends OptimusCanBuildFrom[Seq[AnyRef], AnyRef, OptimusSeq[AnyRef], AnyRef](
        "OptimusSeq.genCBF",
        OptimusSeq.empty,
        OptimusSeqImpl) {
    override type BuilderType = OptimusSeqBuilder[AnyRef]

    override protected def makeBuilder() = {
      new OptimusSeqBuilder[AnyRef]
    }
    private[optimus] final class OptimusSeqBuilder[T]
        extends OptimusBuilder[T, OptimusSeq[T]]
        with OptimusBuilderImpl[T, OptimusSeq[T]] {

      protected var elems: Array[AnyRef] = _
      protected final def capacity: Int = if (elems eq null) 0 else elems.length
      override protected def newArray(size: Int): Array[AnyRef] = new Array[AnyRef](size)
      override protected def copyToArray(): Array[AnyRef] = util.Arrays.copyOf(elems, elemsIndex)

      override def addOne(elem: T): this.type = {
        prepareForAdditional(1)
        addRaw(elem)
        this
      }
      @inline final def addRaw(elem: T): Unit = {
        elems(elemsIndex) = elem.asInstanceOf[AnyRef]
        elemsIndex += 1
      }

      override def addAll(xs: IterableOnce[T]): this.type = {
        xs match {
          case xs: OptimusSeq[T] =>
            xs match {
              case xs: OptimusArraySeq[T] =>
                flushToArrays()
                addArray(xs.data)
              case xs: OptimusArraysSeq[T] =>
                flushToArrays()
                val next = ensureArrays(xs.arrays.length)
                if (next == 0)
                  knownHashes = xs.copyKnownHashes
                System.arraycopy(xs.arrays, 0, arrays, next, xs.arrays.length)
              case xs: SmallOptimusSeq[T] =>
                val size = xs.size
                prepareForAdditional(size)
                xs.copyToBuilderArray(elems, elemsIndex)
                elemsIndex += size
              case OptimusSeqEmpty =>
            }
          case xs: mutable.WrappedArray.ofRef[_] =>
            copyFromArray(xs.array, 0, xs.length)
          case xs =>
            if (!xs.iterator.isEmpty)
              adder.addAll(xs)
        }
        this
      }
      private val adder = new Adder
      class Adder extends AbstractFunction1[T, Unit] {
        private[this] var remaining: Int = _
        private[this] var localElemIndex: Int = _
        def addAll(xs: IterableOnce[T]): Unit = {
          reset()
          xs foreach this
          elemsIndex = localElemIndex
        }
        def reset(): Unit = {
          remaining = prepareForAdditional(1)
          localElemIndex = elemsIndex
        }
        override def apply(v1: T): Unit = {
          if (remaining == 0) {
            elemsIndex = localElemIndex
            reset()
          }
          elems(localElemIndex) = v1.asInstanceOf[AnyRef]
          localElemIndex += 1
          remaining -= 1
        }
      }

      override def clearRefsInArray(array: Array[AnyRef], maxIndex: Int): Unit = {
        if (array ne null)
          util.Arrays.fill(array, 0, elemsIndex, null)
      }
      override def result(): OptimusSeq[T] = {
        val res: OptimusSeq[T] = if ((arrays eq null) || (arrays(0) eq null)) {
          (elemsIndex: @switch) match {
            case 0 => EMPTY
            case 1 => OptimusSeq.apply[AnyRef](elems(0)).asInstanceOf[OptimusSeq[T]]
            case 2 => OptimusSeq.apply[AnyRef](elems(0), elems(1)).asInstanceOf[OptimusSeq[T]]
            case 3 => OptimusSeq.apply[AnyRef](elems(0), elems(1), elems(2)).asInstanceOf[OptimusSeq[T]]
            case 4 => OptimusSeq.apply[AnyRef](elems(0), elems(1), elems(2), elems(3)).asInstanceOf[OptimusSeq[T]]
            case _ =>
              unsafeFromArray(copyToArray())
          }
        } else {
          flushToArrays()
          trimArrays()
          val res = if (arrays.length == 1) {
            // Optimise to retain the original collection??
            // this means that we probably did a
            // builder ++ coll; builder.result
            OptimusArraySeq.unsafeFromArray[T](arrays(0))
          } else
            OptimusArraysSeq[T](arrays, knownHashes)
          arrays = null
          knownHashes = null
          res
        }
        returnElemsArrayIfAppropriate()
        res
      }
    }
  }

  private def unsafeFromArray[T](elems: Array[AnyRef]): OptimusSeq[T] =
    OptimusArraySeq.unsafeFromArray(elems)
  private def unsafeFromAnyArray[T](elems: Array[Any]): OptimusSeq[T] =
    OptimusArraySeq.unsafeFromAnyArray(elems)
  // optimised apply methods
  def apply(): OptimusSeq[Nothing] = EMPTY
  def apply[T](p1: T): OptimusSeq[T] = {
    new OptimusSeq1[T](p1)
  }
  def apply[T](p1: T, p2: T): OptimusSeq[T] = {
    new OptimusSeq2[T](p1, p2)
  }
  def apply[T](p1: T, p2: T, p3: T): OptimusSeq[T] = {
    new OptimusSeq3[T](p1, p2, p3)
  }
  def apply[T](p1: T, p2: T, p3: T, p4: T): OptimusSeq[T] = {
    new OptimusSeq4[T](p1, p2, p3, p4)
  }
  // OSeqCompanion methods
  override private[collection] def emptyOS[A <: Any]: OSeq[A] =
    empty[A]
  override private[collection] def tabulateOS[A <: Any](size: Int)(elemFn: OSeqTabulate[A]): OSeq[A] =
    tabulate(size)(elemFn)
  override private[collection] def fillOS[A <: Any](size: Int)(elem: A): OSeq[A] =
    fill(size)(elem)

  private[this] val _collectMarker = new AbstractFunction1[Any, Any] {
    override def apply(v1: Any): Any = this
  }
  private[collection] def collectMarker[A, B] = _collectMarker.asInstanceOf[Function1[A, B]]
  private[collection] def elemsToCopyToArray(srcLen: Int, destLen: Int, start: Int, len: Int): Int =
    math.max(math.min(math.min(len, srcLen), destLen - start), 0)
}
abstract class OptimusSeq[+T]
    extends OSeq[T]
    with immutable.IndexedSeqOps[T, OptimusSeq, OptimusSeq[T]]
    with immutable.StrictOptimizedSeqOps[T, OptimusSeq, OptimusSeq[T]]
    with IterableFactoryDefaults[T, OptimusSeq] {
  import OptimusSeq.{EMPTY, collectMarker}
  @transient @volatile private[this] var _knownHashCode: Boolean = _
  @transient private[this] var _hashCode: Int = _
  override def hashCode: Int =
    if (_knownHashCode) _hashCode
    else {
      // use seqSeed so that hashCode is consistent with Seq[Double]'s
      val hashCode = computeHash
      // note - this is order important
      // hashcode must be written before known
      _hashCode = hashCode
      // volatile write, forces the _hashcode to be visible
      _knownHashCode = true
      hashCode
    }

  override def stringPrefix = "OptimusSeq"
  override def flatMap[B](f: T => IterableOnce[B]): OptimusSeq[B] = {
    var i = 0
    val builder = OptimusSeq.borrowBuilder[B]
    try {
      while (i < length) {
        val orig = apply(i)
        val res = f(orig)
        builder addAll res
        i += 1
      }
      builder.result()
    } finally builder.returnBorrowed()
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying when new data is needed optimised to save memory and later hashcode for
   * known results
   */
  override def slice(from: Int, to: Int): OptimusSeq[T] = {
    val lo = Math.min(Math.max(from, 0), size)
    val hi = Math.min(Math.max(to, lo), size)
    if (lo == 0 && hi == length) this
    else
      ((hi - lo): @switch) match {
        case 0 => EMPTY
        case 1 => new OptimusSeq1(apply(lo + 0))
        case 2 => new OptimusSeq2(apply(lo + 0), apply(lo + 1))
        case 3 => new OptimusSeq3(apply(lo + 0), apply(lo + 1), apply(lo + 2))
        case 4 => new OptimusSeq4(apply(lo + 0), apply(lo + 1), apply(lo + 2), apply(lo + 3))
        case _ =>
          // for a slice to be >4 it must by already > 4, so this must be a OptimusArray[s]Seq
          this match {
            case os: OptimusArraySeq[T] =>
              val range = Arrays.copyOfRange[AnyRef](os.data, lo, hi)
              val res = OptimusSeq.unsafeFromArray(range)
              res
            case os: OptimusArraysSeq[T] =>
              os.sliceSafe(lo, hi)
          }
      }
  }

  override def take(n: Int): OptimusSeq[T] = {
    slice(0, n)
  }

  override def drop(n: Int): OptimusSeq[T] = {
    slice(n, length)
  }

  override def takeRight(n: Int): OptimusSeq[T] = {
    slice(length - n, length)
  }

  override def dropRight(n: Int): OptimusSeq[T] = {
    slice(0, length - n)
  }

  /**
   * @inheritdoc
   * optimised to use efficient array copying when new data is needed, and this/empty to save memory and later hashcode
   * for known results
   */
  override def appendedAll[B >: T](that: IterableOnce[B]): OptimusSeq[B] = {
    if (that.knownSize == 0) this.asInstanceOf[OptimusSeq[B]]
    else if (this.isEmpty && that.isInstanceOf[OptimusSeq[B]]) that.asInstanceOf[OptimusSeq[B]]
    else {
      OptimusSeq
        .withSharedBuilder[B] { builder =>
          builder ++= this
          builder addAll that
        }
        .asInstanceOf[OptimusSeq[B]]
    }
  }

  override def prependedAll[B >: T](that: IterableOnce[B]): OptimusSeq[B] = {
    if (that.knownSize == 0) this.asInstanceOf[OptimusSeq[B]]
    else if (this.isEmpty && that.isInstanceOf[OptimusSeq[B]]) that.asInstanceOf[OptimusSeq[B]]
    else {
      OptimusSeq
        .withSharedBuilder[B] { builder =>
          builder ++= that
          builder ++= this
        }
        .asInstanceOf[OptimusSeq[B]]
    }
  }

  override final def filter(f: T => Boolean): OptimusSeq[T] = filter0(f, true)
  override final def filterNot(f: T => Boolean): OptimusSeq[T] = filter0(f, false)
  protected def filter0(f: T => Boolean, include: Boolean): OptimusSeq[T] = {
    val builder = OptimusSeq.borrowBuilder[T]
    try {
      var in = 0
      val length = size
      while (in < length) {
        val value = apply(in)
        if (include == f(value)) {
          builder += value
        }
        in += 1
      }
      if (builder.resultSize == length) this
      else builder.result()
    } finally builder.returnBorrowed()
  }

  final override def contains[A1 >: T](elem: A1): Boolean = indexOf(elem) >= 0
  final override def exists(p: T => Boolean): Boolean = indexWhere(p) >= 0
  final override def find(p: T => Boolean): Option[T] = {
    val index = indexWhere(p, 0)
    if (index == -1) None
    else Some(apply(index))
  }

  override def iterator: Iterator[T] = {
    new AbstractIterator[T] {
      var index = 0
      private val collSize = OptimusSeq.this.length
      override def size: Int = collSize - index
      override def knownSize: Int = size
      override def hasNext: Boolean = index < collSize

      override def drop(n: Int): Iterator[T] = {
        index = Math.min(index + n, collSize)
        this
      }
      override def next(): T = {
        val res = apply(index)
        index += 1
        res
      }
    }
  }
  override def reverseIterator: Iterator[T] = {
    new AbstractIterator[T] {
      override def size: Int = index + 1
      var index = OptimusSeq.this.length - 1
      override def knownSize: Int = size
      override def hasNext: Boolean = index >= 0

      override def drop(n: Int): Iterator[T] = {
        index = Math.max(index - n, 0)
        this
      }
      override def next(): T = {
        val res = apply(index)
        index -= 1
        res
      }
    }
  }

  override def head: T = apply(0)

  override def tail: OptimusSeq[T] = OptimusSeq.from(drop(1))

  override def last: T = apply(size - 1)

  override def zip[B](that: IterableOnce[B]): OptimusSeq[(T @uncheckedVariance, B)] = {
    if (isEmpty || that.knownSize == 0) {
      OptimusSeq.empty[(T, B)]
    } else {
      val builder = OptimusSeq.borrowBuilder[(T, B)]
      try {
        val size = length
        var i = 0
        that match {
          case is: IndexedSeq[B] =>
            val max = math.min(size, that.size)
            while (i < max) {
              builder += ((apply(i), is.apply(i)))
              i += 1
            }
          case _ =>
            val thatIt = that.iterator
            while (i < size && thatIt.hasNext) {
              builder += ((apply(i), thatIt.next()))
              i += 1
            }
        }
        builder.result()
      } finally OptimusBuilder.returnIfBorrowed(builder)
    }
  }

  override def zipAll[A1 >: T, B](that: Iterable[B], thisElem: A1, thatElem: B): OptimusSeq[(A1, B)] = {
    if (isEmpty && that.knownSize == 0) {
      OptimusSeq.empty[(A1, B)]
    } else {
      val builder = OptimusSeq.borrowBuilder[(A1, B)]
      try {
        val size = length
        var i = 0
        that match {
          case is: IndexedSeq[B] =>
            val thatSize = that.size
            val max = math.max(size, thatSize)
            while (i < max) {
              val a = if (i < size) apply(i) else thisElem
              val b = if (i < thatSize) is.apply(i) else thatElem
              builder += ((a, b))
              i += 1
            }
          case _ =>
            val thatIt = that.iterator
            while (i < size || thatIt.hasNext) {
              val a = if (i < size) apply(i) else thisElem
              val b = if (thatIt.hasNext) thatIt.next() else thatElem
              builder += ((a, b))
              i += 1
            }
        }
        builder.result()
      } finally OptimusBuilder.returnIfBorrowed(builder)
    }
  }

  override def zipWithIndex: OptimusSeq[(T, Int)] = {
    if (isEmpty) {
      OptimusSeq.empty[(T, Int)]
    } else {
      val builder = OptimusSeq.borrowBuilder[(T, Int)]
      try {
        val size = length
        var i = 0
        while (i < size) {
          builder += ((apply(i), i))
          i += 1
        }
        builder.result()
      } finally OptimusBuilder.returnIfBorrowed(builder)
    }
  }

  override def iterableFactory: SeqFactory[OptimusSeq] = OptimusSeq

  /**
   * Creates new builder for this collection
   */
  final def computeHash: Int = {
    MurmurHash3.indexedSeqHash(this, MurmurHash3.seqSeed)
  }

  /// additional methods

  def addAll[B >: T, That](that1: IterableOnce[B]): OptimusSeq[B] = {
    if (that1.knownSize == 0) this
    else
      OptimusSeq.withSharedBuilder[B] { builder =>
        builder ++= this
        builder ++= that1
      }
  }
  def addAll[B >: T, That](that1: IterableOnce[B], that2: IterableOnce[B]): OptimusSeq[B] = {
    if (that1.knownSize == 0) this
    else
      OptimusSeq.withSharedBuilder[B] { builder =>
        builder ++= this
        builder ++= that1
        builder ++= that2
      }
  }
  def addAll[B >: T, That](that1: IterableOnce[B], that2: IterableOnce[B], that3: IterableOnce[B]): OptimusSeq[B] = {
    if (that1.knownSize == 0) this
    else
      OptimusSeq.withSharedBuilder[B] { builder =>
        builder ++= this
        builder ++= that1
        builder ++= that2
        builder ++= that3
      }
  }
  def addAll[B >: T, That](
      that1: IterableOnce[B],
      that2: IterableOnce[B],
      that3: IterableOnce[B],
      that4: IterableOnce[B]): OptimusSeq[B] = {
    if (that1.knownSize == 0) this
    else
      OptimusSeq.withSharedBuilder[B] { builder =>
        builder ++= this
        builder ++= that1
        builder ++= that2
        builder ++= that3
        builder ++= that4
      }
  }
  def addAll[B >: T, That](
      that1: IterableOnce[B],
      that2: IterableOnce[B],
      that3: IterableOnce[B],
      that4: IterableOnce[B],
      rest: IterableOnce[B]*): OptimusSeq[B] = {
    if (
      that1.knownSize == 0 && that2.knownSize == 0 && that3.knownSize == 0 && that4.knownSize == 0 && rest.forall(
        _.knownSize == 0)
    ) this
    else
      OptimusSeq.withSharedBuilder[B] { builder =>
        builder ++= this
        builder ++= that1
        builder ++= that2
        builder ++= that3
        builder ++= that4
        rest foreach (builder ++= _)
      }
  }
  // extension methods
  /** similar to .zipWithIndex.map, but without the tupling */
  def mapWithIndex[B](f: (T, Int) => B): OptimusSeq[B]

  override protected[this] def className: String = "OptimusSeq"
}

private[collection] object OptimusSeqImpl extends OSeqImpl {
  private[this] def targetArrayExp = 6 // ie 64 elements
  def minArraySize = 2 << (targetArrayExp - 1)
  def maxArraySize = 2 << (targetArrayExp + 1)
}
