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

import java.util.Arrays
import java.util.Objects

import optimus.collection.OptimusSeq.collectMarker

import scala.collection.AbstractIterator
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

private[collection] object OptimusArraysSeq {
  def apply[T](arrays: Array[Array[AnyRef]], knownHashesOrNull: Array[Int]): OptimusArraysSeq[T] = {
    val offsets = {
      assert(arrays.length > 1)
      val res = new Array[Int](arrays.length)
      var i = 0
      var length = 0
      while (i < arrays.length) {
        //        assert(arrays(i).length <= maxArraySize)
        //        assert(arrays(i).length > 1)
        length += arrays(i).length
        res(i) = length
        i += 1
      }
      res
    }
    // it doesn't make sense for the hashes to be for all of the data
    assert((knownHashesOrNull eq null) || knownHashesOrNull.length < arrays.length)
    new OptimusArraysSeq[T](arrays, offsets, knownHashesOrNull)
  }
}
private[collection] final class OptimusArraysSeq[+T] private (
    private[collection] val arrays: Array[Array[AnyRef]],
    private[collection] val offsets: Array[Int],
    knownHashes: Array[Int])
    extends OptimusSeq[T] {
  assert(arrays ne null)
  assert(offsets ne null)
  assert(arrays.length == offsets.length)

  override def length = offsets(offsets.length - 1)

  // its really a lazy val, just reset on serialisation
  @transient private[this] var hashes = knownHashes
  // its racey, run hashes only transitions from null => array
  // should not really need to cop, as the builder should respect the immutability, but safer to not trust
  private[collection] def copyKnownHashes = if (hashes eq null) null else hashes.clone()

  override def apply(idx: Int): T = {
    if (idx < 0 || idx >= length)
      throw new IndexOutOfBoundsException(s"index : $idx")
    val target = Arrays.binarySearch(offsets, idx)
    val arrayIndex = if (target >= 0) target + 1 else -(target + 1)
    if (arrayIndex == 0) arrays(0)(idx).asInstanceOf[T]
    else arrays(arrayIndex)(idx - offsets(arrayIndex - 1)).asInstanceOf[T]
  }
  override def equals(other: Any): Boolean = {
    def compare( //
        d1: Array[Array[AnyRef]],
        d2: Array[Array[AnyRef]]): Boolean = {
      var d1Index, d1Offset, d2Index, d2Offset = 0
      var equal = true
      while (equal && d1Index < d1.length) {
        val d1d = d1(d1Index)
        val d2d = d2(d2Index)
        val max = Math.min(d1d.length - d1Offset, d2d.length - d2Offset)

        var i = 0
        while (equal && i < max) {
          equal = Objects.equals(d1d(d1Offset + i), d2d(d2Offset + i))
          i += 1
        }
        if (d1Offset + i == d1d.length) {
          d1Offset = 0;
          d1Index += 1
        } else d1Offset += i

        if (d2Offset + i == d2d.length) {
          d2Offset = 0;
          d2Index += 1
        } else d2Offset += i
      }
      equal
    }: Boolean
    other match {
      case that: OptimusArraySeq[_] =>
        length == that.length &&
        this.hashCode == that.hashCode &&
        compare(this.arrays, Array(that.data))
      case that: OptimusArraysSeq[_] =>
        (this eq that) || (length == that.length &&
          this.hashCode == that.hashCode &&
          compare(this.arrays, that.arrays))
      case small: OptimusSeq[_] =>
        small == this
      case _ =>
        super.equals(other)
    }
  }
  override def sameElements[B >: T](other: IterableOnce[B]): Boolean = {
    other match {
      case that: OptimusSeq[B] =>
        this == that
      case _ =>
        super.sameElements(other)
    }
  }

  override protected def filter0(f: T => Boolean, include: Boolean): OptimusSeq[T] = {
    val builder = OptimusSeq.borrowBuilder[T]
    try {
      var i = 0
      while (i < arrays.length) {
        var j = 0
        var all = true
        val array = arrays(i)
        while (j < array.length) {
          if (include == f(array(j).asInstanceOf[T])) {
            if (!all)
              builder += array(j).asInstanceOf[T]
          } else if (all) {
            builder.addFromArray(array, 0, j)
            all = false
          }
          j += 1
        }
        if (all)
          builder.addSharedArray(array)
        i += 1
      }
      if (builder.resultSize == length) this
      else builder.result()
    } finally builder.returnBorrowed()
  }

  override def foreach[U](f: T => U): Unit = {
    var i = 0
    while (i < arrays.length) {
      OptimusSeqSupport.foreach(arrays(i), f)
      i += 1
    }
  }

  override def forall(p: T => Boolean): Boolean = {
    var i = 0
    var all = true
    while (all && i < arrays.length) {
      all = OptimusSeqSupport.forall(arrays(i), p)
      i += 1
    }
    all
  }
  override def foldLeft[B](z: B)(op: (B, T) => B): B = {
    var i = 0
    var result = z
    while (i < arrays.length) {
      result = OptimusSeqSupport.foldLeft(arrays(i), result, op)
      i += 1
    }
    result
  }

  override def indexWhere(p: T => Boolean, from: Int): Int = {
    val idx = Math.max(0, from)
    val target = Arrays.binarySearch(offsets, idx)

    var i = if (target >= 0) target + 1 else -(target + 1)

    var offset = if (i == 0) 0 else offsets(i - 1)
    var startIndex = idx - offset

    var result = -1
    while (result == -1 && i < arrays.length) {
      result = OptimusSeqSupport.indexWhere(arrays(i), offset, startIndex, p)
      i += 1
      offset = offsets(i - 1)
      startIndex = 0
    }
    result
  }
  override def lastIndexWhere(p: T => Boolean, end: Int): Int = {
    val idx = Math.min(length - 1, end)
    val target = Arrays.binarySearch(offsets, idx)

    var i = if (target >= 0) target + 1 else -(target + 1)

    var offset = if (i == 0) 0 else offsets(i - 1)
    var endIndex = idx - offset

    var result = -1
    while (result == -1 && i >= 0) {
      result = OptimusSeqSupport.lastIndexWhere(arrays(i), offset, endIndex, p)
      i -= 1
      offset = if (i <= 0) 0 else offsets(i - 1)
      endIndex = Int.MaxValue
    }
    result
  }

  override def indexOf[B >: T](elem: B, from: Int): Int = {
    val idx = Math.max(0, from)
    val target = Arrays.binarySearch(offsets, idx)

    var i = if (target >= 0) target + 1 else -(target + 1)

    var offset = if (i == 0) 0 else offsets(i - 1)
    var startIndex = idx - offset

    var result = -1
    while (result == -1 && i < arrays.length) {
      result = OptimusSeqSupport.indexOf(arrays(i), offset, startIndex, elem)
      i += 1
      offset = offsets(i - 1)
      startIndex = 0
    }
    result
  }

  override def lastIndexOf[B >: T](elem: B, end: Int): Int = {
    val idx = Math.min(length - 1, end)
    val target = Arrays.binarySearch(offsets, idx)

    var i = if (target >= 0) target + 1 else -(target + 1)

    var offset = if (i == 0) 0 else offsets(i - 1)
    var endIndex = idx - offset

    var result = -1
    while (result == -1 && i >= 0) {
      result = OptimusSeqSupport.lastIndexOf(arrays(i), offset, endIndex, elem)
      i -= 1
      offset = if (i <= 0) 0 else offsets(i - 1)
      endIndex = Int.MaxValue
    }
    result
  }

  /**
   * @inheritdoc
   * optimised to avoid builder creation optimised to reduce allocation and return this if the result would == this
   * optimised to reduce allocation where one of the arrays transformed is identical (structural sharing) optimised to
   * maintain known partial hashes
   */
  override def map[B](f: T => B): OptimusArraysSeq[B] = {
    var newArrays: Array[Array[AnyRef]] = null
    var newKnownHashes: Array[Int] = null
    var i = 0
    while (i < arrays.length) {
      val data = arrays(i)
      var newData: Array[Any] = null
      var j = 0
      while (j < data.length) {
        val existing = data(j)
        val result = f(existing.asInstanceOf[T])
        if (newData eq null) {
          if (result.asInstanceOf[AnyRef] ne existing) {
            newData = new Array[Any](data.length)
            System.arraycopy(data, 0, newData, 0, j)
            newData(j) = result
            if (newArrays eq null) {
              newArrays = arrays.clone()
              if (i > 0 && hashes != null)
                newKnownHashes = Arrays.copyOf(hashes, Math.min(i, hashes.length))
            }
          }
        } else
          newData(j) = result
        j += 1
      }
      if (newArrays ne null)
        newArrays(i) = if (newData eq null) data else newData.asInstanceOf[Array[AnyRef]]
      i += 1
    }
    if (newArrays eq null) this.asInstanceOf[OptimusArraysSeq[B]]
    else new OptimusArraysSeq(newArrays, offsets, newKnownHashes).asInstanceOf[OptimusArraysSeq[B]]
  }

  override def copyToArray[B >: T](xs: Array[B], start: Int, len: Int): Int = {
    if (xs.getClass.getComponentType.isPrimitive) super.copyToArray(xs, start, len)
    else {
      var outputOffset = start
      val actualLen = Math.min(length, Math.max(0, len))
      var remaining = actualLen
      var index = 0
      while (remaining > 0) {
        val toCopy = Math.min(arrays(index).length, remaining)
        System.arraycopy(arrays(index), 0, xs, outputOffset, toCopy)
        outputOffset += toCopy
        remaining -= toCopy
        index += 1
      }
      actualLen
    }
  }

  override def toArray[B >: T](implicit evidence$1: ClassTag[B]): Array[B] = {
    if (evidence$1.runtimeClass.isPrimitive) super.toArray
    else {
      val res = new Array[B](length)
      copyToArray(res, 0, length)
      res
    }
  }
  // lo and hi are sensible and always not everything
  // hi is exclusive
  private[collection] def sliceSafe(lo: Int, hi: Int): OptimusSeq[T] = {
    val from_target = Arrays.binarySearch(offsets, lo + 1)

    val to_target = Arrays.binarySearch(offsets, hi)

    if (from_target >= 0 && to_target == from_target + 1) {
      // special case - its the whole of an existing array
      val partialHash = if (from_target == 0 && hashes != null) hashes(0) else 0
      OptimusArraySeq.unsafeFromArray[T](arrays(from_target), partialHash)
    } else {
      OptimusSeq.withSharedBuilder[T] { b =>
        var inputArrayIndex = if (from_target >= 0) from_target else -(from_target + 1)
        var inputCopyStart = if (inputArrayIndex == 0) lo else (lo - offsets(inputArrayIndex - 1))
        var toCopy = hi - lo
        while (toCopy > 0) {

          // share the array if we can
          if (inputCopyStart == 0 && toCopy >= arrays(inputArrayIndex).length) {
            b.addSharedArray(arrays(inputArrayIndex))
            toCopy -= arrays(inputArrayIndex).length
          } else {
            val end = Math.min(inputCopyStart + toCopy, arrays(inputArrayIndex).length)
            b.addFromArray(arrays(inputArrayIndex), inputCopyStart, end)
            toCopy -= end - inputCopyStart
          }
          inputCopyStart = 0
          inputArrayIndex += 1
        }
      }
    }
  }
  override def appended[B >: T](that: B): OptimusArraysSeq[B] = {
    OptimusSeq
      .withSharedBuilder[B] { builder =>
        builder ++= this
        builder += that
      }
      .asInstanceOf[OptimusArraysSeq[B]]
  }

  override def prepended[B >: T](that: B): OptimusArraysSeq[B] = {
    OptimusSeq
      .withSharedBuilder[B] { builder =>
        builder += that
        builder ++= this
      }
      .asInstanceOf[OptimusArraysSeq[B]]
  }

  override def head: T = arrays(0)(0).asInstanceOf[T]

  override def last: T = {
    val lastArray = arrays(arrays.length - 1)
    lastArray(lastArray.length - 1).asInstanceOf[T]
  }

  override def iterator: Iterator[T] = {
    new AbstractIterator[T] {
      var arrayIndex = 0
      while (arrayIndex < arrays.length && arrays(arrayIndex).length == 0) arrayIndex += 1
      var currentArray: Array[AnyRef] = if (arrayIndex >= arrays.length) null else arrays(arrayIndex)

      var indexInArray = 0
      override def hasNext: Boolean = currentArray ne null
      override def next(): T = {
        if (!hasNext) throw new NoSuchElementException
        val res = currentArray(indexInArray).asInstanceOf[T]
        indexInArray += 1
        if (indexInArray >= currentArray.length) {
          indexInArray = 0
          arrayIndex += 1
          if (arrayIndex >= arrays.length) currentArray = null
          else currentArray = arrays(arrayIndex)
        }
        res
      }
    }
  }

  override def reverseIterator: Iterator[T] = {
    new Iterator[T] {
      var arrayIndex = arrays.length - 1
      while (arrayIndex > 0 && arrays(arrayIndex).length == 0) arrayIndex -= 1
      var currentArray: Array[AnyRef] = if (arrayIndex <= 0) null else arrays(arrayIndex)
      var indexInArray = currentArray.length - 1
      override def hasNext: Boolean = currentArray ne null
      override def next(): T = {
        if (!hasNext) throw new NoSuchElementException
        val res = currentArray(indexInArray).asInstanceOf[T]
        indexInArray -= 1
        if (indexInArray < 0) {
          arrayIndex -= 1
          if (arrayIndex < 0) currentArray = null
          else {
            currentArray = arrays(arrayIndex)
            indexInArray = currentArray.length - 1
          }
        }
        res
      }
    }
  }

  override def collectFirst[B](pf: PartialFunction[T, B]): Option[B] = {
    val marker = collectMarker[T, B]
    var arrayIndex = 0
    while (arrayIndex < arrays.length) {
      val currentArray: Array[AnyRef] = arrays(arrayIndex)
      var indexInArray = 0
      while (indexInArray < currentArray.length) {
        val result = pf.applyOrElse(currentArray(indexInArray).asInstanceOf[T], marker)
        if (result.asInstanceOf[AnyRef] ne marker)
          return Some(result)
        indexInArray += 1
      }
      arrayIndex += 1
    }
    None
  }

  override def collect[B](pf: PartialFunction[T, B]): OptimusSeq[B] = {
    OptimusSeq
      .withSharedBuilder[B] { b =>
        val marker = collectMarker[T, B]
        var arrayIndex = 0
        while (arrayIndex < arrays.length) {
          val currentArray: Array[AnyRef] = arrays(arrayIndex)
          var indexInArray = 0
          while (indexInArray < currentArray.length) {
            val result = pf.applyOrElse(currentArray(indexInArray).asInstanceOf[T], marker)
            if (result.asInstanceOf[AnyRef] ne marker)
              b += result
            indexInArray += 1
          }
          arrayIndex += 1
        }
      }
  }
  // extension methods
  /** similar to .zipWithIndex.map, but without the tupling */
  override def mapWithIndex[B](f: (T, Int) => B): OptimusSeq[B] = {
    val builder = OptimusSeq.borrowBuilder[B]
    try {
      builder.sizeHint(this)
      val length = size
      var i = 0
      var identical = true
      while (i < length) {
        val existing = apply(i)
        val transformed = f(existing, i)
        builder += transformed
        identical &= (transformed == existing)
        i += 1
      }
      if (identical)
        this.asInstanceOf[OptimusSeq[B]]
      else
        builder.result()
    } finally builder.returnBorrowed()
  }
}
