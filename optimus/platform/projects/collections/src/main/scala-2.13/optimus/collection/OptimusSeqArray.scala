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

import optimus.collection.OptimusSeq.collectMarker

import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

private[collection] object OptimusArraySeq {
  private[collection] def unsafeFromArray[T](elems: Array[AnyRef], knownPartialHashCode: Int = 0): OptimusSeq[T] =
    unsafeFromAnyArray(elems.asInstanceOf[Array[Any]])
  private[collection] def unsafeFromAnyArray[T](elems: Array[Any], knownPartialHashCode: Int = 0): OptimusSeq[T] =
    if (elems.length == 0) OptimusSeq.empty[T]
    else new OptimusArraySeq(elems.asInstanceOf[Array[AnyRef]], knownPartialHashCode)
}
private[collection] final class OptimusArraySeq[+T] private (
    private[collection] val data: Array[AnyRef],
    knownPartialHashCode: Int)
    extends OptimusSeq[T] {

  override def length: Int = data.length
  override def apply(idx: Int): T = data(idx).asInstanceOf[T]

  @volatile @transient var partialHashcode = knownPartialHashCode

  override def equals(other: Any): Boolean = other match {
    case that: OptimusArraySeq[_] =>
      (this eq that) || (
        data.length == that.data.length &&
          this.hashCode == that.hashCode && {
            var offset = 0
            var equal = true
            while (equal && offset < data.length) {
              val d1 = this.data(offset)
              val d2 = that.data(offset)
              equal = (d1 eq d2) || d1 == d2
              offset += 1
            }
            equal
          }
      )
    case that: OptimusArraysSeq[_] =>
      // OptimusArraysSeq can the more complex implemtation, so it seems sensible to have it in one place only
      that == this
    case that: OptimusSeq[_] => false
    case _                   => super.equals(other)
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
    var builder: OptimusBuilder[T, OptimusSeq[T]] = null
    try {
      var i = 0
      // all == true => builder == null && we can reuse the collection
      // all == false && builder == null => return is empty
      // all == false  && builder ne null => use content of the builder
      var all = true
      while (i < data.length && all) {
        // we are keeping all values currently
        val value = data(i).asInstanceOf[T]
        if (include != f(value)) {
          if (i != 0) {
            builder = OptimusSeq.borrowBuilder[T]
            builder.addFromArray(data, 0, i)
          }
          all = false
        }
        i += 1
      }
      while (i < data.length && (builder eq null)) {
        // we are keeping no values currently
        val value = data(i).asInstanceOf[T]
        if (include == f(value)) {
          builder = OptimusSeq.borrowBuilder[T]
          builder += value
        }
        i += 1
      }
      while (i < data.length && (builder ne null)) {
        // we are keeping values in the builder
        val value = data(i).asInstanceOf[T]
        if (include == f(value))
          builder += data(i).asInstanceOf[T]
        i += 1
      }
      if (all) this
      else if (builder eq null) OptimusSeq.empty
      else builder.result()
    } finally if (builder ne null) builder.returnBorrowed()
  }

  override def foreach[U](f: T => U): Unit = OptimusSeqSupport.foreach(data, f)
  override def forall(p: T => Boolean): Boolean = OptimusSeqSupport.forall(data, p)
  override def foldLeft[B](z: B)(op: (B, T) => B): B = OptimusSeqSupport.foldLeft(data, z, op)
  override def indexWhere(p: T => Boolean, from: Int): Int = OptimusSeqSupport.indexWhere(data, 0, Math.max(0, from), p)
  override def lastIndexWhere(p: T => Boolean, end: Int): Int =
    OptimusSeqSupport.lastIndexWhere(data, 0, Math.min(length - 1, end), p)
  override def indexOf[B >: T](elem: B, from: Int): Int = OptimusSeqSupport.indexOf(data, 0, Math.max(0, from), elem)
  override def lastIndexOf[B >: T](elem: B, end: Int): Int =
    OptimusSeqSupport.lastIndexOf(data, 0, Math.min(length - 1, end), elem)

  /**
   * @inheritdoc
   * optimised to avoid builder creation optimised to zero allocation if the result isempty optimised to zero allocation
   * and return this if the result would == this
   */
  override def map[B](f: T => B): OptimusArraySeq[B] = {
    var newData: Array[Any] = null
    var i = 0
    while (i < data.length && (newData eq null)) {
      val existing = data(i)
      val result = f(existing.asInstanceOf[T])
      if (result.asInstanceOf[AnyRef] ne existing) {
        newData = new Array[Any](data.length)
        System.arraycopy(data, 0, newData, 0, i)
        newData(i) = result
      }
      i += 1
    }
    // mop up the other values
    while (i < data.length) {
      val existing = data(i)
      val result = f(existing.asInstanceOf[T])
      newData(i) = result
      i += 1
    }
    if (newData eq null) this.asInstanceOf[OptimusArraySeq[B]]
    else OptimusArraySeq.unsafeFromAnyArray(newData).asInstanceOf[OptimusArraySeq[B]]
  }
  override def copyToArray[B >: T](xs: Array[B], start: Int, len: Int): Int = {
    if (xs.getClass.getComponentType.isPrimitive) super.copyToArray(xs, start, len)
    else {
      val n = Math.max(0, Math.min(xs.length - start, Math.min(len, data.length)))
      System.arraycopy(data, 0, xs, start, n)
      n
    }

  }

  override def toArray[B >: T](implicit evidence$1: ClassTag[B]): Array[B] = {
    if (evidence$1.runtimeClass.isPrimitive) super.toArray
    else {
      val res = new Array[B](length)
      System.arraycopy(data, 0, res, 0, length)
      res
    }
  }
  override def appended[B >: T](that: B): OptimusSeq[B] = {
    OptimusSeq
      .withSharedBuilder[B] { builder =>
        builder ++= this
        builder += that
      }
  }

  override def prepended[B >: T](that: B): OptimusSeq[B] = {
    OptimusSeq
      .withSharedBuilder[B] { builder =>
        builder += that
        builder ++= this
      }
  }

  override def iterator: Iterator[T] = {
    new Iterator[T] {
      var index = 0
      override def hasNext: Boolean = index < data.length
      override def next(): T = {
        val res = data(index).asInstanceOf[T]
        index += 1
        res
      }
    }
  }
  override def reverseIterator: Iterator[T] = {
    new Iterator[T] {
      var index = data.length - 1
      override def hasNext: Boolean = index >= 0
      override def next(): T = {
        val res = data(index).asInstanceOf[T]
        index -= 1
        res
      }
    }
  }

  override def head: T = data(0).asInstanceOf[T]

  override def last: T = data(data.length - 1).asInstanceOf[T]

  override def collectFirst[B](pf: PartialFunction[T, B]): Option[B] = {
    var index = 0
    val marker = collectMarker[T, B]
    while (index < data.length) {
      val result = pf.applyOrElse(data(index).asInstanceOf[T], marker)
      if (result.asInstanceOf[AnyRef] ne marker)
        return Some(result)
      index += 1
    }
    None
  }

  override def collect[B](pf: PartialFunction[T, B]): OptimusSeq[B] = {
    OptimusSeq
      .withSharedBuilder[B] { b =>
        var index = 0
        val marker = collectMarker[T, B]
        while (index < data.length) {
          val result = pf.applyOrElse(data(index).asInstanceOf[T], marker)
          if (result.asInstanceOf[AnyRef] ne marker)
            b += result
          index += 1
        }
      }
  }
  // extension methods
  override def mapWithIndex[B](f: (T, Int) => B): OptimusSeq[B] = {
    var newData: Array[Any] = null
    var i = 0
    while (i < data.length && (newData eq null)) {
      val existing = data(i)
      val result = f(existing.asInstanceOf[T], i)
      if (result != existing) {
        newData = new Array[Any](data.length)
        System.arraycopy(data, 0, newData, 0, i)
        newData(i) = result
      }
      i += 1
    }
    // work though the rest of the values.
    // if we enter the loop newData ne null
    while (i < data.length) {
      val existing = data(i)
      val result = f(existing.asInstanceOf[T], i)
      newData(i) = result
      i += 1
    }
    if (newData eq null) this.asInstanceOf[OptimusSeq[B]]
    else OptimusArraySeq.unsafeFromAnyArray(newData).asInstanceOf[OptimusSeq[B]]
  }
}
