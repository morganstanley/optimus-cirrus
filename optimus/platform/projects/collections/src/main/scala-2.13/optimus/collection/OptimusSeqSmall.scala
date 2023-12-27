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

import scala.annotation.switch
import scala.util.hashing.MurmurHash3

private[collection] object OptimusSeqEmpty extends OptimusSeq[Nothing] {
  override def length: Int = 0
  override def apply(idx: Int): Nothing = throw new IndexOutOfBoundsException(s"index $idx")
  override def indexOf[B >: Nothing](elem: B, from: Int): Int = -1
  override def lastIndexOf[B >: Nothing](elem: B, end: Int): Int = -1
  override def indexWhere(p: Nothing => Boolean, from: Int): Int = -1
  override def lastIndexWhere(p: Nothing => Boolean, end: Int): Int = -1
  override def map[B](f: Nothing => B): OptimusSeqEmpty.type = this
  override def flatMap[B](f: Nothing => IterableOnce[B]): OptimusSeqEmpty.type = this

  override def foreach[U](f: Nothing => U): Unit = ()
  override def forall(p: Nothing => Boolean): Boolean = true
  override def appended[B >: Nothing](that: B): OptimusSeq[B] =
    new OptimusSeq1(that)
  override def prepended[B >: Nothing](that: B): OptimusSeq[B] =
    new OptimusSeq1(that)
  override protected def filter0(f: Nothing => Boolean, include: Boolean): OptimusSeq[Nothing] = this
  override def iterator: Iterator[Nothing] = Iterator.empty
  override def reverseIterator: Iterator[Nothing] = Iterator.empty
  override def collectFirst[B](pf: PartialFunction[Nothing, B]): Option[B] = None
  override def collect[B](pf: PartialFunction[Nothing, B]): OptimusSeqEmpty.type = this
  override def head: Nothing = Iterator.empty.next()
  override def tail: OptimusSeq[Nothing] = Iterator.empty.next()
  override def last: Nothing = Iterator.empty.next()
  // extension methods
  override def mapWithIndex[B](f: (Nothing, Int) => B): OptimusSeq[B] = {
    this.asInstanceOf[OptimusSeq[B]]
  }
}
private[collection] sealed abstract class SmallOptimusSeq[+T] extends OptimusSeq[T] {
  private[collection] def copyToBuilderArray(elems: Array[AnyRef], startIndex: Int): Unit
  override def equals(that: Any): Boolean = that match {
    case that: OptimusSeq[_] => {
      this.length == that.length &&
      this.hashCode == that.hashCode && {
        var i = length - 1
        while (i >= 0) {
          if (this(i) != that(i))
            return false
          i -= 1
        }
        true
      }
    }
    case _ => super.equals(that)
  }
  override def collectFirst[B](pf: PartialFunction[T, B]): Option[B] = {
    val max = size
    var index = 0
    val marker = collectMarker[T, B]
    while (index < max) {
      val result = pf.applyOrElse(apply(index), marker)
      if (result.asInstanceOf[AnyRef] ne marker)
        return Some(result)
      index += 1
    }
    None
  }

  override def collect[B](pf: PartialFunction[T, B]): OptimusSeq[B] = {
    OptimusSeq.withSharedBuilder[B] { b =>
      val max = size
      var index = 0
      val marker = collectMarker[T, B]
      while (index < max) {
        val result = pf.applyOrElse(apply(index), marker)
        if (result.asInstanceOf[AnyRef] ne marker)
          b += result
        index += 1
      }
    }
  }

}
private[collection] class OptimusSeq1[+T](v1: T) extends SmallOptimusSeq[T] {
  override def length: Int = 1
  override def apply(idx: Int): T = if (idx == 0) v1 else throw new IndexOutOfBoundsException(s"index $idx")
  override private[collection] def copyToBuilderArray(elems: Array[AnyRef], startIndex: Int): Unit = {
    elems(startIndex) = v1.asInstanceOf[AnyRef]
  }
  override def indexOf[B >: T](elem: B, from: Int): Int = {
    if (from <= 0 && elem == v1) 0
    else -1
  }
  override def lastIndexOf[B >: T](elem: B, end: Int): Int = {
    if (end >= 0 && elem == v1) 0
    else -1
  }
  override def indexWhere(p: T => Boolean, from: Int): Int = {
    if (from <= 0 && p(v1)) 0
    else -1
  }
  override def lastIndexWhere(p: T => Boolean, end: Int): Int = {
    if (end >= 0 && p(v1)) 0
    else -1
  }

  override def map[B](f: T => B): OptimusSeq[B] = {
    val n1 = f(v1)
    if (n1.asInstanceOf[AnyRef] eq v1.asInstanceOf[AnyRef])
      this.asInstanceOf[OptimusSeq1[B]]
    else new OptimusSeq1(n1)
  }
  override def foreach[U](f: T => U): Unit = {
    f(v1)
  }
  override def appended[B >: T](that: B): OptimusSeq[B] =
    new OptimusSeq2(v1, that)
  override def prepended[B >: T](that: B): OptimusSeq[B] =
    new OptimusSeq2(that, v1)
  override def iterator: Iterator[T] = Iterator.single(v1)
  override def reverseIterator: Iterator[T] = Iterator.single(v1)
  override def head: T = v1
  override def tail: OptimusSeq[T] = OptimusSeq.empty
  override def last: T = v1
  // extension methods
  override def mapWithIndex[B](f: (T, Int) => B): OptimusSeq[B] = {
    val n1 = f(v1, 0)
    if (n1 == v1)
      this.asInstanceOf[OptimusSeq[B]]
    else
      new OptimusSeq1[B](n1)
  }
}
private[collection] class OptimusSeq2[+T](v1: T, v2: T) extends SmallOptimusSeq[T] {
  override def length: Int = 2
  override def apply(idx: Int): T = (idx: @switch) match {
    case 0 => v1
    case 1 => v2
    case _ => throw new IndexOutOfBoundsException(s"index $idx")
  }

  override private[collection] def copyToBuilderArray(elems: Array[AnyRef], startIndex: Int): Unit = {
    elems(startIndex) = v1.asInstanceOf[AnyRef]
    elems(startIndex + 1) = v2.asInstanceOf[AnyRef]
  }
  override def indexOf[B >: T](elem: B, from: Int): Int = {
    if (from <= 0 && elem == v1) 0
    else if (from <= 1 && elem == v2) 1
    else -1
  }
  override def lastIndexOf[B >: T](elem: B, end: Int): Int = {
    if (end >= 1 && elem == v2) 1
    else if (end >= 0 && elem == v1) 0
    else -1
  }
  override def indexWhere(p: T => Boolean, from: Int): Int = {
    if (from <= 0 && p(v1)) 0
    else if (from <= 1 && p(v2)) 1
    else -1
  }
  override def lastIndexWhere(p: T => Boolean, end: Int): Int = {
    if (end >= 1 && p(v2)) 1
    else if (end >= 0 && p(v1)) 0
    else -1
  }

  override def map[B](f: T => B): OptimusSeq[B] = {
    val n1 = f(v1)
    val n2 = f(v2)
    if (
      (n1.asInstanceOf[AnyRef] eq v1.asInstanceOf[AnyRef]) &&
      (n2.asInstanceOf[AnyRef] eq v2.asInstanceOf[AnyRef])
    )
      this.asInstanceOf[OptimusSeq[B]]
    else new OptimusSeq2(n1, n2)
  }
  override def foreach[U](f: T => U): Unit = {
    f(v1)
    f(v2)
  }
  override def appended[B >: T](that: B): OptimusSeq[B] =
    new OptimusSeq3(v1, v2, that)
  override def prepended[B >: T](that: B): OptimusSeq[B] =
    new OptimusSeq3(that, v1, v2)
  // extension methods
  override def mapWithIndex[B](f: (T, Int) => B): OptimusSeq[B] = {
    val n1 = f(v1, 0)
    val n2 = f(v2, 1)

    if (n1 == v1 && n2 == v2)
      this.asInstanceOf[OptimusSeq[B]]
    else
      new OptimusSeq2[B](n1, n2)
  }
}
private[collection] class OptimusSeq3[+T](v1: T, v2: T, v3: T) extends SmallOptimusSeq[T] {
  override def length: Int = 3
  override def apply(idx: Int): T = (idx: @switch) match {
    case 0 => v1
    case 1 => v2
    case 2 => v3
    case _ => throw new IndexOutOfBoundsException(s"index $idx")
  }

  override private[collection] def copyToBuilderArray(elems: Array[AnyRef], startIndex: Int): Unit = {
    elems(startIndex) = v1.asInstanceOf[AnyRef]
    elems(startIndex + 1) = v2.asInstanceOf[AnyRef]
    elems(startIndex + 2) = v3.asInstanceOf[AnyRef]
  }
  override def indexOf[B >: T](elem: B, from: Int): Int = {
    if (from <= 0 && elem == v1) 0
    else if (from <= 1 && elem == v2) 1
    else if (from <= 2 && elem == v3) 2
    else -1
  }
  override def lastIndexOf[B >: T](elem: B, end: Int): Int = {
    if (end >= 2 && elem == v3) 2
    else if (end >= 1 && elem == v2) 1
    else if (end >= 0 && elem == v1) 0
    else -1
  }
  override def indexWhere(p: T => Boolean, from: Int): Int = {
    if (from <= 0 && p(v1)) 0
    else if (from <= 1 && p(v2)) 1
    else if (from <= 2 && p(v3)) 2
    else -1
  }
  override def lastIndexWhere(p: T => Boolean, end: Int): Int = {
    if (end >= 2 && p(v3)) 2
    else if (end >= 1 && p(v2)) 1
    else if (end >= 0 && p(v1)) 0
    else -1
  }

  override def map[B](f: T => B): OptimusSeq[B] = {
    val n1 = f(v1)
    val n2 = f(v2)
    val n3 = f(v3)
    if (
      (n1.asInstanceOf[AnyRef] eq v1.asInstanceOf[AnyRef]) &&
      (n2.asInstanceOf[AnyRef] eq v2.asInstanceOf[AnyRef]) &&
      (n3.asInstanceOf[AnyRef] eq v3.asInstanceOf[AnyRef])
    )
      this.asInstanceOf[OptimusSeq[B]]
    else new OptimusSeq3(n1, n2, n3)
  }
  override def foreach[U](f: T => U): Unit = {
    f(v1)
    f(v2)
    f(v3)
  }
  override def appended[B >: T](that: B): OptimusSeq[B] =
    new OptimusSeq4(v1, v2, v3, that)
  override def prepended[B >: T](that: B): OptimusSeq[B] =
    new OptimusSeq4(that, v1, v2, v3)
  // extension methods
  override def mapWithIndex[B](f: (T, Int) => B): OptimusSeq[B] = {
    val n1 = f(v1, 0)
    val n2 = f(v2, 1)
    val n3 = f(v3, 2)

    if (n1 == v1 && n2 == v2 && n3 == v3)
      this.asInstanceOf[OptimusSeq[B]]
    else
      new OptimusSeq3[B](n1, n2, n3)
  }
}

private[collection] class OptimusSeq4[+T](v1: T, v2: T, v3: T, v4: T) extends SmallOptimusSeq[T] {
  override def length: Int = 4
  override def apply(idx: Int): T = (idx: @switch) match {
    case 0 => v1
    case 1 => v2
    case 2 => v3
    case 3 => v4
    case _ => throw new IndexOutOfBoundsException(s"index $idx")
  }
  override private[collection] def copyToBuilderArray(elems: Array[AnyRef], startIndex: Int): Unit = {
    elems(startIndex) = v1.asInstanceOf[AnyRef]
    elems(startIndex + 1) = v2.asInstanceOf[AnyRef]
    elems(startIndex + 2) = v3.asInstanceOf[AnyRef]
    elems(startIndex + 3) = v4.asInstanceOf[AnyRef]
  }
  override def indexOf[B >: T](elem: B, from: Int): Int = {
    if (from <= 0 && elem == v1) 0
    else if (from <= 1 && elem == v2) 1
    else if (from <= 2 && elem == v3) 2
    else if (from <= 3 && elem == v4) 3
    else -1
  }
  override def lastIndexOf[B >: T](elem: B, end: Int): Int = {
    if (end >= 3 && elem == v4) 3
    else if (end >= 2 && elem == v3) 2
    else if (end >= 1 && elem == v2) 1
    else if (end >= 0 && elem == v1) 0
    else -1
  }
  override def indexWhere(p: T => Boolean, from: Int): Int = {
    if (from <= 0 && p(v1)) 0
    else if (from <= 1 && p(v2)) 1
    else if (from <= 2 && p(v3)) 2
    else if (from <= 3 && p(v4)) 3
    else -1
  }
  override def lastIndexWhere(p: T => Boolean, end: Int): Int = {
    if (end >= 3 && p(v4)) 3
    else if (end >= 2 && p(v3)) 2
    else if (end >= 1 && p(v2)) 1
    else if (end >= 0 && p(v1)) 0
    else -1
  }

  override def map[B](f: T => B): OptimusSeq[B] = {
    val n1 = f(v1)
    val n2 = f(v2)
    val n3 = f(v3)
    val n4 = f(v4)
    if (
      (n1.asInstanceOf[AnyRef] eq v1.asInstanceOf[AnyRef]) &&
      (n2.asInstanceOf[AnyRef] eq v2.asInstanceOf[AnyRef]) &&
      (n3.asInstanceOf[AnyRef] eq v3.asInstanceOf[AnyRef]) &&
      (n4.asInstanceOf[AnyRef] eq v4.asInstanceOf[AnyRef])
    )
      this.asInstanceOf[OptimusSeq[B]]
    else new OptimusSeq4(n1, n2, n3, n4)
  }

  override def foreach[U](f: T => U): Unit = {
    f(v1)
    f(v2)
    f(v3)
    f(v4)
  }
  override def appended[B >: T](that: B): OptimusSeq[B] =
    OptimusArraySeq.unsafeFromAnyArray(Array[Any](v1, v2, v3, v4, that)).asInstanceOf[OptimusSeq[B]]
  override def prepended[B >: T](that: B): OptimusSeq[B] =
    OptimusArraySeq.unsafeFromAnyArray(Array[Any](that, v1, v2, v3, v4)).asInstanceOf[OptimusSeq[B]]
  // extension methods
  override def mapWithIndex[B](f: (T, Int) => B): OptimusSeq[B] = {
    val n1 = f(v1, 0)
    val n2 = f(v2, 1)
    val n3 = f(v3, 2)
    val n4 = f(v4, 3)

    if (n1 == v1 && n2 == v2 && n3 == v3 && n4 == v4)
      this.asInstanceOf[OptimusSeq[B]]
    else
      new OptimusSeq4[B](n1, n2, n3, n4)
  }
}
