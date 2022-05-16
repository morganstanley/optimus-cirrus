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

import scala.collection.AbstractIterator
import scala.collection.GenTraversableOnce

/**
 * Iterator over two collections. The typical usage is `ZipIterator(c1, c2).map((x, y) => foo).toX`.
 *
 * In 2.12, one can write `(c1, c2).zipped.map((x, y) => foo)(breakOut)`.
 *
 * There is no `breakOut` in 2.13, so the equivalent is `c1.lazyZip(c2).iterator.map { case (x, y) => foo }.toX`. The
 * 2.13 version is more verbose and also builds and de-constructs a tuple for each element.
 */
class ZipIterator[T, U](ts: Iterator[T], us: Iterator[U]) extends AbstractIterator[(T, U)] { self =>
  override def hasNext: Boolean = ts.hasNext && us.hasNext
  override def next(): (T, U) = (ts.next(), us.next())

  def map[R](f: (T, U) => R): Iterator[R] = new AbstractIterator[R] {
    override def hasNext: Boolean = self.hasNext
    override def next(): R = f(ts.next(), us.next())
  }

  def flatMap[R](f: (T, U) => GenTraversableOnce[R]): Iterator[R] = new AbstractIterator[R] {
    private var cur: Iterator[R] = Iterator.empty
    private def nextCur(): Unit = { cur = null; cur = f(ts.next(), us.next()).toIterator }
    def hasNext: Boolean = {
      while (!cur.hasNext) {
        if (!self.hasNext) return false
        nextCur()
      }
      true
    }
    def next(): R = (if (hasNext) cur else Iterator.empty).next()
  }

  def filter(p: (T, U) => Boolean): Iterator[(T, U)] = new AbstractIterator[(T, U)] {
    private var hdT: T = _
    private var hdU: U = _
    private var hdDefined: Boolean = false

    def hasNext: Boolean = hdDefined || {
      do {
        if (!self.hasNext) return false
        hdT = ts.next()
        hdU = us.next()
      } while (!p(hdT, hdU))
      hdDefined = true
      true
    }

    def next(): (T, U) = if (hasNext) { hdDefined = false; (hdT, hdU) }
    else Iterator.empty.next()
  }
}
object ZipIterator {
  def apply[T, U](ts: TraversableOnce[T], us: TraversableOnce[U]): ZipIterator[T, U] =
    new ZipIterator(ts.toIterator, us.toIterator)
}

/**
 * Iterator over 3 collections. The typical usage is `Zip3Iterator(c1, c2, c3).map((x, y, z) => foo).toX`.
 *
 * In 2.12, one can write `(c1, c2, c3).zipped.map((x, y, z) => foo)(breakOut)`.
 *
 * There is no `breakOut` in 2.13, so the equivalent is `c1.lazyZip(c2).lazyZip(c3).iterator.map { case (x, y, z) => foo
 * }.toX`. The 2.13 version is more verbose and also builds and de-constructs a tuple for each element.
 */
class Zip3Iterator[T, U, V](ts: Iterator[T], us: Iterator[U], vs: Iterator[V]) extends AbstractIterator[(T, U, V)] {
  self =>
  override def hasNext: Boolean = ts.hasNext && us.hasNext && vs.hasNext
  override def next(): (T, U, V) = (ts.next(), us.next(), vs.next())

  def map[R](f: (T, U, V) => R): Iterator[R] = new AbstractIterator[R] {
    override def hasNext: Boolean = self.hasNext
    override def next(): R = f(ts.next(), us.next(), vs.next())
  }

  def flatMap[R](f: (T, U, V) => GenTraversableOnce[R]): Iterator[R] = new AbstractIterator[R] {
    private var cur: Iterator[R] = Iterator.empty
    private def nextCur(): Unit = { cur = null; cur = f(ts.next(), us.next(), vs.next()).toIterator }
    def hasNext: Boolean = {
      while (!cur.hasNext) {
        if (!self.hasNext) return false
        nextCur()
      }
      true
    }
    def next(): R = (if (hasNext) cur else Iterator.empty).next()
  }

  def filter(p: (T, U, V) => Boolean): Iterator[(T, U, V)] = new AbstractIterator[(T, U, V)] {
    private var hdT: T = _
    private var hdU: U = _
    private var hdV: V = _
    private var hdDefined: Boolean = false

    def hasNext: Boolean = hdDefined || {
      do {
        if (!self.hasNext) return false
        hdT = ts.next()
        hdU = us.next()
        hdV = vs.next()
      } while (!p(hdT, hdU, hdV))
      hdDefined = true
      true
    }

    def next(): (T, U, V) = if (hasNext) { hdDefined = false; (hdT, hdU, hdV) }
    else Iterator.empty.next()
  }
}
object Zip3Iterator {
  def apply[T, U, V](ts: TraversableOnce[T], us: TraversableOnce[U], vs: TraversableOnce[V]): Zip3Iterator[T, U, V] =
    new Zip3Iterator(ts.toIterator, us.toIterator, vs.toIterator)
}
