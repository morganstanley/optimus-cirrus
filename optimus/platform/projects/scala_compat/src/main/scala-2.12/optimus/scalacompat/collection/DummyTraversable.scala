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
package optimus.scalacompat.collection

import scala.collection.GenTraversableOnce
import scala.collection.TraversableView
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

trait DummyTraversable[A] extends Traversable[A] {
  override def isEmpty: Boolean = ???
  override def size: Int = ???
  override def hasDefiniteSize = ???
  override def ++[B >: A, That](xs: GenTraversableOnce[B])(implicit bf: CanBuildFrom[Traversable[A], B, That]): That =
    ???
  override def map[B, That](f: A => B)(implicit bf: CanBuildFrom[Traversable[A], B, That]): That = ???
  override def flatMap[B, That](f: A => GenTraversableOnce[B])(implicit
      bf: CanBuildFrom[Traversable[A], B, That]): That = ???
  override def filter(p: A => Boolean): Traversable[A] = ???
  override def partition(p: A => Boolean): (Traversable[A], Traversable[A]) = ???
  override def groupBy[K](f: A => K): Map[K, Traversable[A]] = ???
  override def foreach[U](f: A => U): Unit = ???
  override def forall(p: A => Boolean): Boolean = ???
  override def exists(p: A => Boolean): Boolean = ???
  override def count(p: A => Boolean): Int = ???
  override def find(p: A => Boolean): Option[A] = ???
  override def foldLeft[B](z: B)(op: (B, A) => B): B = ???
  override def /:[B](z: B)(op: (B, A) => B): B = ???
  override def foldRight[B](z: B)(op: (A, B) => B): B = ???
  override def :\[B](z: B)(op: (A, B) => B): B = ???
  override def reduceLeft[B >: A](op: (B, A) => B): B = ???
  override def reduceLeftOption[B >: A](op: (B, A) => B): Option[B] = ???
  override def reduceRight[B >: A](op: (A, B) => B): B = ???
  override def reduceRightOption[B >: A](op: (A, B) => B): Option[B] = ???
  override def head: A = ???
  override def headOption: Option[A] = ???
  override def tail: Traversable[A] = ???
  override def last: A = ???
  override def lastOption: Option[A] = ???
  override def init: Traversable[A] = ???
  override def take(n: Int): Traversable[A] = ???
  override def drop(n: Int): Traversable[A] = ???
  override def slice(from: Int, until: Int): Traversable[A] = ???
  override def takeWhile(p: A => Boolean): Traversable[A] = ???
  override def dropWhile(p: A => Boolean): Traversable[A] = ???
  override def span(p: A => Boolean): (Traversable[A], Traversable[A]) = ???
  override def splitAt(n: Int): (Traversable[A], Traversable[A]) = ???
  override def copyToBuffer[B >: A](dest: mutable.Buffer[B]) = ???
  override def copyToArray[B >: A](xs: Array[B], start: Int, len: Int) = ???
  override def copyToArray[B >: A](xs: Array[B], start: Int) = ???
  override def toArray[B >: A: ClassTag]: Array[B] = ???
  override def toList: List[A] = ???
  override def toIterable: Iterable[A] = ???
  override def toSeq: Seq[A] = ???
  override def toStream: Stream[A] = ???
  override def mkString(start: String, sep: String, end: String): String = ???
  override def mkString(sep: String): String = ???
  override def mkString: String = ???
  override def addString(b: StringBuilder, start: String, sep: String, end: String): StringBuilder = ???
  override def addString(b: StringBuilder, sep: String): StringBuilder = ???
  override def addString(b: StringBuilder): StringBuilder = ???
  override def toString = ???
  override def stringPrefix: String = ???
  override def view = ???
  override def view(from: Int, until: Int): TraversableView[A, Traversable[A]] = ???
}
