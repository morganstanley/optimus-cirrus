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
package optimus.platform.interop

import java.util.Comparator

import com.google.common.collect.UnmodifiableIterator

import scala.collection.immutable._
import scala.jdk.CollectionConverters._
import scala.collection.{Map => ScalaMap, Iterable => ScalaIterable, Set => ScalaSet}

object JavaInterop {
  def toScala[K, V](from: java.util.Map[K, V]) = Map(from.asScala.toSeq: _*)

  def toScala[T](from: UnmodifiableIterator[T]): scala.collection.Iterable[T] = from.asScala.toIterable

  def toScala[T](from: Comparator[T]): Ordering[T] = Ordering.comparatorToOrdering[T](from)

  def toScala[K <: java.lang.Comparable[K], V](from: java.util.SortedMap[K, V]) = SortedMap(from.asScala.toSeq: _*)

  def toScala[T](from: java.util.List[T]) = from.asScala

  def toScala[T](from: java.util.Set[T]) = from.asScala.toSet

  def toJava[K, V](from: ScalaMap[K, V]) = from.asJava

  def toJava[T](from: ScalaIterable[T]) = from.asJava

  def toJava[T](from: ScalaSet[T]) = from.asJava

  def none[T]: Option[T] = None

  def some[T](from: T): Option[T] = Some(from)

  def seq[T](elem: T): Seq[T] = Seq(elem)
}
