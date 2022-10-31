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
package optimus.scalacompat.collection.javaapi

import java.util.{concurrent => juc}
import java.{util => ju}
import java.{lang => jl}
import scala.collection.mutable
import scala.jdk.javaapi.{CollectionConverters => CC}
import scala.{collection => sc}

// once on 2.13 only, switch to scala.jdk.javaapi.CollectionConverters
object CollectionConverters {
  def asJavaIterator[A](i: Iterator[A]): ju.Iterator[A] = CC.asJava(i)
  def asJavaEnumeration[A](i: Iterator[A]): ju.Enumeration[A] = CC.asJavaEnumeration(i)
  def asJavaIterable[A](i: Iterable[A]): jl.Iterable[A] = CC.asJava(i)
  def asJavaCollection[A](i: Iterable[A]): ju.Collection[A] = CC.asJavaCollection(i)
  def bufferAsJavaList[A](b: mutable.Buffer[A]): ju.List[A] = CC.asJava(b)
  def mutableSeqAsJavaList[A](s: mutable.Seq[A]): ju.List[A] = CC.asJava(s)
  def seqAsJavaList[A](s: sc.Seq[A]): ju.List[A] = CC.asJava(s)
  def mutableSetAsJavaSet[A](s: mutable.Set[A]): ju.Set[A] = CC.asJava(s)
  def setAsJavaSet[A](s: sc.Set[A]): ju.Set[A] = CC.asJava(s)
  def mutableMapAsJavaMap[A, B](m: mutable.Map[A, B]): ju.Map[A, B] = CC.asJava(m)
  def asJavaDictionary[A, B](m: mutable.Map[A, B]): ju.Dictionary[A, B] = CC.asJavaDictionary(m)
  def mapAsJavaMap[A, B](m: sc.Map[A, B]): ju.Map[A, B] = CC.asJava(m)
  def mapAsJavaConcurrentMap[A, B](m: sc.concurrent.Map[A, B]): juc.ConcurrentMap[A, B] = CC.asJava(m)

  def asScalaIterator[A](i: ju.Iterator[A]): Iterator[A] = CC.asScala(i)
  def enumerationAsScalaIterator[A](i: ju.Enumeration[A]): Iterator[A] = CC.asScala(i)
  def iterableAsScalaIterable[A](i: jl.Iterable[A]): Iterable[A] = CC.asScala(i)
  def collectionAsScalaIterable[A](i: ju.Collection[A]): Iterable[A] = CC.asScala(i)
  def asScalaBuffer[A](l: ju.List[A]): mutable.Buffer[A] = CC.asScala(l)
  def asScalaSet[A](s: ju.Set[A]): mutable.Set[A] = CC.asScala(s)
  def mapAsScalaMap[A, B](m: ju.Map[A, B]): mutable.Map[A, B] = CC.asScala(m)
  def mapAsScalaConcurrentMap[A, B](m: juc.ConcurrentMap[A, B]): sc.concurrent.Map[A, B] = CC.asScala(m)
  def dictionaryAsScalaMap[A, B](p: ju.Dictionary[A, B]): mutable.Map[A, B] = CC.asScala(p)
  def propertiesAsScalaMap(p: ju.Properties): mutable.Map[String, String] = CC.asScala(p)
}
