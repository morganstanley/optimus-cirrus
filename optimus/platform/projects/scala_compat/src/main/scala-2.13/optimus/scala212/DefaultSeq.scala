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
package optimus.scala212

import optimus.scalacompat.collection.UnsafeImmutableBufferWrapper

import scala.annotation.implicitNotFound
import scala.collection.convert.AsScalaExtensions
import scala.{collection => sc}

object DefaultSeq extends optimus.scalacompat.collection.MapBuildFromImplicits {
  type Seq[+A] = sc.Seq[A]
  val Seq: sc.Seq.type = sc.Seq
  type IndexedSeq[+A] = sc.IndexedSeq[A]
  val IndexedSeq: sc.IndexedSeq.type = sc.IndexedSeq
  def toVarargsSeq[A](s: sc.Seq[A]): scala.collection.immutable.Seq[A] = s match {
    case is: scala.collection.immutable.Seq[a] => is.asInstanceOf[scala.collection.immutable.Seq[A]]
    case _                                     => s.toSeq
  }
  def toVarargsSeq[A](s: Array[A]): scala.collection.immutable.Seq[A] =
    scala.collection.immutable.ArraySeq.unsafeWrapArray(s)

  implicit class ListHasAsScalaUnsafe[A](l: java.util.List[A]) {
    /**
     * Extension method to convert a `java.util.List` to an [[UnsafeImmutableBufferWrapper]].
     *
     * This conversion is useful when an immutable collection is needed and the underlying Java list is known to be
     * immutable / fresh / non-aliased.
     */
    def asScalaUnsafeImmutable(implicit
        @implicitNotFound(
          "import scala.jdk.CollectionConverters._ to use asScalaUnsafeImmutable") importEvidence: java.util.List[
          A] => AsScalaExtensions#ListHasAsScala[A]): UnsafeImmutableBufferWrapper[A] =
      new UnsafeImmutableBufferWrapper(l.asScala)
  }
}
