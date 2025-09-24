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
import scala.annotation.unused
import scala.collection.convert.AsScalaExtensions
import scala.collection.immutable.ArraySeq

object DefaultSeq extends optimus.scalacompat.collection.MapBuildFromImplicits {
  implicit class ArrayToVarArgsOps[A](private val self: Array[A]) extends AnyVal {
    /**
     * Convert an Array to an `immutable.Seq` in order to pass it as "sequence argument" to a "vararg" method, a
     * method with a repeated parameter. Example:
     *
     * {{{
     *   Vector(someString.split(' ').toVarArgsSeq: _*)
     * }}}
     *
     * For efficiency, the array is *not* copied. Mutations to the array are reflected in the resulting `immutable.Seq`.
     */
    def toVarArgsSeq: Seq[A] = ArraySeq.unsafeWrapArray(self)
  }

  implicit class ListHasAsScalaUnsafe[A](l: java.util.List[A]) {
    /**
     * This method converts a `java.util.List` to a `scala.collection.immutable.Seq` without copying the collection.
     * The resulting collection therefore changes in case the underlying Java list is mutated, hence the name `Unsafe`.
     *
     * The `asScla` conversion from [[scala.jdk.CollectionConverters]] returns a `mutable.Buffer`.
     * `asScalaUnsafeImmutable` can be used when an immutable collection is needed and the underlying Java list is known
     * to be immutable / fresh / non-aliased. The resulting collection is an [[UnsafeImmutableBufferWrapper]].
     */
    def asScalaUnsafeImmutable(implicit
        @implicitNotFound("import scala.jdk.CollectionConverters._ to use asScalaUnsafeImmutable")
        @unused
        importEvidence: java.util.List[A] => AsScalaExtensions#ListHasAsScala[A]): UnsafeImmutableBufferWrapper[A] =
      if (l == null) null else new UnsafeImmutableBufferWrapper(l.asScala)
  }
}
