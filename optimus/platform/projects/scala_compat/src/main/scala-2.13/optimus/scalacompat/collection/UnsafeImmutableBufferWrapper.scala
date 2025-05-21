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

import scala.annotation.unchecked.uncheckedVariance
import scala.collection.SeqFactory
import scala.collection.StrictOptimizedSeqFactory
import scala.collection.immutable
import scala.collection.immutable.AbstractSeq
import scala.collection.immutable.ArraySeq
import scala.collection.immutable.IndexedSeqOps
import scala.collection.immutable.StrictOptimizedSeqOps
import scala.collection.mutable

/**
 * This collection wraps a [[mutable.Buffer]] as an immutable collection. It is typically used as
 *
 * {{{
 *   import scala.jdk.CollectionConverters._
 *   javaList.asScala                 // returns a mutable.Buffer
 *   javaList.asScalaUnsafeImmutable  // returns an UnsafeImmutableBufferWrapper
 * }}}
 *
 * It is useful for Java interoperability. With `scala.jdk.CollectionConverters`, the `javaList.asScala` conversion
 * returns a `mutable.Buffer`. Mutations to the underlying Java list are reflected in the buffer and vice versa.
 *
 * Often the underlying collection is known to be effectively immutable, for example when using protobufs. In this
 * case, `javaList.asScalaUnsafeImmutable` returns an `UnsafeImmutableBufferWrapper` that can be passed as an
 * `immutable.Seq`.
 *
 * Transformations that return the same collection type (e.g., `filter`) are applied to the underlying buffer
 * (creating a fresh buffer, mo mutation) and return an `UnsafeImmutableBufferWrapper`.
 * Transformations that map the element type (e.g., `map`) return an `ArraySeq`.
 */
final class UnsafeImmutableBufferWrapper[+A](buf: mutable.Buffer[A])
    extends AbstractSeq[A]
    with immutable.IndexedSeq[A]
    with IndexedSeqOps[A, ArraySeq, UnsafeImmutableBufferWrapper[A]]
    with StrictOptimizedSeqOps[A, ArraySeq, UnsafeImmutableBufferWrapper[A]]
    with Serializable {

  override protected def fromSpecific(coll: IterableOnce[A @uncheckedVariance]): UnsafeImmutableBufferWrapper[A] =
    UnsafeImmutableBufferWrapper.from(coll)
  override protected def newSpecificBuilder
      : mutable.Builder[A @uncheckedVariance, UnsafeImmutableBufferWrapper[A @uncheckedVariance]] =
    UnsafeImmutableBufferWrapper.newBuilder
  override def empty: UnsafeImmutableBufferWrapper[A] = UnsafeImmutableBufferWrapper.empty

  override def iterableFactory: SeqFactory[ArraySeq] = ArraySeq.untagged

  def apply(i: Int): A = buf(i)

  def length: Int = buf.length

  override protected[this] def className: String = "UnsafeImmutableBufferWrapper"
}

object UnsafeImmutableBufferWrapper extends StrictOptimizedSeqFactory[UnsafeImmutableBufferWrapper] {
  override def from[A](source: IterableOnce[A]): UnsafeImmutableBufferWrapper[A] = new UnsafeImmutableBufferWrapper(
    mutable.Buffer.from(source))

  override def empty[A]: UnsafeImmutableBufferWrapper[A] = new UnsafeImmutableBufferWrapper[A](mutable.Buffer.empty)

  override def newBuilder[A]: mutable.Builder[A, UnsafeImmutableBufferWrapper[A]] =
    mutable.Buffer.newBuilder[A].mapResult(new UnsafeImmutableBufferWrapper(_))
}
