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

import scala.runtime.AbstractFunction1

object OptimusSeqUtils {

  /**
   * maps across `source` and returns an OptimusSeq[T] containing the result, or source if fn doesn't change any of the
   * values (compared via `eq`)
   * @param source
   *   the source collection
   * @param fn
   *   the transformation to apply
   * @tparam T
   *   the content type
   * @return
   */
  def mapConserve[T](source: collection.Seq[T], fn: T => T): collection.Seq[T] = {
    if (source.isEmpty) source
    else
      source match {
        case os: OptimusSeq[T] =>
          // all OptimusSeq map operation conserve, and are tuned for size
          os.map(fn)
        case os: OptimusDoubleSeq =>
          // all OptimusDoubleSeq map operation conserve, and are tuned for size
          os.map(fn.asInstanceOf[Double => Double])
        case ra: collection.immutable.IndexedSeq[T] =>
          // Indexed sequences do not add any new methods to `Seq`, but promise
          //  efficient implementations of random access patterns.
          //
          var index = 0
          var builder: OptimusBuilder[T, OptimusSeq[T]] = null
          val max = ra.size
          try {
            while (index < max) {
              val value = ra(index)
              val mapped = fn(value)
              if ((builder eq null) && (mapped.asInstanceOf[AnyRef] ne value.asInstanceOf[AnyRef])) {
                builder = OptimusSeq.borrowBuilder[T]
                builder.addFrom(source, 0, index)
              }
              if (builder ne null)
                builder += mapped
              index += 1
            }
            if (builder eq null) ra
            else builder.result()
          } finally {
            if (builder ne null) builder.returnBorrowed()
          }
        case ls: collection.immutable.LinearSeq[T] =>
          // LinearSeq sequences do not add any new methods to `Seq`, but promise
          //  efficient implementations of head-tail.
          //
          var index = 0
          var builder: OptimusBuilder[T, OptimusSeq[T]] = null
          var remaining = ls
          try {
            while (!remaining.isEmpty) {
              val value = remaining.head
              val mapped = fn(value)
              if ((builder eq null) && (mapped.asInstanceOf[AnyRef] ne value.asInstanceOf[AnyRef])) {
                builder = OptimusSeq.borrowBuilder[T]
                builder.addFrom(source, 0, index)
              }
              if (builder ne null)
                builder += mapped
              index += 1
              remaining = remaining.tail
            }
            if (builder eq null) ls
            else builder.result()
          } finally {
            if (builder ne null) builder.returnBorrowed()
          }
        case _ =>
          class Accumulator extends AbstractFunction1[T, Unit] {
            def returnBuilder(): Unit = if (builder ne null) {
              OptimusBuilder.returnIfBorrowed(builder)
            }
            var size = 0
            var builder: OptimusBuilder[T, OptimusSeq[T]] = null
            override def apply(value: T): Unit = {
              val mapped = fn(value)
              if ((builder eq null) && (mapped.asInstanceOf[AnyRef] ne value.asInstanceOf[AnyRef])) {
                builder = OptimusSeq.borrowBuilder[T]
                builder.addFrom(source, 0, size)
              }
              if (builder ne null)
                builder += mapped
              size += 1
            }

            def result = if (builder eq null) source else builder.result()
          }
          // its basically an object, but this avoids the LazyRef creation
          val accumulator = new Accumulator
          try {
            source foreach accumulator
            accumulator.result
          } finally {
            accumulator.returnBuilder()
          }
      }
  }
}
