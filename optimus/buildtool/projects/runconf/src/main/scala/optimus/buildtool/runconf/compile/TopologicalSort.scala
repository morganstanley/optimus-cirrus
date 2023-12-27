/*
 * Copyright the original author(s) at https://github.com/pelamfi/pelam-scala-incubator using the Apache 2.0 license.
 *
 * Modifications to this file were made by Morgan Stanley to use immutable collections in the result.
 * For those changes only:
 *
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
package optimus.buildtool.runconf.compile

import scala.collection.mutable
import scala.collection.immutable.IndexedSeq

private[compile] object TopologicalSort {
  final case class Result[T](result: IndexedSeq[T], loops: IndexedSeq[IndexedSeq[T]])

  type Visit[T] = T => Unit

  // A visitor is a function that takes a node and a callback.
  // The visitor calls the callback for each node referenced by the given node.
  type Visitor[T] = (T, Visit[T]) => Unit

  def sort[T <: AnyRef](input: Iterable[T])(visitor: Visitor[T]): Result[T] = {
    // Buffer, because it is operated in a stack like fashion
    val temporarilyMarked = mutable.Buffer[T]()
    val permanentlyMarked = mutable.HashSet[T]()
    val loopsBuilder = IndexedSeq.newBuilder[IndexedSeq[T]]
    val resultBuilder = IndexedSeq.newBuilder[T]

    def visit(node: T): Unit = {
      if (temporarilyMarked.contains(node)) {
        val loopStartIndex = temporarilyMarked.indexOf(node)
        val loop = node +: temporarilyMarked.slice(loopStartIndex + 1, temporarilyMarked.size).reverse.toIndexedSeq
        // Add the cycle separately so it is in one place and its order remains stable
        // when this algorithm is run multiple times.
        resultBuilder ++= loop
        permanentlyMarked ++= loop
        loopsBuilder += loop
      } else if (!permanentlyMarked.contains(node)) {
        temporarilyMarked += node
        visitor(node, visit)
        temporarilyMarked.remove(temporarilyMarked.size - 1, 1)
        if (!permanentlyMarked.contains(node)) {
          // Extra check due to cycle handling above.
          permanentlyMarked += node
          resultBuilder += node
        }
      }
    }

    for (i <- input) {
      if (!permanentlyMarked.contains(i)) {
        visit(i)
      }
    }

    Result(resultBuilder.result(), loopsBuilder.result())
  }
}
