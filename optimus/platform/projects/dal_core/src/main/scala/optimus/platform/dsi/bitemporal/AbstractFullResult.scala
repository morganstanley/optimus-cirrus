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
package optimus.platform.dsi.bitemporal

abstract class AbstractFullResult[T, R](val values: Iterable[T]) { _: R =>
  type PartialType <: AbstractPartialResult[T, R] with R

  def createPartial(batchSize: Int): Iterator[AbstractPartialResult[T, R] with R] = {
    var first = true
    val keysIter = values.iterator
    if (keysIter.hasNext) {
      keysIter
        .grouped(batchSize)
        .map(partialResult => {
          val isFirst = first
          first = false
          val isLast = !keysIter.hasNext
          generatePartialInstance(partialResult, isFirst, isLast)
        })
    } else {
      // if there are no results we still need to send an (empty) response otherwise the client doesn't know there are no
      // results and will just hang forever
      Iterator(generatePartialInstance(Nil, isFirst = true, isLast = true))
    }
  }

  def generatePartialInstance(partialResult: Iterable[T], isFirst: Boolean, isLast: Boolean): PartialType
}
