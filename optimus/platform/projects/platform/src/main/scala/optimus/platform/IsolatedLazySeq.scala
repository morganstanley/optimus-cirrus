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

package optimus.platform
import optimus.core.needsPlugin
import optimus.platform.annotations.alwaysAutoAsyncArgs

import scala.collection.mutable

trait IsolatedLazySeq {
  implicit class IsolatedLazySequenceOps[T](private val in: Iterable[T]) {
    @alwaysAutoAsyncArgs @async def laze[U](nBatch: Int, nTake: Int)(f: Iterable[T] => Iterable[U]): Iterable[U] =
      needsPlugin
    @async def laze$NF[U](nBatch: Int, nTake: Int)(f: NodeFunction1[Iterable[T], Iterable[U]]): Iterable[U] = {
      val res = mutable.ArrayBuffer.empty[U]
      val it: Iterator[Iterable[T]] = in.grouped(nBatch)
      var continue = true
      while (continue && res.size < nTake && it.hasNext) {
        val batch: Iterable[T] = it.next()
        if (batch.isEmpty) continue = false
        res ++= f(batch)
      }
      res.take(nTake)
    }
  }
}
