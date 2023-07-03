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
package optimus.platform.relational.internal

import optimus.platform._
import scala.collection.mutable.ArrayBuffer

private[optimus] trait AsyncIterator[T] {
  @node def hasNextBatch: Boolean
  @node def nextBatch: Iterable[T]
}

abstract class FlatMapPriqlGrouppedIterator[SrcType, DesType](val source: AsyncIterator[SrcType], val size: Int)
    extends AsyncIterator[DesType] {
  val group: ArrayBuffer[SrcType] = ArrayBuffer()
  var filled = false
  var lastRoundSourceBatchRemaining = 0
  var lastRoundSourceBatch: Iterable[SrcType] = Nil
  var res: Iterable[DesType] = Nil

  @node private def fill = {
    filled = true
    var needToFill = size

    if (lastRoundSourceBatchRemaining <= needToFill) {
      group ++= lastRoundSourceBatch
      lastRoundSourceBatchRemaining = 0
      lastRoundSourceBatch = Nil
      needToFill = needToFill - lastRoundSourceBatchRemaining
    } else {
      group ++= lastRoundSourceBatch.take(needToFill)
      lastRoundSourceBatch = lastRoundSourceBatch.drop(needToFill)
      lastRoundSourceBatchRemaining = lastRoundSourceBatchRemaining - needToFill
      needToFill = 0
    }

    while (needToFill > 0 && source.hasNextBatch) {
      val sourceBatch = source.nextBatch
      val sourceSize = sourceBatch.size
      if (sourceSize <= needToFill) {
        group ++= sourceBatch
        needToFill = needToFill - sourceSize
      } else {
        group ++= sourceBatch.take(needToFill)
        lastRoundSourceBatchRemaining = sourceSize - needToFill
        lastRoundSourceBatch = sourceBatch.drop(needToFill)
        needToFill = 0
      }
    }
  }

  @node private[optimus] def fillGroup: Boolean = {
    if (!filled) fill
    !group.isEmpty
  }

  private[optimus] def afterUseGroup = {
    group.clear()
    filled = false
  }

  @node override def nextBatch: Iterable[DesType] = {
    afterUseGroup
    res
  }
}
