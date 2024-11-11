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
package optimus.buildtool.cache.remote

import java.util.concurrent.atomic.AtomicInteger

class CacheOperationRecorder {
  private val _incompleteReads = new AtomicInteger(0)
  def incompleteReads: Int = _incompleteReads.get

  private val _incompleteWrites = new AtomicInteger(0)
  def incompleteWrites: Int = _incompleteWrites.get

  private[cache] def recordIncomplete(opType: OperationType): Unit = opType match {
    case OperationType.Read  => _incompleteReads.incrementAndGet()
    case OperationType.Write => _incompleteWrites.incrementAndGet()
  }
}
