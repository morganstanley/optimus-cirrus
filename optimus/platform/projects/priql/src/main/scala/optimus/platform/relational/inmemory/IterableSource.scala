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
package optimus.platform.relational.inmemory

import optimus.platform._

/**
 * Relation provider should implement this trait if it supports in memory execution to Iterable[_]
 */
trait IterableSource[T] {

  /**
   * whether it is safe to call getSync() method
   */
  def isSyncSafe: Boolean

  def getSync(): Iterable[T]

  @async def get(): Iterable[T]

  def shouldExecuteDistinct: Boolean = true
}
