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

import scala.collection.mutable
import scala.collection.IndexedSeqView

// Write-through views were removed from Scala 2.13 collections, we need to roll our own.
class MutableIndexedSeqView[A](self: MutableIndexedSeqView.SomeIndexedSeqOps[A], from: Int, until: Int)
    extends IndexedSeqView.Slice[A](self, from, until) {
  def update(idx: Int, elem: A): Unit = {
    if (idx >= 0 && idx + from < until) self.update(idx + from, elem)
    else throw new IndexOutOfBoundsException(idx.toString)
  }
}

object MutableIndexedSeqView {
  type AnyConstr[X] = Any
  type SomeIndexedSeqOps[A] = mutable.IndexedSeqOps[A, AnyConstr, _]
}
