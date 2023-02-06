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
package optimus.platform.relational.dal.internal

import optimus.platform._
import optimus.platform._
import optimus.platform.storable._

/**
 * RawReferenceKey is used with Optimus Entity/ EntityReferenceHolder/ Event and simply uses the RawReference as
 * arranging key
 */
private[dal] class RawReferenceKey[-T] private (val keyOf: T => RawReference) extends RelationKey[T] {

  override def isSyncSafe = true
  override def ofSync(t: T): Any = keyOf(t)
  @node override def of(t: T) = keyOf(t)

  val fields = Nil
  override lazy val isKeyComparable: Boolean = true

  override def compareKeys(leftKey: Any, rightKey: Any): Int = {
    val (l: RawReference, r: RawReference) = (leftKey, rightKey)
    RelationKey.compareArrayData(l.data, r.data)
  }
}

private[dal] object RawReferenceKey {
  def apply[T](keyOf: T => RawReference) = new RawReferenceKey[T](keyOf)
}
