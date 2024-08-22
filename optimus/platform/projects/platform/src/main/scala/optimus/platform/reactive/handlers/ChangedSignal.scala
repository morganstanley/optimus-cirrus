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
package optimus.platform.reactive.handlers
import optimus.platform.storable.EntityReference

sealed trait ChangedSignal[+V] {

  /**
   * the identity of the value. if updates are tracked over time this can be used to identify the object e.g. if V is an
   * Entity, this would be the EntityReference
   */
  val id: EntityReference

  def copyWithValue[B](v: B): ChangedSignal[B]
}

sealed trait ChangedSignalWithCurrent[+V] extends ChangedSignal[V] {
  val value: V
}

object ChangedSignalWithCurrent {
  def unapply[V](value: ChangedSignalWithCurrent[V]): Some[(EntityReference, V)] =
    Some(value.id, value.value)
}

sealed trait ChangedSignalWithPrevious[+V] extends ChangedSignal[V]

final case class InsertedSignal[+V](id: EntityReference, value: V) extends ChangedSignalWithCurrent[V] {
  override def copyWithValue[B](v: B): InsertedSignal[B] = copy(value = v)
}

final case class DeletedSignal[+V](id: EntityReference, value: V) extends ChangedSignalWithPrevious[V] {
  override def copyWithValue[B](v: B): DeletedSignal[B] = copy(value = v)
}

final case class UpdatedSignal[+V](id: EntityReference, valueChanged: Boolean, value: V)
    extends ChangedSignalWithCurrent[V]
    with ChangedSignalWithPrevious[V] {
  override def copyWithValue[B](v: B): UpdatedSignal[B] = copy(value = v)
}
