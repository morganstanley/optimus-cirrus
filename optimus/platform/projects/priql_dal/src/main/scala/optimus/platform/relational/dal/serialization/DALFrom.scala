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
package optimus.platform.relational.dal.serialization

import optimus.platform.dal.DalAPI
import optimus.platform._
import optimus.platform.relational._
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.dal.StorableConverterImplicits
import optimus.platform.relational.serialization.QueryBuilder
import optimus.platform.relational.tree.MethodPosition
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Entity

object StorableDalImplicits extends StorableConverterImplicits with DalAPI

final case class DALFrom[T <: Entity](
    entity: Class[T],
    keyPolicy: KeyPropagationPolicy,
    pos: MethodPosition
) extends QueryBuilder[T] {
  import StorableDalImplicits._
  def build: Query[T] = {
    from(entity, keyPolicy)(mkEntityClassConverter[T], TypeInfo.javaTypeInfo(entity), pos)
  }
}
