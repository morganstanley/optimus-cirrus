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
package optimus.platform.relational.data.tree

import optimus.platform.relational.tree.ElementType

/**
 * Extended element types for custom relation element
 */
trait DbElementType extends ElementType

object DbElementType {
  case object Aggregate extends DbElementType
  case object AggregateSubquery extends DbElementType
  case object Table extends DbElementType
  case object Column extends DbElementType
  case object Select extends DbElementType
  case object Projection extends DbElementType
  case object Join extends DbElementType
  case object DbEntity extends DbElementType
  case object Contains extends DbElementType
  case object NamedValue extends DbElementType
  case object Scalar extends DbElementType
  case object Exists extends DbElementType

  // special projector to map ResultSet to in-memory objects
  case object EmbeddableCollection extends DbElementType
  case object EmbeddableCaseClass extends DbElementType
  case object DALHeapEntity extends DbElementType
  case object Tuple extends DbElementType
  case object Option extends DbElementType
  case object DynamicObject extends DbElementType
  case object OuterJoined extends DbElementType
}
