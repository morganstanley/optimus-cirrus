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
import optimus.platform._
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.serialization.InMemoryFrom
import optimus.platform.relational.serialization.QueryBuilder
import optimus.platform.relational.serialization.QueryBuilderConvertible
import optimus.platform.relational.tree._

import scala.collection.mutable.ListBuffer

/**
 * ScalaTypeMultiRelation is In Memory PriQL Provider which allows PriQL queries to be executed against in memory data
 * structures.
 */
class ScalaTypeMultiRelation[T](
    val data: Iterable[T],
    typeInfo: TypeInfo[T],
    key: RelationKey[_],
    pos: MethodPosition,
    override val shouldExecuteDistinct: Boolean,
    keyPolicy: KeyPropagationPolicy
) extends ProviderRelation(typeInfo, key, pos)
    with IterableSource[T]
    with ArrangedSource
    with QueryBuilderConvertible {
  override def getProviderName = "ScalaProvider"

  override def prettyPrint(indent: Int, out: StringBuilder): Unit = {
    indentAndStar(indent, out)
    if (data == null || data.isEmpty)
      out ++= serial + " Provider:" + " '" + getProviderName + "' its data source is empty" + "\n"
    else
      out ++= serial + " Provider:" + " '" + getProviderName + "' " + "of" + " type " + data.last.getClass.getName + "\n"
  }

  override def fillQueryExplainItem(level_id: Integer, table: ListBuffer[QueryExplainItem]) = {
    if (data == null || data.isEmpty)
      table += new QueryExplainItem(level_id, getProviderName, "Nil", "InMemoryScan", 0)
    else {
      val object_name = data.head.getClass.getName
      table += new QueryExplainItem(level_id, getProviderName, object_name.toString(), "InMemoryScan", 0)
    }
  }

  override def isSyncSafe = true
  override def getSync() = data.toIndexedSeq
  @async override def get() = getSync()

  override def makeKey(newKey: RelationKey[_]) = {
    new ScalaTypeMultiRelation[T](data, typeInfo, newKey, pos, shouldExecuteDistinct, keyPolicy)
  }

  override def toQueryBuilder: QueryBuilder[_] = {
    InMemoryFrom(data, typeInfo, shouldExecuteDistinct, keyPolicy, pos)
  }

}

object ScalaTypeMultiRelation {
  def apply[A](
      iter: Iterable[A],
      key: RelationKey[_],
      pos: MethodPosition = MethodPosition.unknown,
      typeInfo: TypeInfo[A] = TypeInfo.noInfo[A],
      shouldExecuteDistinct: Boolean = true,
      keyPolicy: KeyPropagationPolicy
  ): ScalaTypeMultiRelation[A] = {
    new ScalaTypeMultiRelation[A](iter, typeInfo, key, pos, shouldExecuteDistinct, keyPolicy)
  }
}
