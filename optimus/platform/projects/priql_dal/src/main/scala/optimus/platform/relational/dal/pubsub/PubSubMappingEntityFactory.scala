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
package optimus.platform.relational.dal.pubsub

import optimus.entity.ClassEntityInfo
import optimus.entity.IndexInfo
import optimus.platform.dsi.bitemporal.proto.ProtoSerialization.distinctBy
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Id
import optimus.platform.dsi.expressions.{Entity => EntityExpression}
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.dal.core.IndexColumnInfo
import optimus.platform.relational.dal.core.IndexMemberInfo
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.tree.ColumnInfo
import optimus.platform.relational.data.tree.ColumnType
import optimus.platform.relational.tree.TypeInfo
import optimus.platform.storable.Storable

import scala.collection.immutable.HashMap
import scala.collection.mutable
import optimus.scalacompat.collection._

import scala.collection.compat._

object PubSubMappingEntityFactory {
  def create(entityInfo: ClassEntityInfo, projectedType: TypeInfo[_], tableId: String): DALMappingEntity = {
    DALEntity(entityInfo, projectedType, tableId)
  }

  private final case class DALEntity(entityInfo: ClassEntityInfo, projectedType: TypeInfo[_], tableId: String)
      extends DALMappingEntity {
    val mappedMembers = {
      val propertyMap =
        entityInfo.storedProperties.map(p => (p.name, p.typeInfo)).toMap[String, TypeInfo[_]]
      val memberList = new mutable.ListBuffer[MemberInfo]
      memberList += MemberInfo(projectedType, DALProvider.EntityRef, DALProvider.EntityRefType)
      memberList += MemberInfo(projectedType, DALProvider.StorageTxTime, DALProvider.StorageTxTimeType)
      memberList += MemberInfo(projectedType, DALProvider.VersionedRef, DALProvider.VersionedRefType)

      val queryableIndexInfoIt = entityInfo.indexes.iterator.filter(_.queryable)
      val queryableIndexInfoSeq =
        distinctBy[IndexInfo[_ <: Storable, _], String](queryableIndexInfoIt, _.name).toIndexedSeq

      propertyMap foreach { case (name, tinfo) =>
        queryableIndexInfoSeq.find(i => i.name == name) match {
          case Some(indexInfo) =>
            if (indexInfo.propertyNames.isEmpty) {
              memberList += new IndexMemberInfo(projectedType, TypeInfo.UNIT, indexInfo)
            } else if (indexInfo.propertyNames.size > 1) {
              val memberTypes: List[TypeInfo[_]] =
                indexInfo.propertyNames.iterator.map(name => propertyMap(name)).toList
              memberList += new IndexMemberInfo(projectedType, TypeInfo.mkTuple(memberTypes), indexInfo)
            } else {
              propertyMap
                .get(indexInfo.propertyNames.head)
                .orElse(projectedType.propertyMap.get(indexInfo.name)) foreach { memberType =>
                memberList += new IndexMemberInfo(projectedType, memberType, indexInfo)
              }
            }
          case _ =>
            memberList += MemberInfo(projectedType, name, tinfo)
        }

      }
      val (aliasAndCompoundIndexes, defIndexes) = queryableIndexInfoSeq
        .filterNot(idx => propertyMap.contains(idx.name))
        .partition(idx => idx.propertyNames.forall(propertyMap.contains))

      // compound index
      aliasAndCompoundIndexes.filter(_.propertyNames.size > 1).foreach { indexInfo =>
        val memberTypes: List[TypeInfo[_]] = indexInfo.propertyNames.iterator.map(name => propertyMap(name)).toList
        memberList += new IndexMemberInfo(projectedType, TypeInfo.mkTuple(memberTypes), indexInfo)
      }

      // single def index
      aliasAndCompoundIndexes.filter(_.propertyNames.size == 1).foreach { indexInfo =>
        val memberTypes: List[TypeInfo[_]] = indexInfo.propertyNames.iterator.map(name => propertyMap(name)).toList
        memberList += new IndexMemberInfo(projectedType, memberTypes.head, indexInfo)
      }

      // unique-index def is not supported because it requires TxnAction changes to be able to
      // populate it on pubsub server.
      defIndexes.filter(!_.unique).foreach { indexInfo =>
        val memberTypes: List[TypeInfo[_]] =
          indexInfo.propertyNames.iterator.map(name => projectedType.propertyMap(name)).toList
        memberList += new IndexMemberInfo(projectedType, memberTypes.head, indexInfo)
      }

      memberList.result()
    }

    val mappedMemberLookup: HashMap[String, MemberInfo] =
      mappedMembers.iterator.map(t => t.name -> t).convertTo(HashMap)

    def getMappedMember(member: String): MemberInfo = mappedMemberLookup(member)
    def isMapped(member: String): Boolean = mappedMemberLookup.contains(member)
    def getColumnInfo(member: MemberInfo): ColumnInfo = member match {
      case m: IndexMemberInfo => IndexColumnInfo(m.index)
      case _ =>
        getMappedMember(member.name) match {
          case m: IndexMemberInfo => IndexColumnInfo(m.index)
          case m                  => ColumnInfo(ColumnType.Default, m.unpickler)
        }
    }
    def isColumn(member: MemberInfo): Boolean = isMapped(member.name)
    def getCompoundMembers(member: String): Option[List[MemberInfo]] = None
    def getDefaultValue(member: String): Option[Any] = None
    def getOriginalType(member: String): TypeInfo[_] = getMappedMember(member).memberType
    def format(id: Id): Expression = {
      val types: Seq[String] = entityInfo.baseTypes.iterator.map { _.runtimeClass.getName }.toIndexedSeq
      EntityExpression(tableId, null, types, id)
    }
  }
}
