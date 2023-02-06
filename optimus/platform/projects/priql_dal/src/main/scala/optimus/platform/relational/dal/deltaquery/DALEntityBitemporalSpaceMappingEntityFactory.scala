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
package optimus.platform.relational.dal.deltaquery

import optimus.entity.ClassEntityInfo
import optimus.platform.dsi.expressions.EntityBitemporalSpace
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.Id
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.dal.core.IndexColumnInfo
import optimus.platform.relational.dal.core.IndexMemberInfo
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.tree.ColumnInfo
import optimus.platform.relational.data.tree.ColumnType
import optimus.platform.relational.tree.TypeInfo

import scala.collection.immutable.HashMap
import scala.collection.mutable
import optimus.scalacompat.collection._
import scala.collection.compat._

object DALEntityBitemporalSpaceMappingEntityFactory {
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
      // Focus only on index field
      for (indexInfo <- entityInfo.indexes if indexInfo.queryable && indexInfo.indexed && !indexInfo.unique) {
        if (indexInfo.propertyNames.isEmpty) {
          memberList += new IndexMemberInfo(projectedType, TypeInfo.UNIT, indexInfo)
        } else if (indexInfo.propertyNames.size > 1) {
          val memberTypes: List[TypeInfo[_]] = indexInfo.propertyNames.iterator.map(name => propertyMap(name)).toList
          memberList += new IndexMemberInfo(projectedType, TypeInfo.mkTuple(memberTypes), indexInfo)
        } else {
          propertyMap.get(indexInfo.propertyNames.head).orElse(projectedType.propertyMap.get(indexInfo.name)) foreach {
            case memberType => memberList += new IndexMemberInfo(projectedType, memberType, indexInfo)
          }
        }
      }
      memberList.result()
    }
    val mappedMemberLookup: HashMap[String, MemberInfo] =
      mappedMembers.iterator.map(t => t.name -> t).convertTo(HashMap)

    def getMappedMember(member: String): MemberInfo = mappedMemberLookup(member)
    def isMapped(member: String): Boolean = mappedMemberLookup.contains(member)
    def getColumnInfo(member: MemberInfo): ColumnInfo = member match {
      case m: IndexMemberInfo => new IndexColumnInfo(m.index)
      case _ =>
        getMappedMember(member.name) match {
          case m: IndexMemberInfo => new IndexColumnInfo(m.index)
          case m                  => ColumnInfo(ColumnType.Default, m.unpickler)
        }
    }
    def isColumn(member: MemberInfo): Boolean = isMapped(member.name)
    def getCompoundMembers(member: String): Option[List[MemberInfo]] = None
    def getDefaultValue(member: String): Option[Any] = None
    def getOriginalType(member: String): TypeInfo[_] = getMappedMember(member).memberType

    // null values will be filled later on
    def format(id: Id): Expression = {
      val types: Seq[String] = entityInfo.baseTypes.iterator.map { _.runtimeClass.getName }.toIndexedSeq
      EntityBitemporalSpace(tableId, null, null, types, id)
    }
  }
}
