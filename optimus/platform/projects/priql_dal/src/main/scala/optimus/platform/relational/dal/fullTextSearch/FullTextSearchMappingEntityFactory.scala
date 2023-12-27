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

package optimus.platform.relational.dal.fullTextSearch

import optimus.entity.ClassEntityInfo
import optimus.graph.NodeTaskInfo
import optimus.platform.annotations.internal._fullTextSearch
import optimus.platform.dsi.expressions.Expression
import optimus.platform.dsi.expressions.{Entity => EntityExpression}
import optimus.platform.dsi.expressions.Id
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.dal.core.IndexColumnInfo
import optimus.platform.relational.dal.core.IndexMemberInfo
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.tree.ColumnInfo
import optimus.platform.relational.data.tree.ColumnType
import optimus.platform.relational.internal.RelationalUtils
import optimus.platform.relational.tree.TypeInfo

import scala.collection.immutable.HashMap
import scala.collection.mutable
import optimus.scalacompat.collection._

import scala.collection.compat._

object FullTextSearchMappingEntityFactory {
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
      val entityMethods = RelationalUtils.getMethodsMap(entityInfo.runtimeClass)

      for (indexInfo <- entityInfo.indexes if indexInfo.queryable) {
        val fullTextSearchAnno =
          RelationalUtils.getMethodAnnotation(entityMethods, indexInfo.name, classOf[_fullTextSearch]).getOrElse(null)
        if (fullTextSearchAnno != null && fullTextSearchAnno.queryable()) {
          if (indexInfo.propertyNames.isEmpty) {
            memberList += new IndexMemberInfo(projectedType, TypeInfo.UNIT, indexInfo)
          } else {
            require(indexInfo.propertyNames.size == 1, "We don't expect compound index for fullTextSearch fields")
            propertyMap
              .get(indexInfo.propertyNames.head)
              .orElse(projectedType.propertyMap.get(indexInfo.name)) foreach { case memberType =>
              memberList += new IndexMemberInfo(projectedType, memberType, indexInfo)
            }
          }
        }
      }
      for (property <- entityInfo.storedProperties) {
        val isIndex = property.snapFlags() & NodeTaskInfo.INDEXED
        val isKey = property.snapFlags() & NodeTaskInfo.KEY
        if (isIndex == 0 && isKey == 0) {
          val fullTextSearchAnno =
            RelationalUtils.getMethodAnnotation(entityMethods, property.name, classOf[_fullTextSearch]).getOrElse(null)
          if (fullTextSearchAnno != null && fullTextSearchAnno.queryable()) {
            memberList += MemberInfo(projectedType, property.name, propertyMap(property.name))
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
