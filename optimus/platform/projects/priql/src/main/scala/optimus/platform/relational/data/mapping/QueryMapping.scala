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
package optimus.platform.relational.data.mapping

import optimus.platform._
import optimus.platform.pickling.Unpickler
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.data.DataProvider
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.translation._
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.TypeInfo

trait QueryMapping extends MappingEntityLookup {
  def getTableName(e: MappingEntity): String
  def isMapped(entity: MappingEntity, member: MemberInfo): Boolean
  def isColumn(entity: MappingEntity, member: MemberInfo): Boolean
  def getColumnName(entity: MappingEntity, member: MemberInfo): String
  def getColumnInfo(entity: MappingEntity, member: MemberInfo): ColumnInfo
  def getMappedMembers(entity: MappingEntity): Seq[MemberInfo]
  def createMapper(translator: QueryTranslator): QueryMapper
  def isRelationship(entity: MappingEntity, member: MemberInfo): Boolean
  def isSingletonRelationship(entity: MappingEntity, member: MemberInfo): Boolean
  def isProviderSupported(dp: DataProvider): Boolean
  def inferKeyColumnDeclarations(select: SelectElement): List[ColumnDeclaration]
}

trait QueryMapper {
  def mapping: QueryMapping
  def translator: QueryTranslator
  def binder: QueryBinder
  def getQueryElement(e: MappingEntity, key: RelationKey[_], keyPolicy: KeyPropagationPolicy): ProjectionElement
  def getEntityElement(root: RelationElement, entity: MappingEntity): DbEntityElement
  def getMemberElement(root: RelationElement, entity: MappingEntity, member: MemberInfo): RelationElement

  def translate(element: RelationElement): RelationElement = {

    // convert references to PriQL operators into query specific nodes
    val d = DistinctByKeyOptimizer.optimize(element)
    var e = binder.bind(this, d)
    if (e ne d) {
      // move aggregate computations so they occur in same select as group-by
      e = AggregateRewriter.rewrite(translator.dialect.language, e)

      // do reduction so duplicate association's are likely to be clumped together
      e = UnusedColumnRemover.remove(mapping, e)
      e = RedundantColumnRemover.remove(e)
      e = RedundantSubqueryRemover.remove(e)
      e = DuplicateJoinRemover.remove(e)

      // we do not handle 1-* relationship yet
      e = RelationshipBinder.bind(this, e)
    }
    e
  }
}

trait MappingEntity {
  def tableId: String
  def projectedType: TypeInfo[_]
}

trait MemberInfo {
  def reflectType: TypeInfo[_]
  def name: String
  def memberType: TypeInfo[_]
  def unpickler: Option[Unpickler[_]]
}

object MemberInfo {
  def apply(
      reflectType: TypeInfo[_],
      name: String,
      memberType: TypeInfo[_],
      unpickler: Option[Unpickler[_]] = None): MemberInfo = {
    MemberInfoImpl(reflectType, name, memberType, unpickler)
  }

  private final case class MemberInfoImpl(
      reflectType: TypeInfo[_],
      name: String,
      memberType: TypeInfo[_],
      unpickler: Option[Unpickler[_]])
      extends MemberInfo
}
