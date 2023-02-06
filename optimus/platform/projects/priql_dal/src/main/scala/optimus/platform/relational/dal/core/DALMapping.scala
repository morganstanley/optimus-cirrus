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
package optimus.platform.relational.dal.core

import optimus.entity.EntityInfoRegistry
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.data.DataProvider
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.mapping.BasicMapper
import optimus.platform.relational.data.mapping.BasicMapping
import optimus.platform.relational.data.mapping.MappingEntity
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.translation.UnusedColumnRemover
import optimus.platform.relational.data.tree.ColumnInfo
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.TypeInfo

class DALMapping extends BasicMapping {
  protected override def getEntityImpl(shape: TypeInfo[_]): MappingEntity = {
    val info = EntityInfoRegistry.getClassInfo(shape.clazz)
    DALMappingEntityFactory.create(info, shape, shape.runtimeClassName)
  }

  override def getColumnInfo(entity: MappingEntity, member: MemberInfo): ColumnInfo = {
    entity.asInstanceOf[DALMappingEntity].getColumnInfo(member)
  }

  override def getMappedMembers(entity: MappingEntity): Seq[MemberInfo] = {
    entity.asInstanceOf[DALMappingEntity].mappedMembers
  }

  override def isMapped(entity: MappingEntity, member: MemberInfo): Boolean = {
    entity.asInstanceOf[DALMappingEntity].isMapped(member.name)
  }

  override def createMapper(translator: QueryTranslator): DALMapper = {
    new DALMapper(this, translator)
  }

  override def isProviderSupported(dp: DataProvider): Boolean = {
    dp.isInstanceOf[DALProvider]
  }
}

class DALMapper(m: DALMapping, t: QueryTranslator) extends BasicMapper(m, t) {
  override val binder: QueryBinder = DALBinder

  override def translate(element: RelationElement): RelationElement = {
    var e = binder.bind(this, element)
    e = SpecialElementRewriter.rewrite(e)
    UnusedColumnRemover.remove(mapping, e)
  }
}
