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

import optimus.entity.EntityInfo
import optimus.entity.EntityInfoRegistry
import optimus.platform._
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.translation._
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase

import scala.collection.mutable.ListBuffer
import optimus.scalacompat.collection._

abstract class BasicMapping extends QueryMapping {
  protected def getEntityImpl(shape: TypeInfo[_]): MappingEntity = {
    val clazz = shape.clazz
    val tableId =
      try {
        clazz.getSimpleName
      } catch {
        // workaround for REPL case
        case e: InternalError if e.getMessage == "Malformed class name" => clazz.getName
      }
    BasicMappingEntity(shape, tableId)
  }

  def getTableName(e: MappingEntity): String = {
    e.tableId
  }

  def isMapped(entity: MappingEntity, member: MemberInfo): Boolean = {
    true
  }

  def isColumn(entity: MappingEntity, member: MemberInfo): Boolean = {
    isMapped(entity, member)
  }

  def getColumnName(entity: MappingEntity, member: MemberInfo): String = {
    member.name
  }

  def getColumnInfo(entity: MappingEntity, member: MemberInfo): ColumnInfo = {
    ColumnInfo(ColumnType.Default, member.unpickler)
  }

  def getMappedMembers(entity: MappingEntity): Seq[MemberInfo] = {
    val t = entity.projectedType
    val propertyMap = t.propertyParameterizedTypes.mapValuesNow(TypeInfo.toTypeInfo)
    if (t <:< classOf[Entity]) {
      val info = EntityInfoRegistry.getInfo(t.clazz).asInstanceOf[EntityInfo]
      if (info.isStorable)
        info.properties.withFilter(_.isStored).map(p => MemberInfo(t, p.name, propertyMap(p.name))).sortBy(_.name)
      else // transient entity
        t.primaryConstructorParams.map { case (name, _) => MemberInfo(t, name, propertyMap(name)) }
    } else {
      if (t.classes.nonEmpty && t.classes.forall(t => t.isInterface && t.getTypeParameters.length == 0)) {
        propertyMap.toSeq.map(p => MemberInfo(t, p._1, p._2)).sortBy(_.name)
      } else {
        t.primaryConstructorParams.map { case (name, _) => MemberInfo(t, name, propertyMap(name)) }
      }
    }
  }

  def isRelationship(entity: MappingEntity, member: MemberInfo): Boolean = {
    isAssociationRelationship(entity, member)
  }

  def isAssociationRelationship(entity: MappingEntity, member: MemberInfo): Boolean = {
    false
  }

  def isSingletonRelationship(entity: MappingEntity, member: MemberInfo): Boolean = {
    if (!isRelationship(entity, member)) false
    else {
      val seqType = member.memberType
      if (seqType eq null) true
      else if (seqType.clazz.isArray) false
      else !(seqType <:< classOf[Iterable[_]])
    }
  }

  protected def getRelatedEntityImpl(entity: MappingEntity, member: MemberInfo): MappingEntity = {
    val relatedType = Query.findShapeType(member.memberType)
    getEntity(relatedType)
  }

  def inferKeyColumnDeclarations(select: SelectElement): List[ColumnDeclaration] = {
    select.columns
  }

  def createMapper(translator: QueryTranslator): QueryMapper = {
    new BasicMapper(this, translator)
  }

  protected case class BasicMappingEntity(override val projectedType: TypeInfo[_], override val tableId: String)
      extends MappingEntity
}

class BasicMapper(override val mapping: BasicMapping, override val translator: QueryTranslator) extends QueryMapper {
  def binder: QueryBinder = DefaultBinder

  def getQueryElement(e: MappingEntity, key: RelationKey[_], keyPolicy: KeyPropagationPolicy): ProjectionElement = {
    val tableAlias = new TableAlias
    val selectAlias = new TableAlias
    val table = new TableElement(tableAlias, e, mapping.getTableName(e))

    val projector = getEntityElement(table, e)
    val pc = ColumnProjector.projectColumns(this.translator.language, projector, null, selectAlias, tableAlias)
    new ProjectionElement(
      new SelectElement(selectAlias, pc.columns, table, null, null, null, null, null, false, false),
      pc.projector,
      key,
      keyPolicy,
      entitledOnly = false)
  }

  def getEntityElement(root: RelationElement, entity: MappingEntity): DbEntityElement = {
    val memberNames = new ListBuffer[String]
    val members = new ListBuffer[RelationElement]
    mapping.getMappedMembers(entity).foreach { m =>
      if (!mapping.isAssociationRelationship(entity, m)) {
        memberNames += m.name
        members += getMemberElement(root, entity, m)
      }
    }
    new DbEntityElement(entity, buildEntityElement(entity, members.result(), memberNames.result()))
  }

  def getAssociationElement(root: RelationElement, entity: MappingEntity, member: MemberInfo): RelationElement = ???

  def getMemberElement(root: RelationElement, entity: MappingEntity, member: MemberInfo): RelationElement = {
    if (mapping.isAssociationRelationship(entity, member)) {
      getAssociationElement(root, entity, member)
    } else
      root match {
        case a: AliasedElement if mapping.isColumn(entity, member) =>
          new ColumnElement(
            member.memberType,
            a.alias,
            mapping.getColumnName(entity, member),
            mapping.getColumnInfo(entity, member))
        case _ =>
          binder.bindMember(root, member)
      }
  }

  protected def buildEntityElement(
      entity: MappingEntity,
      members: List[RelationElement],
      memberNames: List[String]): RelationElement = {
    val t = entity.projectedType
    if (t <:< classOf[Entity]) {
      val companion = EntityInfoRegistry.getCompanion(t.clazz).asInstanceOf[EntityCompanionBase[_]]
      new DALHeapEntityElement(companion, t, members, memberNames)
    } else {
      val ctor = new RuntimeConstructorDescriptor(t)
      if (t.classes.nonEmpty && t.classes.forall(t => t.isInterface && t.getTypeParameters.length == 0)) {
        val memberDescs =
          memberNames.zip(members).map { case (name, m) => new RuntimeFieldDescriptor(t, name, m.rowTypeInfo) }
        ElementFactory.makeNew(ctor, members, memberDescs)
      } else {
        val arguments = t.primaryConstructorParams.map { case (name, _) => members(memberNames.indexOf(name)) }
        ElementFactory.makeNew(ctor, arguments.toList, Nil)
      }
    }
  }
}
