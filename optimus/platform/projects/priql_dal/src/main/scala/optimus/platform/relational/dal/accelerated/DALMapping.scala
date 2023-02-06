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
package optimus.platform.relational.dal.accelerated

import optimus.entity.EntityInfoRegistry
import optimus.platform.NoKey
import optimus.platform.Query
import optimus.platform.annotations.internal.EmbeddableMetaDataAnnotation
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.SpecialElementRewriter
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.data.Aggregator
import optimus.platform.relational.data.DataProvider
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.mapping.BasicMapper
import optimus.platform.relational.data.mapping.BasicMapping
import optimus.platform.relational.data.mapping.MappingEntity
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.translation.ColumnProjector
import optimus.platform.relational.data.translation.RelationshipBinder
import optimus.platform.relational.data.translation.SourceColumnFinder
import optimus.platform.relational.data.translation.UnusedColumnRemover
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityCompanionBase

import scala.collection.mutable

class DALMapping extends BasicMapping {
  protected override def getEntityImpl(shape: TypeInfo[_]): MappingEntity = {
    val underlyingType = TypeInfo.underlying(shape)
    val info = EntityInfoRegistry.getClassInfo(underlyingType.clazz)
    DALMappingEntityFactory.entity(info, underlyingType, underlyingType.clazz.getName)
  }

  protected override def getRelatedEntityImpl(entity: MappingEntity, member: MemberInfo): MappingEntity = {
    val relatedType = TypeInfo.underlying(Query.findShapeType(member.memberType))
    if (relatedType <:< classOf[Entity]) {
      if (isSingletonRelationship(entity, member)) getEntity(relatedType)
      else DALMappingEntityFactory.linkage(entity.tableId, relatedType)
    } else {
      // embeddable
      DALMappingEntityFactory.embeddable(entity.projectedType, member.name, relatedType)
    }
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

  override def isColumn(entity: MappingEntity, member: MemberInfo): Boolean = {
    entity.asInstanceOf[DALMappingEntity].isColumn(member)
  }

  override def isAssociationRelationship(entity: MappingEntity, member: MemberInfo): Boolean = {
    isMapped(entity, member) && !isColumn(entity, member)
  }

  override def createMapper(translator: QueryTranslator): DALMapper = {
    new DALMapper(this, translator)
  }

  override def isProviderSupported(dp: DataProvider): Boolean = {
    dp match {
      case d: DALProvider => d.canBeProjected
      case _              => false
    }
  }

  // If there is EntityRef column in select.columns, we only keep EntityRef and remove other columns
  // from the same source.
  override def inferKeyColumnDeclarations(select: SelectElement): List[ColumnDeclaration] = {
    if (select.columns.size == 1) select.columns
    else {
      val aliasSet = mutable.Set[TableAlias]()
      val declList = mutable.ListBuffer[ColumnDeclaration]()
      val columns = select.columns.map(d => (d, SourceColumnFinder.findSourceColumn(select.from, d)))

      columns.foreach {
        case (d, Some(c))
            if (c.name eq DALProvider.EntityRef) ||
              (c.name eq DALProvider.Payload) || (c.name eq DALProvider.ChildRef) =>
          aliasSet += c.alias
          declList += d
        case (d, None) => declList += d
        case _         =>
      }

      columns.foreach {
        case (d, Some(c)) if (!aliasSet.contains(c.alias)) =>
          declList += d
        case _ =>
      }
      declList.result()
    }
  }
}

class DALMapper(m: DALMapping, t: QueryTranslator) extends BasicMapper(m, t) {
  import DALMappingEntityFactory._

  override val binder: QueryBinder = DALBinder

  override def getAssociationElement(
      root: RelationElement,
      entity: MappingEntity,
      member: MemberInfo): RelationElement = {
    if (mapping.isSingletonRelationship(entity, member)) {
      val relatedEntity = mapping.getRelatedEntity(entity, member)
      val proj1 = getQueryElement(relatedEntity, NoKey, KeyPropagationPolicy.NoKey)
      val proj = proj1.projector match {
        case entityElement: DbEntityElement =>
          new ProjectionElement(
            proj1.select,
            OptionElement.wrapWithOptionElement(member.memberType, entityElement),
            proj1.key,
            KeyPropagationPolicy.NoKey,
            proj1.aggregator,
            entitledOnly = proj1.entitledOnly
          )
        case _ => proj1
      }

      val memberType = TypeInfo.replaceUnderlying(member.memberType, DALProvider.EntityRefType)
      val memberElem = getMemberElement(root, entity, MemberInfo(entity.projectedType, member.name, memberType))
      val relatedMemberElem = getMemberElement(
        proj1.projector,
        relatedEntity,
        MemberInfo(relatedEntity.projectedType, DALProvider.EntityRef, DALProvider.EntityRefType))
      val where = ElementFactory.equal(relatedMemberElem, OptionElement.underlying(memberElem))

      val newAlias = new TableAlias()
      val pc = ColumnProjector.projectColumns(translator.language, proj.projector, null, newAlias, proj.select.alias)
      val aggregator = Aggregator.getHeadAggregator(pc.projector.rowTypeInfo)
      val sel = new SelectElement(newAlias, pc.columns, proj.select, where, null, null, null, null, false, false)
      new ProjectionElement(
        sel,
        pc.projector,
        NoKey,
        KeyPropagationPolicy.NoKey,
        aggregator,
        entitledOnly = proj1.entitledOnly)
    } else {
      // c2p (child-to-parent) linkage
      val relatedEntity = mapping.getRelatedEntity(entity, member)
      val proj1 = getQueryElement(relatedEntity, NoKey, KeyPropagationPolicy.NoKey)

      val memberElem = getMemberElement(
        root,
        entity,
        MemberInfo(entity.projectedType, DALProvider.EntityRef, DALProvider.EntityRefType))
      val relatedMemberElem = getMemberElement(
        proj1.projector,
        relatedEntity,
        MemberInfo(relatedEntity.projectedType, DALProvider.ParentRef, DALProvider.EntityRefType))
      // linkage.parent_ref == root.dal$entityRef
      val refEqual = ElementFactory.equal(relatedMemberElem, memberElem)

      val propertyNameElem = getMemberElement(
        proj1.projector,
        relatedEntity,
        MemberInfo(relatedEntity.projectedType, DALProvider.ParentPropName, TypeInfo.STRING))
      // linkage.parent_prop_name == "xxx"
      val nameEqual = ElementFactory.equal(propertyNameElem, ElementFactory.constant(member.name, TypeInfo.STRING))
      val where = ElementFactory.andAlso(refEqual, nameEqual)

      val newAlias = new TableAlias()
      val pc = ColumnProjector.projectColumns(translator.language, proj1.projector, null, newAlias, proj1.select.alias)
      val sel = new SelectElement(newAlias, pc.columns, proj1.select, where, null, null, null, null, false, false)
      val childRefDesc = new RuntimeFieldDescriptor(
        relatedEntity.projectedType,
        DALProvider.ChildRef,
        Query.findShapeType(member.memberType))
      val childRef = ElementFactory.makeMemberAccess(pc.projector, childRefDesc)
      val unbound =
        new ProjectionElement(
          sel,
          childRef,
          NoKey,
          KeyPropagationPolicy.NoKey,
          null,
          Some(member.memberType.clazz),
          entitledOnly = proj1.entitledOnly)
      UnusedColumnRemover.remove(mapping, RelationshipBinder.bind(this, unbound))
    }
  }

  override def getMemberElement(root: RelationElement, entity: MappingEntity, member: MemberInfo): RelationElement = {
    if (mapping.isAssociationRelationship(entity, member)) {
      getAssociationElement(root, entity, member)
    } else
      root match {
        case a: AliasedElement if mapping.isColumn(entity, member) =>
          val de = entity.asInstanceOf[DALMappingEntity]
          de.getCompoundMembers(member.name)
            .map { members =>
              val columns = members.map { m =>
                val cn = mapping.getColumnName(entity, m)
                val ci = mapping.getColumnInfo(entity, m)
                createMemberElement(de, m.name, m.memberType, a.alias, cn, ci, de.getDefaultValue(m.name))
              }
              if (columns.size > 1) new TupleElement(columns) else columns.head
            }
            .getOrElse {
              val cn = mapping.getColumnName(entity, member)
              val ci = mapping.getColumnInfo(entity, member)
              createMemberElement(de, member.name, member.memberType, a.alias, cn, ci, de.getDefaultValue(member.name))
            }
        case _ =>
          binder.bindMember(root, member)
      }
  }

  private def createMemberElement(
      entity: DALMappingEntity,
      memberName: String,
      memberType: TypeInfo[_],
      alias: TableAlias,
      columnName: String,
      info: ColumnInfo,
      defaultValue: Option[Any]): RelationElement = {

    def isEmbeddableCollection(t: TypeInfo[_]): Boolean = {
      t <:< classOf[Iterable[_]] && t.typeParams.size == 1 &&
      (t.typeParams(0).clazz.getAnnotation(classOf[EmbeddableMetaDataAnnotation]) ne null)
    }

    if (memberType.clazz == classOf[Option[_]]) {
      defaultValue.foreach(v => {
        if (v != None)
          throw new RelationalUnsupportedException(
            "only None is allowed to add as default value of Option field in SafeTransformer")
      })
      OptionElement(createMemberElement(entity, memberName, memberType.typeParams.head, alias, columnName, info, None))
    } else if (memberType <:< classOf[Entity]) {
      val companion = EntityInfoRegistry.getCompanion(memberType.clazz).asInstanceOf[EntityCompanionBase[_]]
      val column = new ColumnElement(DALProvider.EntityRefType, alias, columnName, info)
      new DALHeapEntityElement(companion, memberType, List(column), List(columnName))
    } else {
      val c = new ColumnElement(memberType, alias, columnName, info)
      defaultValue
        .map(v => {
          val const = ElementFactory.constant(v, memberType)
          val bin = ElementFactory.equal(c, ElementFactory.constant(null, TypeInfo.ANY))
          ElementFactory.condition(bin, const, c, memberType)
        })
        .getOrElse {
          if (
            entity.projectedType <:< classOf[Entity] && isEmbeddableCollection(c.rowTypeInfo) && memberName.indexOf(
              '.') == -1
          ) {
            val m = entity.getMappedMember(DALProvider.VersionedRef)
            val ci = entity.getColumnInfo(m)
            val foreignKey = createMemberElement(entity, m.name, m.memberType, alias, m.name, ci, None)
            EmbeddableCollectionElement(entity.getMappedMember(memberName), c, foreignKey)
          } else c
        }
    }
  }

  override protected def buildEntityElement(
      entity: MappingEntity,
      members: List[RelationElement],
      memberNames: List[String]): RelationElement = {
    entity match {
      case DALEntity(_, t, _) =>
        val companion = EntityInfoRegistry.getCompanion(t.clazz).asInstanceOf[EntityCompanionBase[_]]
        new DALHeapEntityElement(companion, t, members, memberNames)
      case dl: DALLinkage =>
        val descriptors =
          members.zip(memberNames).map(t => new RuntimeFieldDescriptor(TypeInfo.ANY, t._2, t._1.rowTypeInfo))
        ElementFactory.makeNew(new RuntimeConstructorDescriptor(dl.projectedType), members, descriptors)
      case DALEmbeddable(owner, property, t) =>
        new EmbeddableCaseClassElement(owner, property, t, members, memberNames)
      case _ =>
        throw new RelationalUnsupportedException(s"Unsupported mapping entity: $entity")
    }
  }

  override def translate(element: RelationElement): RelationElement = {
    SpecialElementRewriter.rewrite(super.translate(element))
  }
}
