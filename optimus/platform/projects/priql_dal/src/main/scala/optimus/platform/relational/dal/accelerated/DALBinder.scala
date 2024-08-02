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

import java.util
import optimus.platform.NoKey
import optimus.platform.Query
import optimus.platform.annotations.internal._projected
import optimus.platform.relational.KeyPropagationPolicy
import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.RichZonedDateTime
import optimus.platform.relational.TraversableLikeOps
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.DALQueryMethod
import optimus.platform.relational.dal.core.DALMappingEntity
import optimus.platform.relational.dal.core.SpecialElementRewriter
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.translation.ColumnCollector
import optimus.platform.relational.data.translation.ColumnProjector
import optimus.platform.relational.data.translation.ComparisonRewriter
import optimus.platform.relational.data.translation.DefaultBinder
import optimus.platform.relational.data.translation.ElementReplacer
import optimus.platform.relational.data.tree._
import optimus.platform.relational.tree._
import optimus.platform.storable.Entity
import optimus.platform.util.ZonedDateTimeOps

class DALBinder protected (mapper: QueryMapper, root: RelationElement) extends DefaultBinder(mapper, root) {
  import DALBinder._

  private val projected = new util.IdentityHashMap[ProjectionElement, RelationElement]

  protected override def handleQuerySrc(provider: ProviderRelation): RelationElement = {
    super.handleQuerySrc(provider) match {
      case p: ProjectionElement =>
        projected.put(p, provider)
        p
      case x => x
    }
  }

  protected override def handleMemberRef(member: MemberElement): RelationElement = {
    member.member.metadata.get(Query.ReifiedTarget) map { case LambdaElement(_, body, List(p)) =>
      visitElement(DefaultBinder.ParameterReplacer.replace(body, p, member.instanceProvider))
    } getOrElse { super.handleMemberRef(member) }
  }

  protected override def handleFuncCall(func: FuncElement): RelationElement = {
    func.callee match {
      // TODO (OPTIMUS-26009): Update following code/comments accordingly.
      case mc: MethodCallee
          if mc.method.declaringType <:< classOf[RichZonedDateTime] && mc.name == "richEqualInstant" =>
        func.instance match {
          // Converts
          //      "RichZonedDateTime.<ctr>(zdt).richEqualInstant(otherZdt)" =>
          //          "ZonedDateTimeOps.equalInstant(zdt, otherZdt)"
          case fe: FuncElement if fe.arguments.size == 1 =>
            require(func.arguments.size == 1, "Expected richEqualInstant to accept only a single argument.")
            val methodCallee = new MethodCallee(ZonedDateTimeOps.runtimeMethodDescriptor)
            new FuncElement(methodCallee, List(fe.arguments.head, func.arguments.head), null)
          case _ => super.handleFuncCall(func)
        }
      case mc: MethodCallee if func.instance ne null =>
        mc.method.metadata.get(Query.ReifiedTarget) map { case LambdaElement(_, body, params) =>
          visitElement(ElementReplacer.replace(body, params.toList, func.instance :: func.arguments))
        } getOrElse { super.handleFuncCall(func) }
      case _ => super.handleFuncCall(func)
    }
  }

  protected override def handleMethod(method: MethodElement): RelationElement = {
    method.methodCode match {
      case DALQueryMethod.ProjectedViewOnly => bindProjectedViewOnly(method)
      case DALQueryMethod.SortByTT          => bindSortByTT(method)
      case _                                => super.handleMethod(method)
    }
  }

  protected def bindProjectedViewOnly(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection ne null) {
      projected.put(projection, method)
      projection
    } else throw new RelationalUnsupportedException("Violate the 'projected-only' execution constraint.")
  }

  protected def bindSortByTT(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = visitElement(s.param)
    val projection = convertToSequence(source)
    if (
      (projection ne null) && projection.projector
        .isInstanceOf[DbEntityElement] && projection.projector.rowTypeInfo <:< classOf[Entity]
    ) {
      val ConstValueElement(isAsc: Boolean, _) = others.head.param
      val select = projection.select
      val alias = nextAlias()
      val pc = projectColumns(projection.projector, alias, select.alias)
      val txColumn = pc.columns.find(p => p.name.startsWith(DALProvider.StorageTxTime)).get.element
      val txOrder = List(OrderDeclaration(if (isAsc) SortDirection.Ascending else SortDirection.Descending, txColumn))
      new ProjectionElement(
        new SelectElement(alias, pc.columns, select, null, txOrder, null, null, null, false, false),
        pc.projector,
        method.key,
        projection.keyPolicy,
        entitledOnly = projection.entitledOnly
      )
    } else {
      if (source eq s.param) method else replaceArgs(method, others, source)
    }
  }

  protected override def bindPermitTableScan(method: MethodElement): RelationElement = {
    super.bindPermitTableScan(method) match {
      case p: ProjectionElement =>
        projected.put(p, method)
        p
      case x => x
    }
  }

  protected override def bindDistinct(method: MethodElement): RelationElement = {
    val (s :: others) = method.methodArgs
    val source = super.visitElement(s.param)
    val projection = convertToSequence(source)
    if (projection eq null) {
      if (source eq s.param) method else replaceArgs(method, others, source)
    } else {
      val alias = nextAlias()
      val pc = projectColumns(projection.projector, alias, projection.select.alias)
      val proj = new ProjectionElement(
        new SelectElement(alias, pc.columns, projection.select, null, null, null, null, null, true, false),
        pc.projector,
        method.key,
        projection.keyPolicy,
        entitledOnly = projection.entitledOnly
      )
      pc.projector match {
        case e: DbEntityElement if e.rowTypeInfo <:< classOf[Entity] => proj
        case _: ColumnElement                                        => proj
        case OptionElement(_: ColumnElement)                         => proj
        case _ =>
          if ((source eq s.param) && method.methodCode == QueryMethod.TAKE_DISTINCT_BYKEY) method
          else {
            // in this case, we do both server-side and client-side distinct
            new MethodElement(
              QueryMethod.TAKE_DISTINCT_BYKEY,
              List(MethodArg("src", proj)),
              proj.rowTypeInfo,
              proj.key,
              proj.pos)
          }
      }
    }
  }

  protected override def getGroupKeyColumns(keyElem: RelationElement, alias: TableAlias): List[RelationElement] = {
    val canBeGroupByColumn = new CanBeGroupByColumn(lookup)
    val keyProj = ColumnProjector.projectColumns(canBeGroupByColumn, keyElem, Nil, alias, alias)
    keyProj.columns.map(_.element)
  }

  protected override def getGroupKeyElement(select: SelectElement, projector: RelationElement): RelationElement = {
    val newProj = ComparisonRewriter.rewriteGroupKey(projector)
    val searchScope = new ExpressionListElement(List(select, newProj))
    SpecialElementRewriter.rewrite(newProj, Some(searchScope))
  }

  protected override def convertToSequence(e: RelationElement): ProjectionElement = {
    import DALMappingEntityFactory._

    e match {
      case EmbeddableCollectionElement(info, _, foreignKey) if isProjectedEmbeddableCollection(info.memberType) =>
        val owner = lookup.getEntity(info.reflectType)
        val embeddable = lookup.getRelatedEntity(owner, info)
        val proj = mapper.getQueryElement(embeddable, NoKey, KeyPropagationPolicy.NoKey)
        val memberInfo = MemberInfo(embeddable.projectedType, DALProvider.VersionedRef, DALProvider.VersionedRefType)
        val relatedMemberElem = mapper.getMemberElement(proj.projector, embeddable, memberInfo)
        val where = ElementFactory.equal(relatedMemberElem, foreignKey)
        val newAlias = new TableAlias()
        val pc =
          ColumnProjector.projectColumns(language, excludeVRef(proj.projector), null, newAlias, proj.select.alias)
        val sel = new SelectElement(newAlias, pc.columns, proj.select, where, null, null, null, null, false, false)
        new ProjectionElement(
          sel,
          pc.projector,
          NoKey,
          KeyPropagationPolicy.NoKey,
          null,
          viaCollection = Some(e.projectedType().clazz),
          entitledOnly = proj.entitledOnly)

      case FuncElement(
            m1: MethodCallee,
            List(ConstValueElement(target: TypeInfo[_], _), _),
            FuncElement(m2, List(x), null))
          if m1.name == "ofType" && m1.method.declaringType <:< classOf[
            TraversableLikeOps[AnyRef, List]] && m2.name == "TraversableLikeOps" =>
        x match {
          case e @ EmbeddableCollectionElement(info, _, _) =>
            val newInfo = MemberInfo(info.reflectType, info.name, info.memberType.copy(typeParams = List(target)), None)
            convertToSequence(e.copy(info = newInfo))

          case p: ProjectionElement if target <:< classOf[Entity] && DALProvider.isProjectedEntity(target.clazz) =>
            // the logic is similar to bindFlatMap, we have to construct the right proj here
            val targetEntity = lookup.getEntity(target)
            val proj = mapper.getQueryElement(targetEntity, NoKey, KeyPropagationPolicy.NoKey)
            val memberInfo = MemberInfo(target, DALProvider.EntityRef, DALProvider.EntityRefType)
            val relatedMemberElem = mapper.getMemberElement(proj.projector, targetEntity, memberInfo)
            val foreignKeyOpt = ColumnCollector.collect(p.projector).collectFirst {
              case d: DALHeapEntityElement => d.members.head
              case c: ColumnElement        => c
            }
            foreignKeyOpt
              .map { foreignKey =>
                val on = ElementFactory.equal(relatedMemberElem, foreignKey)
                val join = new JoinElement(JoinType.InnerJoin, p.select, proj.select, on)
                val alias = nextAlias()
                val pc = projectColumns(proj.projector, alias, p.select.alias, proj.select.alias)
                new ProjectionElement(
                  new SelectElement(alias, pc.columns, join, null, null, null, null, null, false, false),
                  pc.projector,
                  p.key,
                  p.keyPolicy,
                  null,
                  p.viaCollection,
                  entitledOnly = p.entitledOnly
                )
              }
              .getOrElse(null)

          case _ => null
        }

      case _ =>
        super.convertToSequence(e)
    }
  }

  override protected def convertToLinkageProj(
      e: RelationElement,
      viaLinkage: Option[Class[_]]): Option[RelationElement] = {
    val e1 = LinkageSubqueryRewriter.rewrite(mapper.mapping, language, e)
    super.convertToLinkageProj(e1, viaLinkage)
  }
}

object DALBinder extends QueryBinder {

  def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
    val binder = new DALBinder(mapper, e)
    val bound = binder.visitElement(e)
    new SingleTableReverter(binder.projected).visitElement(bound)
  }

  override def bindMember(source: RelationElement, member: MemberInfo): RelationElement = {
    source match {
      case FuncElement(mc: MethodCallee, List(e: DbEntityElement), null)
          if member.name == "initiatingEvent" && mc.name == "entityEventView" =>
        // _.initiatingEvent
        super.bindMember(e, MemberInfo(e.rowTypeInfo, DALProvider.InitiatingEvent, member.memberType))
      case e: DALHeapEntityElement if e.members.size == 1 && member.name == DALProvider.EntityRef =>
        e.members.head
      case _ => super.bindMember(source, member)
    }

  }

  override def bindUntypedMember(
      source: RelationElement,
      memberName: String,
      lookup: MappingEntityLookup): Option[RelationElement] = {
    source match {
      case e: DALHeapEntityElement =>
        e.memberNames.zip(e.members).collectFirst {
          case (name, m) if name == memberName =>
            if (TypeInfo.underlying(m.rowTypeInfo) <:< DALProvider.EntityRefType) {
              val de = lookup.getEntity(e.rowTypeInfo).asInstanceOf[DALMappingEntity]
              val realType = de.getOriginalType(memberName)
              makeMemberAccess(e, MemberInfo(e.rowTypeInfo, memberName, realType))
            } else m
        }
      case _ =>
        super.bindUntypedMember(source, memberName, lookup)
    }
  }

  private class SingleTableReverter(map: util.IdentityHashMap[ProjectionElement, RelationElement])
      extends DbQueryTreeVisitor {
    protected override def handleProjection(proj: ProjectionElement): RelationElement = {
      if (proj.select.from.isInstanceOf[TableElement]) map.get(proj) else proj
    }
  }

  private class CanBeGroupByColumn(l: MappingEntityLookup) extends DALLanguage(l) {
    override def canBeColumn(e: RelationElement): Boolean = {
      def doCheck(elem: RelationElement) = elem match {
        case c: ConditionalElement         => canBeConditionalColumn(c)
        case _: ColumnElement              => true
        case _: DbEntityElement            => true
        case _: DALHeapEntityElement       => true
        case _: EmbeddableCaseClassElement => true
        case m: MemberElement
            if m.member.declaringType.clazz.getMethod(m.member.name).getAnnotation(classOf[_projected]) != null =>
          val e = lookup.getEntity(m.member.declaringType).asInstanceOf[DALMappingEntity]
          e.isMapped(m.member.name)
        case _ => false
      }

      e match {
        case o: OptionElement => doCheck(o.element) // we only allow OptionElement at top level
        case _                => doCheck(e)
      }
    }
  }
}
