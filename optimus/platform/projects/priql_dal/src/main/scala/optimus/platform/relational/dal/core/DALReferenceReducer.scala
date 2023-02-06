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

import optimus.platform.dal.DSIStorageInfo
import optimus.platform.dal.EntityResolverWriteImpl
import optimus.platform.EvaluationContext
import optimus.platform.NoKey
import optimus.platform.dsi.Feature
import optimus.platform.relational._
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.accelerated.DALAccExecutionProvider
import optimus.platform.relational.dal.internal.RawReferenceKey
import optimus.platform.relational.data.Aggregator
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.language.QueryDialect
import optimus.platform.relational.data.language.QueryLanguage
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.mapping.QueryMapping
import optimus.platform.relational.data.translation.AliasReplacer
import optimus.platform.relational.data.translation.OrderByRewriter
import optimus.platform.relational.data.tree.AliasedElement
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.DbEntityElement
import optimus.platform.relational.data.tree.OptionElement
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.MethodArg
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.MethodElement
import optimus.platform.relational.tree.QueryMethod
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.RuntimeMethodDescriptor
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReferenceHolder

class DALReferenceReducer(p: DALProvider) extends DALReducer(p) {
  import DALReferenceReducer._

  override def createMapping(): QueryMapping = new DALReferenceMapping
  override def createLanguage(lookup: MappingEntityLookup) = new DALReferenceLanguage(lookup)

  protected override def compileAndExecute(
      command: QueryCommand,
      projector: LambdaElement,
      proj: ProjectionElement): RelationElement = {
    proj.projector match {
      case f @ FuncElement(
            m: MethodCallee,
            List(_: ColumnElement, _, OptionElement(_: ColumnElement), OptionElement(_: ColumnElement)),
            null) if m.method.declaringType <:< EntityReferenceHolder.getClass && m.method.name == "apply" =>
        val abstractType = f.rowTypeInfo.typeParams.head.cast[Entity]
        provider.execute(
          command,
          Right(DALProvider.readEntityReferenceHolderWithVref(abstractType)),
          proj.key,
          m.method.returnType,
          // ExecuteReference does not throw entitlement check failed error, so it always comply to entitledOnly
          // this flag does not impact its behavior, so just use false to avoid duplicate cache
          executeOptions.copy(entitledOnly = false)
        )
      case f @ FuncElement(m: MethodCallee, List(_: ColumnElement, _, OptionElement(_: ColumnElement)), null)
          if m.method.declaringType <:< EntityReferenceHolder.getClass && m.method.name == "apply" =>
        val abstractType = f.rowTypeInfo.typeParams.head.cast[Entity]
        provider.execute(
          command,
          Right(DALProvider.readEntityReferenceHolder(abstractType)),
          proj.key,
          m.method.returnType,
          executeOptions.copy(entitledOnly = false))
      case _ =>
        throw new RelationalUnsupportedException(
          "Unsupported projector, DAL reference query must return a ReferenceHolder")
    }
  }
}

object DALReferenceReducer {
  private val arrangeKey = RawReferenceKey((t: EntityReferenceHolder[_]) => t.ref)
  val executeReferenceWithVrefProjector = reifyLambda { e: Entity =>
    EntityReferenceHolder[Entity](
      e.dal$entityRef,
      e.dal$temporalContext,
      Option(e.dal$storageInfo.txTime),
      Option(e.dal$storageInfo.asInstanceOf[DSIStorageInfo].versionedRef))
  } get

  val executeReferenceWithoutVrefProjector = reifyLambda { e: Entity =>
    EntityReferenceHolder[Entity](e.dal$entityRef, e.dal$temporalContext, Option(e.dal$storageInfo.txTime))
  } get

  def executeReferenceProjector = {
    val resolver = EvaluationContext.env.entityResolver.asInstanceOf[EntityResolverWriteImpl]
    val execRefWithVref = resolver.serverFeatures.supports(Feature.ExecuteRefQueryWithVersionedRef)
    if (execRefWithVref) executeReferenceWithVrefProjector else executeReferenceWithoutVrefProjector
  }

  class DALReferenceLanguage(l: MappingEntityLookup) extends DALLanguage(l) {
    override def canBeWhere(e: RelationElement): Boolean = {
      val result = super.canBeWhere(e)
      if (!result)
        throw new RelationalUnsupportedException(
          "DAL reference query only supports '==' or 'contains' or combinations with '&&' on indexed fields in filter.")
      result
    }

    override def createDialect(translator: QueryTranslator): QueryDialect = {
      new DALReferenceDialect(this, translator)
    }
  }

  private class DALReferenceDialect(lan: QueryLanguage, tran: QueryTranslator) extends DALDialect(lan, tran) {
    override def translate(e: RelationElement): RelationElement = {
      val elem = OrderByRewriter.rewrite(language, e)
      elem
    }
  }

  class DALReferenceMapping extends DALMapping {
    override def createMapper(translator: QueryTranslator): DALMapper = {
      new DALReferenceMapper(this, translator)
    }
  }

  private class DALReferenceMapper(m: DALMapping, t: QueryTranslator) extends DALMapper(m, t) {
    override val binder: QueryBinder = DALReferenceBinder
  }

  private class DALReferenceBinder(mapper: QueryMapper, root: RelationElement) extends DALBinder(mapper, root) {
    override def bind(e: RelationElement): RelationElement = {
      visitElement(e) match {
        case x: ProjectionElement =>
          val p @ ProjectionElement(sel, prj, _) = mapToReferenceHolder(x)
          if ((sel.take eq null) && (sel.orderBy ne null))
            throw new RelationalUnsupportedException(
              "DAL reference query only supports 'sortByTT/sortByTTDesc' with 'take/takeRight' behind.")

          if (sel.take ne null)
            new ConditionOptimizer().optimize(p)
          else {
            val projWithFakeAggregator =
              new ProjectionElement(
                sel,
                prj,
                p.key,
                p.keyPolicy,
                Aggregator.getHeadAggregator(p.rowTypeInfo),
                entitledOnly = p.entitledOnly)
            new ConditionOptimizer().optimize(projWithFakeAggregator) match {
              case ProjectionElement(newSel, newPrj, _) =>
                val newProj = new ProjectionElement(newSel, newPrj, p.key, p.keyPolicy, entitledOnly = p.entitledOnly)
                new MethodElement(QueryMethod.ARRANGE, List(MethodArg("src", newProj)), p.rowTypeInfo, arrangeKey)
              case x => x // empty ScalaTypeMultiRelation
            }
          }
        case p: DALAccExecutionProvider[_] => p
        case _ =>
          throw new RelationalUnsupportedException("DAL reference query requires whole query to run on the server side")
      }
    }

    private def mapToReferenceHolder(projection: ProjectionElement): ProjectionElement = {
      val LambdaElement(_, FuncElement(m: MethodCallee, args, inst), List(param)) = executeReferenceProjector
      // fix up the body type, since the template we use has type ReferenceHolder[Entity]
      val desc = new RuntimeMethodDescriptor(
        m.method.declaringType,
        m.method.name,
        m.resType.copy(typeParams = projection.rowTypeInfo :: Nil))

      // Replace e.dal$temporalContext with null since the value is read from pickledInputStream.temporalContext.
      val newArgs = args.updated(1, ElementFactory.constant(null, DALProvider.TemporalContextType))
      val body = ElementFactory.call(inst, desc, newArgs)
      map.put(param, projection.projector)
      val sel = projection.select
      val alias = sel.from.asInstanceOf[AliasedElement].alias
      val b = AliasReplacer.replace(visitElement(body), sel.alias, alias)
      val pc = projectColumns(b, sel.alias, alias)
      val newSel = updateSelect(sel, sel.from, sel.where, sel.orderBy, sel.groupBy, sel.skip, sel.take, pc.columns)
      new ProjectionElement(newSel, pc.projector, NoKey, projection.keyPolicy, entitledOnly = projection.entitledOnly)
    }

    override protected def bindUntype(method: MethodElement): RelationElement = {
      throw new RelationalUnsupportedException("DAL reference query doesn't support 'untype' operator")
    }

    override protected def bindPermitTableScan(method: MethodElement): RelationElement = {
      visitSource(method.methodArgs.head.param)
    }
  }

  private object DALReferenceBinder extends QueryBinder {
    def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
      new DALReferenceBinder(mapper, e).bind(e)
    }

    override def bindMember(source: RelationElement, member: MemberInfo): RelationElement = {
      source match {
        case MemberElement(e: DbEntityElement, DALProvider.StorageInfo) if member.name == "txTime" =>
          // _.dal$storageInfo.txTime
          super.bindMember(e, MemberInfo(e.rowTypeInfo, DALProvider.StorageTxTime, DALProvider.StorageTxTimeType))
        case MemberElement(e: DbEntityElement, DALProvider.StorageInfo) if member.name == "versionedRef" =>
          // _.dal$storageInfo.asInstanceOf[DSIStorage].versionedRef
          super.bindMember(e, MemberInfo(e.rowTypeInfo, DALProvider.VersionedRef, DALProvider.VersionedRefType))

        case e: DALHeapEntityElement =>
          e.memberNames
            .zip(e.members)
            .collectFirst {
              case (name, e) if name == member.name => e
            }
            .getOrElse(makeMemberAccess(source, member))

        case _ => super.bindMember(source, member)
      }
    }
  }
}
