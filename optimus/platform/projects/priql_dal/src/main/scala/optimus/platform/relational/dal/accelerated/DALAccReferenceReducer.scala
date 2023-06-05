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

import optimus.platform.relational.RelationalUnsupportedException
import optimus.platform.relational.dal.DALProvider
import optimus.platform.relational.dal.core.DALReferenceReducer
import optimus.platform.relational.data.QueryCommand
import optimus.platform.relational.data.QueryTranslator
import optimus.platform.relational.data.mapping.MappingEntityLookup
import optimus.platform.relational.data.mapping.MemberInfo
import optimus.platform.relational.data.mapping.QueryBinder
import optimus.platform.relational.data.mapping.QueryMapper
import optimus.platform.relational.data.tree.ColumnElement
import optimus.platform.relational.data.tree.DALHeapEntityElement
import optimus.platform.relational.data.tree.DbEntityElement
import optimus.platform.relational.data.tree.OptionElement
import optimus.platform.relational.data.tree.ProjectionElement
import optimus.platform.relational.data.tree.SelectElement
import optimus.platform.relational.inmemory.MethodKeyOptimizer
import optimus.platform.relational.tree.ElementFactory
import optimus.platform.relational.tree.FuncElement
import optimus.platform.relational.tree.LambdaElement
import optimus.platform.relational.tree.MemberElement
import optimus.platform.relational.tree.MethodCallee
import optimus.platform.relational.tree.RelationElement
import optimus.platform.relational.tree.RuntimeMethodDescriptor
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReferenceHolder

class DALAccReferenceReducer(p: DALProvider) extends DALAccReducer(p) {
  import DALAccReferenceReducer._
  override def createMapping() = new DALAccReferenceMapping
  override def createLanguage(lookup: MappingEntityLookup) = new DALLanguage(lookup)

  protected override def buildInner(e: RelationElement): RelationElement = {
    val r = new DALAccReferenceReducer(provider)
    r.scope = scope
    r.nReaders = nReaders
    r.translator = translator
    r.executeOptions = executeOptions
    r.visitElement(e)
  }

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

object DALAccReferenceReducer {
  class DALAccReferenceMapping extends DALMapping {
    override def createMapper(translator: QueryTranslator): DALMapper = {
      new DALAccReferenceMapper(this, translator)
    }
  }

  private class DALAccReferenceMapper(m: DALMapping, t: QueryTranslator) extends DALMapper(m, t) {
    override val binder: QueryBinder = DALAccReferenceBinder

    override def translate(element: RelationElement): RelationElement = {
      val e = MethodKeyOptimizer.optimize(element)
      super.translate(e)
    }
  }

  private class DALAccReferenceBinder(mapper: QueryMapper, root: RelationElement) extends DALBinder(mapper, root) {
    def bind(e: RelationElement): RelationElement = {
      val ele = visitElement(e)
      val rewritedElement = referenceRewrite(ele)
      rewritedElement.getOrElse(e)
    }

    private def referenceRewrite(element: RelationElement): Option[ProjectionElement] = {
      element match {
        case x: ProjectionElement
            if x.projector.isInstanceOf[DbEntityElement] && x.projector.rowTypeInfo <:< classOf[Entity] =>
          mapToReferenceHolder(x)

        case _ => None
      }
    }

    private def mapToReferenceHolder(projection: ProjectionElement): Option[ProjectionElement] = {
      val LambdaElement(_, FuncElement(m: MethodCallee, args, inst), List(param)) =
        DALReferenceReducer.executeReferenceProjector
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
      val alias = nextAlias()
      val newBody = visitElement(body)
      val pc = projectColumns(newBody, alias, sel.alias)

      Some(
        new ProjectionElement(
          new SelectElement(alias, pc.columns, sel, null, null, null, null, null, false, false),
          pc.projector,
          projection.key,
          projection.keyPolicy,
          entitledOnly = projection.entitledOnly
        ))
    }
  }

  private object DALAccReferenceBinder extends QueryBinder {
    def bind(mapper: QueryMapper, e: RelationElement): RelationElement = {
      new DALAccReferenceBinder(mapper, e).bind(e)
    }

    override def bindMember(source: RelationElement, member: MemberInfo): RelationElement = {
      source match {
        case MemberElement(e: DbEntityElement, DALProvider.StorageInfo) if member.name == "txTime" =>
          // _.dal$storageInfo.txTime
          super.bindMember(e, MemberInfo(e.rowTypeInfo, DALProvider.StorageTxTime, DALProvider.StorageTxTimeType))
        case MemberElement(e: DbEntityElement, DALProvider.StorageInfo) if member.name == "versionedRef" =>
          // _.dal$storageInfo.asInstanceOf[DSIStorage].versionedRef
          super.bindMember(e, MemberInfo(e.rowTypeInfo, DALProvider.VersionedRef, DALProvider.VersionedRefType))
        case FuncElement(mc: MethodCallee, List(e: DbEntityElement), null)
            if member.name == "initiatingEvent" && mc.name == "entityEventView" =>
          // _.initiatingEvent
          super.bindMember(e, MemberInfo(e.rowTypeInfo, DALProvider.InitiatingEvent, member.memberType))
        case e: DALHeapEntityElement if e.members.size == 1 && member.name == DALProvider.EntityRef =>
          e.members.head
        case _ => super.bindMember(source, member)
      }
    }
  }
}
