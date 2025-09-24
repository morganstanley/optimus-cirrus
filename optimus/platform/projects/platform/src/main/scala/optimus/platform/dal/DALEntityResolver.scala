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
package optimus.platform.dal

import java.time.Instant
import optimus.config.RuntimeComponents
import optimus.dsi.util.OptimusConstantRegistry
import optimus.entity.EntityInfoRegistry
import optimus.graph.PropertyNode
import optimus.graph.cache.CacheFilter
import optimus.graph.cache.Caches
import optimus.graph.cache.CauseEntityResolverClosed
import optimus.platform.TemporalContext
import optimus.platform._
import optimus.platform.dal.session.InitSession
import optimus.platform.dal.session.RolesetMode
import optimus.platform.dsi.Feature.EstablishAllRolesWithSession
import optimus.platform.dsi.SupportedFeatures
import optimus.platform.dsi.bitemporal
import optimus.platform.dsi.bitemporal.DALQueryResultType
import optimus.platform.dsi.bitemporal.ExpressionQueryCommand
import optimus.platform.dsi.bitemporal.NoDSI
import optimus.platform.dsi.bitemporal.QueryReferenceResult
import optimus.platform.dsi.bitemporal.QueryResult
import optimus.platform.dsi.bitemporal.QueryResultSet
import optimus.platform.dsi.bitemporal.SelectResult
import optimus.platform.dsi.bitemporal.TemporalityQueryCommand
import optimus.platform.dsi.bitemporal.UnsupportedSessionException
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.relational.PriqlSettings
import optimus.platform.relational.RelationalException
import optimus.platform.relational.dal.EntityMultiRelation
import optimus.platform.relational.dal.persistence.DalRelTreeEncoder
import optimus.platform.relational.debugPrint.DebugPrintFormatter
import optimus.platform.relational.tree._
import optimus.platform.storable._
import optimus.platform.temporalSurface.operations.AndTemporalSurfaceQuery
import optimus.platform.temporalSurface.operations.OrTemporalSurfaceQuery
import optimus.platform.temporalSurface.operations.QueryByClass
import optimus.platform.temporalSurface.operations.QueryByNonUniqueKey
import optimus.platform.temporalSurface.operations.QueryByUniqueKey
import optimus.platform.temporalSurface.operations.TemporalSurfaceQuery
import optimus.session.utils.ClientEnvironment

object DALEntityResolver {
  doEnumerateQueryEntity.setCacheable(false)
  enumeratePersistentEntity.setCacheable(false)
  findByExpressionCommand.setCacheable(false)
}

@entity
class DALEntityResolver(override val dsi: bitemporal.DSI) extends ResolverImpl with InitSession with SessionFetcher {
  import EntityResolverReadImpl.mapResult

  override private[optimus] def initGlobalSessionEstablishmentTime(time: Instant): Unit = {
    val oldTt = ClientEnvironment.initGlobalSessionEstablishmentTime(this, time)
    if (oldTt.isDefined && !oldTt.get.equals(time)) {
      log.warn(s"The runtime is shared, and the first global session tt [$oldTt] will be used")
    }
  }

  override private[optimus] def forceGlobalSessionEstablishmentTimeTo(time: Instant): Option[Instant] = {
    ClientEnvironment.forceGlobalSessionEstablishmentTimeTo(this, time)
  }

  override private[optimus] def getGlobalSessionEstablishmentTime: Option[Instant] = {
    ClientEnvironment.getGlobalSessionEstablishmentTime(this)
  }

  @node @scenarioIndependent private[optimus] def getSession(tryEstablish: Boolean = true) =
    dsi.getSession(tryEstablish)

  /**
   * Reference query should only be executed with a given
   */
  @node @scenarioIndependent
  private[optimus] /*[platform]*/ def enumerateReferenceQuery[E <: Entity](
      query: ElementQuery,
      classNames: Seq[String],
      loadContext: TemporalContext): Iterable[ReferenceHolder[E]] = {
    val dsiResult = doEnumerateQueryEntity(query.element, classNames, loadContext, DALQueryResultType.StorableReference)
    val refs = mapResult(dsiResult) { case QueryReferenceResult(dt, value) =>
      value
    } getOrElse Nil
    val abstractType = query.element.rowTypeInfo.clazz.asSubclass(classOf[Entity]).asInstanceOf[Class[E]]

    refs map { case (r, tx, vref) =>
      EntityReferenceHolder.withKnownAbstractType(
        EntityReference.finalTypedRef(r.data, r.typeId),
        loadContext,
        tx,
        vref,
        abstractType)
    }
  }

  @node @scenarioIndependent
  private def doEnumerateQueryEntity(
      query: MultiRelationElement,
      classNames: Seq[String],
      loadContext: TemporalContext,
      resultType: DALQueryResultType.DALQueryResultType) = {
    val q = QueryTemporalityFinderHelper.generateTSQuery(query)
    val when = QueryTemporalityFinder.findQueryTemporality(loadContext, q)
    val encoder = new DalRelTreeEncoder
    val tree = ImmutableByteArray(encoder.encode(query).toByteArray)
    val prettyPrint = DebugPrintFormatter.format(query)
    val cmd =
      TemporalityQueryCommand(tree, classNames, when, bitemporal.DALDataType.Entity, resultType)(Some(prettyPrint))

    val dsiResult = executor.executeQuery(dsi, cmd)
    dsiResult
  }

  @node @scenarioIndependent
  private[optimus] /*[platform]*/ def enumerateQuery[E <: Entity](
      query: ElementQuery,
      classNames: Seq[String],
      loadContext: TemporalContext): Iterable[E] = {
    val dsiResult = doEnumerateQueryEntity(query.element, classNames, loadContext, DALQueryResultType.FullData)
    val blobs = mapResult(dsiResult) { case SelectResult(value) =>
      value
    } getOrElse Nil

    blobs.apar(maxConcurrency = AsyncInternals.DefaultConcurrency, runtimeChecks = false).map { e: PersistentEntity =>
      EntitySerializer.deserialize(e.serialized, loadContext, DSIStorageInfo.fromPersistentEntity(e)).asInstanceOf[E]
    }
  }
  @node @scenarioIndependent
  private[optimus] /*[platform]*/ def enumeratePersistentEntity[E <: Entity](
      query: MultiRelationElement,
      classNames: Seq[String],
      loadContext: TemporalContext): Iterable[PersistentEntity] = {

    val dsiResult = doEnumerateQueryEntity(query, classNames, loadContext, DALQueryResultType.FullData)
    val blobs = mapResult(dsiResult) { case SelectResult(value) =>
      value
    } getOrElse Nil

    blobs
  }

  @node @scenarioIndependent
  private[optimus] def findByExpressionCommand(command: ExpressionQueryCommand): QueryResultSet = {
    mapResult(executor.executeQuery(dsi, command)) { case QueryResult(value, metaData) =>
      new QueryResultSet(value, metaData)
    } getOrElse (throw new GeneralDALException("Unexpected query result."))
  }

  @node @scenarioIndependent
  private[optimus] /*[platform]*/ def findByExpressionCommandWithTemporalContext(
      command: ExpressionQueryCommand,
      loadContext: TemporalContext): QueryResultSet = {

    val resultSet = findByExpressionCommand(command)

    // If we have persistent entity in the result, we deserialize them into entity here before cache,
    // to reduce the memory pressure on the client side when it loads lots of data back
    val peFields = resultSet.metaData.peFields

    // if we only have one PE which need the deserialization, do it directly to avoid the apar overhead
    if (peFields.size == 1) {
      val peIndex = peFields.head
      resultSet.result.apar(PriqlSettings.concurrencyLevel).foreach { array =>
        array(peIndex) match {
          case pe: PersistentEntity =>
            array(peIndex) =
              EntitySerializer.deserialize(pe.serialized, loadContext, DSIStorageInfo.fromPersistentEntity(pe))
          case _ => // skip all the cases which are not PersistentEntity
        }
      }
    } else if (peFields.size > 1) {
      resultSet.result.apar(PriqlSettings.concurrencyLevel).foreach { array =>
        peFields.apar(PriqlSettings.concurrencyLevel).foreach { i =>
          array(i) match {
            case pe: PersistentEntity =>
              array(i) =
                EntitySerializer.deserialize(pe.serialized, loadContext, DSIStorageInfo.fromPersistentEntity(pe))
            case _ => // skip all the cases which are not PersistentEntity
          }
        }
      }
    }

    resultSet
  }

  override def close(): Unit = {
    dsi.close(true) // true; shutdown the full dsi

    // Very large log files problem should not reoccur but just in case:
    var nullSSLogged, nullEnvLogged = false

    /** null nodes are cleared from cache before we apply the filter (won't count towards those cleared here) */
    def filterCacheNodes(node: PropertyNode[_]): Boolean = {
      node.scenarioStack() match {
        case null =>
          if (!nullSSLogged) {
            log.error(s"Unexpected null scenarioStack on $node - contact Optimus Graph")
            nullSSLogged = true
          }
          false
        case ss if ss.isConstant =>
          // these don't have an associated environment and can stay
          false
        case ss =>
          val env = ss.env
          if (env ne null) env.entityResolver eq this
          else {
            if (!nullEnvLogged) {
              log.error(s"Unexpected null env on $node with scenarioStack $ss - contact Optimus Graph")
              nullEnvLogged = true
            }
            false
          }
      }
    }

    val cleared = Caches.clearCaches(CauseEntityResolverClosed, filter = CacheFilter(filterCacheNodes))
    log.debug(s"Cleared $cleared nodes from the cache when closing EntityResolver")
  }

  override def init(config: RuntimeComponents): Unit = {
    if (SessionFetcher.enableGlobalSessionTt) {
      dsi match {
        case clientSideDsi: ClientSideDSI => clientSideDsi.bindSessionFetcher(this)
        case _                            =>
      }
    }
    initSession(dsi, config.runtimeConfig)
  }

  @entersGraph
  protected[optimus] /*[platform]*/ override def reinitWithLocallyResolvedAllRoles(
      config: optimus.config.RuntimeComponents): Unit = {
    val allRoles =
      config.runtimeConfig.get(RuntimeProperties.DsiSessionRolesetModeProperty).contains(RolesetMode.AllRoles)
    if (allRoles) {
      val serverFeatures = dsi.serverFeatures()
      if (
        !serverFeatures.supports(EstablishAllRolesWithSession) ||
        !SupportedFeatures.myFeatures.supports(EstablishAllRolesWithSession)
      ) {
        // Broker will not have resolved roles in the initial session; we must explicitly request and reestablish.
        initSession(
          dsi,
          config.runtimeConfig,
          Some(RolesetMode.SpecificRoleset(Set(OptimusConstantRegistry.security.defaultRole)))
        )

        val rolesets: Seq[Set[String]] = Seq(DAL.entitlements.availableRoles)
        initSession(dsi, config.runtimeConfig, Some(RolesetMode.fromSeqSetString(rolesets)))
      }
    }
  }

  /** returns true only if this EntityResolver would return the same result from any call as the other */
  override def equivalentTo(other: EntityResolver): Boolean = (this eq other) ||
    (other match {
      case other: DALEntityResolver =>
        (dsi eq other.dsi) || ((dsi, other.dsi) match {
          // currently only implemented for partitioned DSI (which is what we always have in practice)
          case (p1: PartitionedDsiProxy, p2: PartitionedDsiProxy) =>
            // different regions (e.g. devln) of the DAL have the same data as long as the env.mode (e.g. dev) and the
            // contexts are the same. Also sessions (entitlements etc.) need to match otherwise one resolver might throw
            // entitlement errors when the other would not
            try {
              p1.env.mode == p2.env.mode && p1.baseContext == p2.baseContext &&
              relevantSessionInfo(dsi) == relevantSessionInfo(other.dsi)
            } catch {
              // some DSIs don't support getSession
              case _: UnsupportedSessionException => false
            }
          // returning false is always safe (might just cause lower performance if the other resolver was somehow
          // "better" than this, e.g. different region or zone)
          case _ => false
        })
      case _ => false
    })

  @entersGraph private def relevantSessionInfo(dsi: bitemporal.DSI): Product = {
    val info = dsi.getSession(true).establishingCommand.sessionInfo
    // appId, zone etc. doesn't affect the results of queries (only the performance) so we don't need to consider them
    (info.realId, info.effectiveId, info.rolesetMode, info.establishmentTime, info.onBehalfSessionToken)
  }
}

object QueryTemporalityFinderHelper {
  private def flattenBinaryExpressionElement(q: BinaryExpressionElement): List[BinaryExpressionElement] = {
    q match {
      case BinaryExpressionElement(
            BinaryExpressionType.BOOLAND,
            left: BinaryExpressionElement,
            right: BinaryExpressionElement,
            _) =>
        flattenBinaryExpressionElement(left) ++ flattenBinaryExpressionElement(right)
      case _ => List(q)
    }
  }

  private def getTemporalSurfaceQuery(
      companion: StorableCompanionBase[_],
      targetCls: Class[Entity],
      memberName: String,
      propertyValues: List[ConstValueElement]): TemporalSurfaceQuery = {
    val indexInfo =
      companion.info.indexes.find(t => t.queryable && ((t.propertyNames == Seq(memberName)) || (t.name == memberName)))
    val propertyMap =
      indexInfo.get.propertyNames zip propertyValues.map(
        _.value
      ) // since we get propertyValues via indexInfo.propertyNames { propName => val propVal = serializedKey.properties(propName)}
    val key = SerializedKey(
      targetCls.getName,
      propertyMap,
      indexInfo.get.unique,
      indexInfo.get.indexed,
      indexInfo.get.queryByEref)
    (key.unique, key.indexed) match {
      case (true, _)     => QueryByUniqueKey(null, key, entitledOnly = false)
      case (false, true) => QueryByNonUniqueKey(null, key)
      case _             => throw new RelationalException("only key or index query could be passed to server")
    }
  }

  private def getTemporalQuery(
      entity: EntityMultiRelation[_],
      q: BinaryExpressionElement): Option[TemporalSurfaceQuery] = {
    val companion = EntityInfoRegistry.getCompanion(entity.classEntityInfo.runtimeClass)
    val targetCls = entity.classEntityInfo.runtimeClass.asInstanceOf[Class[Entity]]

    val binaries = flattenBinaryExpressionElement(q)

    def handleContainsQuery(memberName: String, exprList: RelationElement): TemporalSurfaceQuery = exprList match {
      case ExpressionListElement(propertyValues: List[ConstValueElement @unchecked]) =>
        getTemporalSurfaceQuery(companion, targetCls, memberName, propertyValues)
    }

    def toTemporalSurfaceQuery(binaryExpr: BinaryExpressionElement): Option[TemporalSurfaceQuery] = {
      Option(binaryExpr match {
        case BinaryExpressionElement(
              BinaryExpressionType.ITEM_IS_IN,
              left: MemberElement,
              ExpressionListElement(exprList),
              _) =>
          if (exprList.isEmpty) null
          else
            exprList.tail.foldLeft(handleContainsQuery(left.memberName, exprList.head)) { (res, expr) =>
              OrTemporalSurfaceQuery(res, handleContainsQuery(left.memberName, expr))
            }
        case BinaryExpressionElement(
              BinaryExpressionType.EQ,
              left: MemberElement,
              ExpressionListElement(propertyValues: List[ConstValueElement @unchecked]),
              _) =>
          getTemporalSurfaceQuery(companion, targetCls, left.memberName, propertyValues)
      })
    }

    binaries.headOption.map { binary =>
      binaries.tail.foldLeft(toTemporalSurfaceQuery(binary)) { (res, expr) =>
        val next = toTemporalSurfaceQuery(expr)
        if (res.isEmpty) next
        else if (next.isEmpty) res
        else Some(AndTemporalSurfaceQuery(res.get, next.get))
      }
    }.get
  }

  def generateTSQuery(query: MultiRelationElement): TemporalSurfaceQuery = {
    query match {
      case MethodElement(
            QueryMethod.WHERE,
            List(
              MethodArg(_, entity: EntityMultiRelation[_]),
              MethodArg(_, ExpressionListElement(List(filter: BinaryExpressionElement)))),
            _,
            _,
            _) =>
        val query = getTemporalQuery(entity, filter)
        if (query.isDefined) query.get
        else QueryByClass(null, entity.classEntityInfo.runtimeClass)
      case MethodElement(QueryMethod.EXTRACT, List(MethodArg(_, entity: EntityMultiRelation[_]), _, _, _), _, _, _) =>
        QueryByClass(null, entity.classEntityInfo.runtimeClass)
      case MethodElement(
            QueryMethod.EXTRACT,
            List(
              MethodArg(
                _,
                MethodElement(
                  QueryMethod.WHERE,
                  List(
                    MethodArg(_, entity: EntityMultiRelation[_]),
                    MethodArg(_, ExpressionListElement(List(filter: BinaryExpressionElement)))),
                  _,
                  _,
                  _)),
              _,
              _,
              _),
            _,
            _,
            _) =>
        val query = getTemporalQuery(entity, filter)
        if (query.isDefined) query.get
        else QueryByClass(null, entity.classEntityInfo.runtimeClass)
      case EntityMultiRelation(classEntityInfo, _, _) =>
        QueryByClass(null, classEntityInfo.runtimeClass)
      case _ => throw new RelationalException("only filter extract or their combination could pass to server")
    }
  }
}
