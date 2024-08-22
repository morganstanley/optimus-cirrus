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
package optimus.platform.relational.reactive

import java.util.Collections
import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, LinkedBlockingQueue}

import msjava.slf4jutils.scalalog._
import optimus.entity.{ClassEntityInfo, IndexInfo}
import optimus.graph.Settings
import optimus.platform._
import optimus.platform.dal.ClientSideDSI
import optimus.platform.dal.EntityEventModule.{EntityBitemporalSpaceImpl, EntityOps}
import optimus.platform.dal.GeneralDALException
import optimus.platform.dal.RangeQueryOptions
import optimus.platform.dsi.bitemporal
import optimus.platform.dsi.bitemporal.DSIQueryTemporality.OpenVtTxRange
import optimus.platform.dsi.bitemporal._
import optimus.platform.relational.tree._
import optimus.platform.storable._

import scala.util.{Failure, Success, Try}

sealed trait RangeQueryResult[T] {
  // TODO (OPTIMUS-17009): using Try here would cause several sync stacks, need to switch to
  //                                                       NodeResult
  val result: Try[Map[EntityReference, Seq[T]]]
  def isSuccess = result.isSuccess
  def isFailure = result.isFailure
  def isEmpty = result.isSuccess && result.get.isEmpty
  def get = result.get
  def getOrElse(default: => Map[EntityReference, Seq[T]]) = result.getOrElse(default)
  def failed = result.failed
  def recoverWith(f: PartialFunction[Throwable, RangeQueryResult[PersistentEntity]]) = {
    result match {
      case Failure(exception) =>
        if (f isDefinedAt exception)
          f(exception)
        else
          this
      case Success(_) => this
    }
  }
}

final case class HistoricalAPIQueryResult[E <: Entity](
    override val result: Try[Map[EntityReference, Seq[EntityChange[E]]]])
    extends RangeQueryResult[EntityChange[E]]

final case class LegacyAPIQueryResult(override val result: Try[Map[EntityReference, Seq[PersistentEntity]]])
    extends RangeQueryResult[PersistentEntity]

private[optimus] class RangeQuery(
    private val catchupQuery: MultiRelationElement,
    ttRange: TimeInterval
) extends ExpressionNormalizer {
  import RangeQuery.logger

  // hack around for type parameter that historical API needed
  // we would like to call the function in this way:
  // val entityTypeInfo = catchupQuery.rowTypeInfo.asInstanceOf[TypeInfo[_ <: Entity]]
  // DAL.resolver.findByIndexInRange[entityTypeInfo.Type](key, fromTc, toTc)
  // ide saying compilation passed. but no class generated
  // TODO (https://issues.scala-lang.org/browse/SI-6255): pending typesafe fix recursive Manifest stack-overflow bug
  val entityTypeInfo = catchupQuery.rowTypeInfo.asInstanceOf[TypeInfo[Entity]]
  type EntityType = entityTypeInfo.Type

  private val processed = ReactiveQueryProcessor.buildFromRelation(catchupQuery)

  /**
   * nonUniqueIndexQueries is a special case of serverSideQueries, whose range query can be implemented by
   * findByIndexInRange
   */
  val nonUniqueIndexQueries = (processed.nonUniqueindexedFilter map {
    convertToKey(_, processed.entityInfo)
  }).getOrElse {
    logger.info(s"No nonUnique index query found from $catchupQuery")
    Nil
  }

  lazy val serverSideQueries = processed.allIndexAndKeyFilter map { f =>
    convertToQuery(f, processed.entityInfo)
  } getOrElse {
    logger.warn("No top-level indexed (==) filter is found in the query, full class query will be executed")
    EntityClassQuery(processed.entityInfo.runtimeClass.getName) :: Nil
  }

  lazy val restFiltersForNonUniqueIndex = processed.restFiltersForNonUniqueIndex
  lazy val restFiltersForIndex = processed.restFiltersForIndex

  @async def nonUniqueIndexQueryServerSideChanges: LegacyAPIQueryResult = {
    import scala.jdk.CollectionConverters._

    val result: Try[Map[EntityReference, Seq[PersistentEntity]]] = asyncResult {
      val builder = new ConcurrentHashMap[EntityReference, BlockingQueue[PersistentEntity]]
      val fromTc = TemporalContext(TimeInterval.NegInfinity, ttRange.from)
      val toTc = TemporalContext(TimeInterval.Infinity, ttRange.to)

      nonUniqueIndexQueries.apar.map { key =>
        val entityVersionHolders = DAL.resolver.findByIndexInRange(key.asInstanceOf[Key[EntityType]], fromTc, toTc)
        entityVersionHolders.apar.map {
          case entityVersionHolder: EntityVersionHolder[EntityType] =>
            val eref = entityVersionHolder.bitempSpace.eref
            val pes = RangeQuery.getPEFromVersionHolder(entityVersionHolder)
            val prev = builder.putIfAbsent(eref, new LinkedBlockingQueue[PersistentEntity](pes.asJava))
            if (prev != null) {
              prev.addAll(pes.asJava)
            }
          case _ =>
            logger.error(
              "There shouldn't be other subclass of " +
                "VersionHolder except for EntityVersionHolder")
            throw new RuntimeException(
              "There shouldn't be other subclass of " +
                "VersionHolder except for EntityVersionHolder")
        }
      }
      val res = builder.asScala.map { case (eref, pes) =>
        (eref, pes.asScala.toList)
      }.toMap
      res
    }.toTry
    LegacyAPIQueryResult(result)
  }

  // Note marked as SI because it is ( so a complie time validation applies ).
  // It is a raw node on a non entity so the result will not be cached
  @scenarioIndependent @node def serverSideChanges: HistoricalAPIQueryResult[EntityType] = {
    import scala.jdk.CollectionConverters._
    import java.util.{Set => JavaSet, HashMap => JavaHashMap}
    import java.lang.{Boolean => JavaBoolean}

    // TODO (OPTIMUS-12675): Currently, there're two exactly same copy for each of
    // the PersistentEntity returned by EntityResolverReadImpl.rangeQuery
    // DAL team should fix the API so that it won't duplicate the results
    lazy val builder = new JavaHashMap[EntityReference, JavaSet[EntityChange[EntityType]]]
    val rawResult = Try {
      val res = serverSideQueries.head match {
        case EntityClassQuery(classNameStr, _) =>
          val fromTc = TemporalContext(TimeInterval.NegInfinity, ttRange.from)
          val toTc = TemporalContext(TimeInterval.Infinity, ttRange.to)
          val rangeQueryOpts = RangeQueryOptions(None, Option(Settings.rangeQueryOptsUser), true)
          val result =
            EntityOps
              .getEntityChangesWithoutManifest[EntityType](fromTc, toTc, typeName = classNameStr, rangeQueryOpts)
          val flatten = result.flatten.groupBy(entityChange => entityChange.eref)
          flatten
        case _: SerializedKeyQuery =>
          // combine contains into one API call
          // Cannot change entityTypeInfo.classes.head.asInstanceOf[Class[EntityType]] to classOf[EntityType] for it
          // will cause runtime class cast exception
          val result = EntityOps.getEntityChangesInRangeWithFilters[EntityType](
            Seq(serverSideQueries.map {
              case q: SerializedKeyQuery => q.key
              case _ => throw new IllegalArgumentException("query shouldn't be other type than SerializedKeyQuery")
            }.toSeq),
            entityTypeInfo.classes.head.asInstanceOf[Class[EntityType]],
            OpenVtTxRange(ttRange)
          )
          val flatten = result.flatten.groupBy(entityChange => entityChange.eref)
          flatten
        case unsupportedQuery =>
          throw new UnsupportedOperation(unsupportedQuery.toString)
      }
      res foreach { case (eref, changes) =>
        if (changes.nonEmpty) {
          builder.putIfAbsent(
            eref,
            Collections.newSetFromMap(new ConcurrentHashMap[EntityChange[EntityType], JavaBoolean]()))
          builder.get(eref).addAll(changes.asJava)
        }
      }
      builder.asScala.map { case (eref, pes) =>
        (eref, pes.asScala.toList)
      }.toMap
    }

    HistoricalAPIQueryResult(rawResult)
  }

  // Note marked as SI because it is ( so a complie time validation applies ).
  // It is a raw node on a non entity so the result will not be cached
  // TODO (OPTIMUS-18021): decommission of recoverWithManualRangeDiff, legacy
  // rangequery, findByIndexInRange
  @scenarioIndependent @node def serverSideChangesLegacy: LegacyAPIQueryResult = {
    import scala.collection.mutable

    val rawResult = serverSideQueries.head match {
      case query: EntityClassQuery =>
        DAL.resolver.rangeQuery(query, ttRange) :: Nil
      case _: SerializedKeyQuery =>
        serverSideQueries.apar.map {
          case query: SerializedKeyQuery => DAL.resolver.rangeQuery(query, ttRange)
          case _ => Failure(new IllegalArgumentException("query shouldn't be other type than SerializedKeyQuery"))
        }
      case unsupportedQuery => Failure(new UnsupportedOperation(unsupportedQuery.toString)) :: Nil
    }

    val res: Try[Map[EntityReference, Seq[PersistentEntity]]] = Try {
      val resultBuilder = mutable.Map.empty[EntityReference, mutable.Set[PersistentEntity]]
      rawResult.foreach {
        case Success(currentMap) =>
          currentMap foreach { case (eref, changes: Seq[PersistentEntity]) =>
            // TODO (OPTIMUS-12675): Currently, there are two identical copies
            // for each of the PersistentEntity returned by EntityResolverReadImpl.rangeQuery
            // DAL team should fix the API so that it won't duplicate the results
            val existing = resultBuilder.getOrElse(eref, mutable.Set.empty[PersistentEntity])
            resultBuilder.update(eref, existing ++= changes)
          }
        case Failure(throwable) =>
          throw throwable
      }
      resultBuilder.map { case (eref: EntityReference, changes: mutable.Set[PersistentEntity]) =>
        (eref, changes.toList)
      }.toMap
    }

    LegacyAPIQueryResult(res)
  }

  private def convertToQuery(elem: RelationElement, entityInfo: ClassEntityInfo): Iterable[bitemporal.Query] = {

    def buildIndexQuery(indexInfo: optimus.entity.IndexInfo[_, _], propValue: Any): bitemporal.Query = {
      val key = indexInfo match {
        case i: IndexInfo[_, Any @unchecked] => i.makeKey(propValue)
      }
      SerializedKeyQuery(key.toSerializedKey)
    }

    def extractUniqueQueries(member: MemberElement, param: RelationElement, entityInfo: ClassEntityInfo) = {
      entityInfo.indexes.find(_.name == member.memberName) match {
        case Some(indexInfo) =>
          param match {
            case ConstValueElement(value, info) =>
              value match {
                case l: Iterable[_] =>
                  l.collect { case elem => buildIndexQuery(indexInfo, elem) }
                case _ => throw new UnsupportedOperand("contains range query only support Iterable parameter")
              }
            case _ => throw new UnsupportedOperand("contains range query only support ConstValueElement parameter")
          }
        case None =>
          throw new RuntimeException(
            s"Member element ${member.memberName} is not an index on ${entityInfo.runtimeClass}")
      }
    }

    elem match {
      case exp: BinaryExpressionElement =>
        exp.op match {
          case BinaryExpressionType.EQ =>
            val (member, _, param) = normalize(exp.left, exp.op, exp.right)

            entityInfo.indexes.find(_.name == member.memberName) match {
              case Some(indexInfo) =>
                buildIndexQuery(indexInfo, getConstOrFreevar(param)) :: Nil
              case None =>
                throw new RuntimeException(
                  s"Member element ${member.memberName} is not an index on ${entityInfo.runtimeClass}")
            }
          case BinaryExpressionType.ITEM_IS_IN =>
            val (member, _, param) = normalize(exp.left, exp.op, exp.right)
            extractUniqueQueries(member, param, entityInfo)

          case _ =>
            throw new UnsupportedOperation(
              s"WhereElement expression is ${exp.asInstanceOf[BinaryExpressionElement].op} should be an == or Contains BinaryExpressionElement.")
        }
      case func: FuncElement =>
        val member = func.arguments.head.asInstanceOf[MemberElement]
        val param = func.instance
        extractUniqueQueries(member, param, entityInfo)
      case _ =>
        throw new UnsupportedOperation(s"WhereElement expression is $elem should be an == or contains expression.")
    }
  }

  private def convertToKey(elem: RelationElement, entityInfo: ClassEntityInfo) = {

    def extractNonUniqueKeys(member: MemberElement, param: RelationElement, entityInfo: ClassEntityInfo) = {
      entityInfo.indexes.find(_.name == member.memberName) match {
        case Some(indexInfo) =>
          param match {
            case ConstValueElement(value, info) =>
              value match {
                case l: Iterable[_] =>
                  l.collect { case elem => buildKey(indexInfo, elem) }
                case _ => throw new UnsupportedOperand("contains range query only support Iterable parameter")
              }
            case _ => throw new UnsupportedOperand("contains range query only support ConstValueElement parameter")
          }
        case None =>
          throw new RuntimeException(
            s"Member element ${member.memberName} is not an index on ${entityInfo.runtimeClass}")
      }
    }

    def buildKey(indexInfo: IndexInfo[_, _], propValue: Any) = {
      indexInfo match {
        case i: IndexInfo[_, Any @unchecked] => i.makeKey(propValue)
      }
    }

    elem match {
      case exp: BinaryExpressionElement =>
        exp.op match {
          case BinaryExpressionType.EQ =>
            val (member, _, param) = normalize(exp.left, exp.op, exp.right)

            entityInfo.indexes.find(_.name == member.memberName) match {
              case Some(indexInfo) => {
                buildKey(indexInfo, getConstOrFreevar(param)).asInstanceOf[Key[Storable]] :: Nil
                // indexInfo.asInstanceOf[IndexInfo[EntityType, Any]].makeKey(param)
              }
              case None =>
                throw new RuntimeException(
                  s"Member element ${member.memberName} is not an index on ${entityInfo.runtimeClass}")
            }
          case BinaryExpressionType.ITEM_IS_IN =>
            val (member, _, param) = normalize(exp.left, exp.op, exp.right)
            extractNonUniqueKeys(member, param, entityInfo)

          case _ =>
            throw new UnsupportedOperation(
              s"WhereElement expression is ${exp.asInstanceOf[BinaryExpressionElement].op} should be an == or Contains BinaryExpressionElement.")
        }
      case func: FuncElement =>
        val member = func.arguments.head.asInstanceOf[MemberElement]
        val param = func.instance
        extractNonUniqueKeys(member, param, entityInfo)
      case _ =>
        throw new UnsupportedOperation(s"WhereElement expression is $elem should be an == or contains expression.")
    }
  }

  // TODO (OPTIMUS-18021): decommission of recoverWithManualRangeDiff, legacy
  // rangequery, findByIndexInRange
  private[reactive] def filterResults(
      results: LegacyAPIQueryResult,
      rawFilters: Seq[RelationElement]): RangeQueryResult[_] = {
    val peFilters = rawFilters map (PersistentEntityCondition.parseCondition(_))
    if (peFilters.nonEmpty) {
      val filteredResult = results.result map {
        _ map { case (eref, entities) =>
          eref -> entities.filter(pe => peFilters.forall(_.check(pe)))
        } filter (_._2.nonEmpty)
      }
      LegacyAPIQueryResult(filteredResult)
    } else
      results
  }

  /**
   * mark this as @node because we need access dal to get persistent entity and we want the dal requests are batched
   */
  @scenarioIndependent @node private[reactive] def filterResults(
      results: HistoricalAPIQueryResult[EntityType],
      rawFilters: Seq[RelationElement]): RangeQueryResult[_] = {
    val peFilters = rawFilters map (PersistentEntityCondition.parseCondition(_))
    if (peFilters nonEmpty) {
      val filteredResult = results.result map {
        _.apar map { case (eref, changes) =>
          eref -> changes.apar.flatMap { case change =>
            // TODO (OPTIMUS-17009): revisit this logic and performance
            // (especially we will do another getPEFromVersionHolder in EventReplayer)
            // this probably should end up with switch multi filter query to OpenVtRange API
            val newfrom = change.from.filter {
              case evh: EntityVersionHolder[_] =>
                RangeQuery.getPEFromVersionHolder(evh).forall(pe => peFilters.forall(_.check(pe)))
              case _ =>
                throw new IllegalArgumentException("VersionHolder only have EntityVersionHolder implementation")
            }
            val newTo = change.to.filter {
              case evh: EntityVersionHolder[_] =>
                RangeQuery.getPEFromVersionHolder(evh).forall(pe => peFilters.forall(_.check(pe)))
              case _ =>
                throw new IllegalArgumentException("VersionHolder only have EntityVersionHolder implementation")
            }
            if (newfrom.isEmpty && newTo.isEmpty) {
              // if both from and to of this change don't have entity we are interested in-
              // remove it from the change list
              None
            } else {
              Some(EntityChange[EntityType](change.eref, newfrom, newTo, change.readTemporalContext))
            }
          }
        } filter (_._2.nonEmpty)
      }
      HistoricalAPIQueryResult(filteredResult)
    } else
      results
  }
}

private[optimus] object RangeQuery {
  val logger = getLogger[RangeQuery]

  /**
   * we have to introduce a type parameter, otherwise when annotated with node and scenarioIndependent for case
   * bitempSpace: EntityBitemporalSpaceImpl[_] cannot pass compilation
   */
  @scenarioIndependent @node def getPEFromVersionHolder[T <: Entity](
      entityVersionHolder: EntityVersionHolder[T]): List[PersistentEntity] = {
    val pes = (entityVersionHolder.bitempSpace match {
      case bitempSpace: EntityBitemporalSpaceImpl[T] =>
        bitempSpace.data.apar.map { case rect =>
          val peOpt = entityVersionHolder.persistentEntityOptionAt(
            FlatTemporalContextFactory.generator.temporalContext(
              rect.vtInterval.from,
              rect.txInterval.from
            ))
          peOpt map { case pe: PersistentEntity =>
            PersistentEntity(
              pe.serialized,
              pe.versionedRef,
              pe.lockToken,
              pe.vtInterval,
              TimeInterval(pe.txInterval.from, rect.txInterval.to))
          }
        }
      case wrapperBitemporalSpace =>
        logger.error(
          s"Unexpected bitemporal space: $wrapperBitemporalSpace" +
            s" type: ${wrapperBitemporalSpace.getClass}")
        throw new RuntimeException(
          s"Unexpected bitemporal space: $wrapperBitemporalSpace" +
            s" type: ${wrapperBitemporalSpace.getClass}")
    }).flatten
    pes.toList
  }

  /**
   * Fetch all the persistent entities that reflects the entity changes that matches the query and falls in the given
   * ttRange.
   *
   * @param catchupQuery
   *   PriQL query to define the scope of interested entity changes.
   * @param ttRange
   *   Transaction time range between which we want the entity changes back. The range is half open as [from, to).
   * @return
   *   A map of EntityReference to Seq[PersistentEntity] which reflects the changes in the ttRange.
   * @see
   *   RangePriqlQueryTest
   */
  // Note marked as SI because it is ( so a complie time validation applies ).
  // It is a raw node on a non entity so the result will not be cached
  @scenarioIndependent @node def rangeQuery(
      catchupQuery: MultiRelationElement,
      ttRange: TimeInterval,
      useHistoricalAPI: Boolean = Settings.useHistoricalRangeQuery,
      useOpenVtRangeQuery: Boolean = Settings.useOpenVtTtRangeQuery
  ): RangeQueryResult[_] = {
    val query = new RangeQuery(catchupQuery, ttRange)

    // use resolver.rangeQuery
    logger.info(
      s"range query is split into server side " +
        s"query(headOption): ${query.serverSideQueries.headOption}, " +
        s"and client side filter(headOption): ${query.restFiltersForIndex.headOption}")
    logger.debug(
      s"range query: ${query.catchupQuery} is split into server side " +
        s"query(s): ${query.serverSideQueries}, and client side filters: ${query.restFiltersForIndex}")

    if (!useHistoricalAPI) {
      query.filterResults(query.serverSideChangesLegacy, query.restFiltersForIndex)
    } else if (useOpenVtRangeQuery) {
      query.filterResults(query.serverSideChanges, query.restFiltersForIndex)
    } else {
      if (query.nonUniqueIndexQueries.nonEmpty) {
        query.filterResults(query.nonUniqueIndexQueryServerSideChanges, query.restFiltersForNonUniqueIndex)
      } else {
        val errMsg = "Can not use -Doptimus.reactive.historicalRangeQuery on query which is not non-unique index query"
        logger.error(errMsg)
        LegacyAPIQueryResult(Failure(new IllegalArgumentException(errMsg)))
      }
    }
  }

  // Note marked as SI because it is ( so a complie time validation applies ).
  // It is a raw node on a non entity so the result will not be cached
  @scenarioIndependent @node def rangeQuery(erefs: Seq[EntityReference], ttRange: TimeInterval): RangeQueryResult[_] = {
    val requests = erefs map { eref =>
      Select(ReferenceQuery(eref), DSIQueryTemporality.TxRange(ttRange))
    }
    val results = DAL.resolver.dsi.executeReadOnlyCommands(requests)
    processRangeQueryResults(results)
  }

  private def processRangeQueryResults(results: Seq[optimus.platform.dsi.bitemporal.Result]) = {
    val collected = results.foldLeft(Try(Map.newBuilder[EntityReference, Seq[PersistentEntity]])) { (builder, result) =>
      if (builder.isSuccess) {
        result match {
          case ErrorResult(err, _) => Failure(err)
          case SelectResult(pes) =>
            val entry = pes.headOption.map(pe => pe.entityRef -> pes.toSeq.sortBy(_.txInterval.from))
            val accumulated = builder.get
            entry foreach (accumulated += _)
            Success(accumulated)
          case o => Failure(new GeneralDALException(s"Unexpected DSI result type ${o.getClass.getName}"))
        }
      } else builder
    }

    LegacyAPIQueryResult(collected map (_.result()))
  }
}
