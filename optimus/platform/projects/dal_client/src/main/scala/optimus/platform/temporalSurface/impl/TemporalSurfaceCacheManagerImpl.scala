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
package optimus.platform.temporalSurface.impl

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import java.time.Instant

import com.google.common.cache.CacheBuilder
import optimus.graph.{DiagnosticSettings, Settings}
import optimus.platform._
import optimus.platform.dal.{QueryTemporality, TemporalContextEntityResolver}
import optimus.platform.EvaluationContext
import optimus.platform.storable.{EntityReference, PersistentEntity}
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.operations.{NoCommandRecorder, QueryByEntityReference, TemporalSurfaceQuery}
import optimus.platform.util.interner.{Interner, NoInterner}
import optimus.platform.AsyncImplicits._
import optimus.platform.internal._
import optimus.platform.storable.Entity
import optimus.platform.temporalSurface.operations.EntityReferenceQueryReason
import optimus.platform.temporalSurface.operations.{DataQueryByEntityReference, EntityClassBasedQuery}
import optimus.platform.util.PrettyStringBuilder
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import optimus.scalacompat.collection._

private[optimus /*platform*/ ] object TemporalSurfaceCachedDataAccess {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val maxSize = DiagnosticSettings.getIntProperty("optimus.platform.temporalSurface.classInfoCache-size", 10000)
  // the periodicity for logging stats - set to a +ve number to enable logging
  private val statsLog =
    DiagnosticSettings.getIntProperty("optimus.platform.temporalSurface.classInfoCache-statsLog", 0)
  private val classInfoCache =
    CacheBuilder.newBuilder().maximumSize(maxSize).build[EntityReference, ClassInfo]().asMap()

  private val statsLogEnabled = statsLog > 0
  private val statsLogCounter = new AtomicInteger(statsLog)

  def getClassInfo(eref: EntityReference, reason: EntityReferenceQueryReason): Option[ClassInfo] = {
    val res = classInfoCache.get(eref)
    if (res eq null) {
      tickCounter(cacheMisses, reason)
      None
    } else {
      tickCounter(cacheHits, reason)
      Some(res)
    }
  }
  def refineClassInfo(
      eref: EntityReference,
      clazz: Class[_ <: Entity],
      isConcreteType: Boolean,
      reason: EntityReferenceQueryReason): Some[ClassInfo] = {
    lazy val classInfo = if (isConcreteType) ConcreteClassInfo(clazz) else SpecificationClassInfo(clazz)
    val existing = classInfoCache.get(eref)
    existing match {
      case c: ConcreteClassInfo[_] =>
        tickCounter(cacheHits, reason)
        // do nothing
        Some(c)
      case _ if isConcreteType =>
        tickCounter(cacheRefinements, reason)
        // we don't care about race
        classInfoCache.put(eref, classInfo)
        Some(classInfo)
      case null =>
        if (classInfoCache.putIfAbsent(eref, classInfo) ne null)
          // race so retry - cant be null on re-entry, so will not recurse repeatedly
          refineClassInfo(eref, clazz, isConcreteType, reason)
        else {
          tickCounter(cacheRefinements, reason)
          Some(classInfo)
        }
      case u: UnknownClassInfo =>
        if (!classInfoCache.replace(eref, u, classInfo))
          // if we lose the race it is refined, so retry
          refineClassInfo(eref, clazz, isConcreteType, reason)
        else {
          tickCounter(cacheRefinements, reason)
          Some(classInfo)
        }
      case s: SpecificationClassInfo[_] =>
        if (s.clazz.isAssignableFrom(clazz))
          if (!classInfoCache.replace(eref, s, classInfo))
            // if we lose the race it is refined, so retry
            refineClassInfo(eref, clazz, isConcreteType, reason)
          else {
            tickCounter(cacheRefinements, reason)
            Some(classInfo)
          }
        else {
          tickCounter(cacheHits, reason)
          Some(s)
        }

    }
  }
  def dalAccessedConcreteClassInfo(
      entityRef: EntityReference,
      result: ClassInfo,
      reason: EntityReferenceQueryReason): Unit = {
    tickCounter(concreteClassRequest, reason)
    // we don't care about any race conditions - this is a final state
    classInfoCache.put(entityRef, result)
  }

  private val cacheHits = newCountersByReason()
  private val cacheRefinements = newCountersByReason()
  private val cacheMisses = newCountersByReason()
  private val concreteClassRequest = newCountersByReason()
  private def tickCounter(
      counter: util.EnumMap[EntityReferenceQueryReason, AtomicLong],
      reason: EntityReferenceQueryReason): Unit = {
    counter.get(reason).incrementAndGet()
    if (statsLogEnabled && statsLogCounter.decrementAndGet() == 0) {
      val stats = currentStats
      log.info(s"classInfoCache stats - hits ${stats.hits}")
      log.info(s"classInfoCache stats - refinements ${stats.refinements}")
      log.info(s"classInfoCache stats - misses ${stats.misses}")
      log.info(s"classInfoCache stats - concreteRequests ${stats.concreteRequests}")
      while (statsLogCounter.addAndGet(statsLog) <= 0) {
        // keep adding
        ;
      }
    }
  }

  private def newCountersByReason(): util.EnumMap[EntityReferenceQueryReason, AtomicLong] = {
    val res = new util.EnumMap[EntityReferenceQueryReason, AtomicLong](classOf[EntityReferenceQueryReason])
    EntityReferenceQueryReason.values() foreach { reason =>
      res.put(reason, new AtomicLong())
    }
    res
  }
  def currentStats = {
    ClassInfoCacheStats(snap(cacheHits), snap(cacheRefinements), snap(cacheMisses), snap(concreteClassRequest))
  }
  private def snap(data: util.Map[EntityReferenceQueryReason, AtomicLong]): Map[EntityReferenceQueryReason, Long] = {
    import scala.jdk.CollectionConverters._
    data.asScala.iterator.map { case (k, v) => (k -> v.get) }.toMap
  }
}
final case class ClassInfoCacheStats(
    hits: Map[EntityReferenceQueryReason, Long],
    refinements: Map[EntityReferenceQueryReason, Long],
    misses: Map[EntityReferenceQueryReason, Long],
    concreteRequests: Map[EntityReferenceQueryReason, Long])
class TemporalSurfaceCachedDataAccess(private[optimus] val resolver: TemporalContextEntityResolver)
    extends TemporalSurfaceDataAccess {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val dumpGetClassInfoStackTrace =
    DiagnosticSettings.getBoolProperty("optimus.platform.temporalSurface.dumpClassInfoSource", false)

  object cacheManager extends TemporalSurfaceCacheManager {
    def addMonotemporalCache(tt: Instant, cache: TemporalSurfaceCache) =
      TemporalSurfaceCachedDataAccess.this.addMonotemporalCache(tt, cache)
    def removeMonotemporalCache(tt: Instant, cache: TemporalSurfaceCache) =
      TemporalSurfaceCachedDataAccess.this.removeMonotemporalCache(tt, cache)
  }

  val monotemporalCaches = new ConcurrentHashMap[Instant, Set[TemporalSurfaceCache]]()

  @tailrec private def addMonotemporalCache(tt: Instant, cache: TemporalSurfaceCache): Unit = {
    monotemporalCaches.get(tt) match {
      case null     => if (null != monotemporalCaches.putIfAbsent(tt, Set(cache))) addMonotemporalCache(tt, cache)
      case existing => if (!monotemporalCaches.replace(tt, existing, existing + cache)) addMonotemporalCache(tt, cache)
    }
  }

  @tailrec private def removeMonotemporalCache(tt: Instant, cache: TemporalSurfaceCache): Unit = {
    monotemporalCaches.get(tt) match {
      case null =>
      case existing =>
        val newCaches = existing - cache
        if (newCaches ne existing) {
          if (newCaches.isEmpty) {
            if (!monotemporalCaches.remove(tt, existing)) removeMonotemporalCache(tt, cache)
          } else if (!monotemporalCaches.replace(tt, existing, newCaches)) removeMonotemporalCache(tt, cache)
        }
    }
  }

  // TODO (OPTIMUS-65703): Remove once we have fully deprecated implicit subscribing
  private def maybeSubscribeToERef(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface): Unit = {
    if (!(Settings.implicitlySubscribeTickingErefs && sourceTemporalitySurface.canTick)) return

    operation match {
      case op: QueryByEntityReference[_] =>
        // This 1) proves via the compiler that currentTemporalityFor(op) can ONLY be of type QueryTemporality.At
        // and 2) produces breadcrumbs as a side-effects
        //
        // (really, we should just get rid of the whole TemporalityType path dependent type)
        (sourceTemporalitySurface.currentTemporalityFor(op): QueryTemporality.At)

        TemporalSurfaceCacheFactory.cacheEntityReference(op.eRef, op.targetClass, sourceTemporalitySurface)
      case _ =>
    }
  }

  def foldCachesWithTimeUntilDefined[T](time: QueryTemporality, fn: TemporalSurfaceCache => Option[T]): Option[T] = {
    val monoCaches = time match {
      case QueryTemporality.At(vt, tt) => monotemporalCaches.get(tt)
      case _ => throw new UnsupportedOperationException("Both vt and tt need to be specified to query the cache")
    }

    if (monoCaches == null || monoCaches.isEmpty) None
    else
      monoCaches.foldLeft[Option[T]](None) {
        case (existing: Some[r], _)              => existing
        case (None, cache: TemporalSurfaceCache) => fn(cache)
      }
  }

  // ItemKeys
  @scenarioIndependent @node private def resolverGetItemKeys(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType): Seq[operation.ItemKey] = {
    val result = resolver.getItemKeys(operation)(sourceTemporality)
    result
  }
  private def cacheGetItemKeys(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType): Option[Seq[operation.ItemKey]] = {
    foldCachesWithTimeUntilDefined(sourceTemporality, { _.getItemKeys(operation)(sourceTemporality) })
  }

  @scenarioIndependent @node def getItemKeys(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface): Seq[operation.ItemKey] =
    pluginHackDataAccess
      .getItemKeysInSurface(this, operation, sourceTemporalitySurface)
      .asInstanceOf[Seq[operation.ItemKey]]

  @scenarioIndependent @node def getItemKeysCallback(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface): Seq[operation.ItemKey] = {

    type Key = operation.ItemKey
    val rec = recorder
    val sourceTemporality = sourceTemporalitySurface.currentTemporalityFor(operation)
    rec.startCache
    cacheGetItemKeys(operation)(sourceTemporality) match {
      case r: Some[Seq[Key]] =>
        rec.cacheHit; r.get // calling get rather than using unapply as this asyncs
      case None =>
        rec.cacheMiss
        val result = resolverGetItemKeys(operation)(sourceTemporality)
        if (result.isEmpty) rec.cacheMissEmpty
        result
    }
  }
  // ItemData
  @scenarioIndependent @node private def resolverGetItemData(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType,
      itemTemporality: operation.TemporalityType): Map[operation.ItemKey, operation.ItemData] = {
    val result = resolver.getItemData(operation)(sourceTemporality, itemTemporality)
    result
  }
  private def cacheGetItemData(operation: TemporalSurfaceQuery)(
      sourceTemporality: operation.TemporalityType,
      itemTemporality: operation.TemporalityType): Option[Map[operation.ItemKey, operation.ItemData]] = {
    foldCachesWithTimeUntilDefined(sourceTemporality, { _.getItemData(operation)(sourceTemporality, itemTemporality) })
  }

  @scenarioIndependent @node def getItemData(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface,
      itemTemporalitySurface: LeafTemporalSurface): Map[operation.ItemKey, operation.ItemData] =
    pluginHackDataAccess
      .getItemDataInSurface(this, operation, sourceTemporalitySurface, itemTemporalitySurface)

  @scenarioIndependent @node def getItemDataCallback(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface,
      itemTemporalitySurface: LeafTemporalSurface): Map[operation.ItemKey, operation.ItemData] = {
    val rec = recorder

    // <~ pluginHackDataAccess.getItemDataInSurface <~ TemporalSurfaceCachedDataAccess#getItemData
    // <~ TemporalContextImpl#startDataAccess <~ pluginHa.start <~ TCI#dataAccess .
    // with priql:
    // <~ DALEntityResolver#findByIndex <~ DALExecutionProvider
    val sourceTemporality = sourceTemporalitySurface.currentTemporalityFor(operation)
    val itemTemporality = itemTemporalitySurface.currentTemporalityFor(operation)

    rec.startCache
    cacheGetItemData(operation)(sourceTemporality, itemTemporality) match {
      case Some(r) =>
        rec.cacheHit
        r
      case None =>
        rec.cacheMiss
        // first check the cache for the keys and values - we may have all, or some of the information that we need
        // we don't want to call getItemkeys as this will fetch from the DAL, and if we fetch it should be a single hit

        rec.startCache
        cacheGetItemKeys(operation)(sourceTemporality) match {
          case Some(keys) =>
            rec.cacheHit
            // we have the keys, so just fetch the data, but do this async so the DAL will batch it
            val result: Map[operation.ItemKey, operation.ItemData] = keys.apar.map { key =>
              key -> getSingleItemDataCallback(operation, itemTemporalitySurface)(key)
            }(Map.breakOut)
            result
          case None =>
            rec.cacheMiss
            maybeSubscribeToERef(operation, sourceTemporalitySurface)
            val result = resolverGetItemData(operation)(sourceTemporality, itemTemporality)
            if (result isEmpty) rec.cacheMissEmpty
            result
        }
    }
  }
  // SingleItemData

  @node private def resolverGetSingleItemData(operation: TemporalSurfaceQuery)(
      temporality: operation.TemporalityType,
      key: operation.ItemKey): operation.ItemData = {
    val result = resolver.getSingleItemData(operation)(temporality, key)
    result
  }

  private def cacheGetSingleItemData(operation: TemporalSurfaceQuery)(
      temporality: operation.TemporalityType,
      key: operation.ItemKey): Option[operation.ItemData] = {
    foldCachesWithTimeUntilDefined(temporality, { _.getSingleItemData(operation)(temporality, key) })
  }

  @scenarioIndependent @node def getSingleItemData(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface)(itemKey: operation.ItemKey): operation.ItemData = {
    pluginHackDataAccess
      .getSingleItemDataInSurface(this, operation, sourceTemporalitySurface)(itemKey)
      .asInstanceOf[operation.ItemData]
  }
  @scenarioIndependent @node def getSingleItemDataCallback(
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface)(itemKey: operation.ItemKey): operation.ItemData = {
    val temporality = sourceTemporalitySurface.currentTemporalityFor(operation)
    cacheGetSingleItemData(operation)(temporality, itemKey) getOrElse resolverGetSingleItemData(operation)(
      temporality,
      itemKey)
  }

  // classInfo
  @scenarioIndependent @node override def getClassInfo(
      operation: TemporalSurfaceQuery,
      temporalSurface: TemporalSurface,
      entityRef: EntityReference,
      requireConcrete: Boolean,
      reason: EntityReferenceQueryReason): ClassInfo = {
    val info = operation match {
      case q: EntityClassBasedQuery[_] =>
        TemporalSurfaceCachedDataAccess.refineClassInfo(entityRef, q.targetClass, false, reason)
      case eq: DataQueryByEntityReference[_] =>
        require(eq.eRef == entityRef)
        TemporalSurfaceCachedDataAccess.refineClassInfo(entityRef, eq.targetClass, eq.classIsConcreteType, reason)
      case _ =>
        TemporalSurfaceCachedDataAccess.getClassInfo(entityRef, reason)
    }
    info match {
      case Some(known: KnownClassInfo[_]) if (known.isClazzConcrete || !requireConcrete) => known
      case Some(unknown: UnknownClassInfo)                                               => unknown
      case _ =>
        if (dumpGetClassInfoStackTrace) {
          val psb = new PrettyStringBuilder
          psb.append(s"Node Stack for access to $entityRef for $reason\n")
          EvaluationContext.currentNode.waitersToFullMultilineNodeStack(true, psb)
          log.info(psb.toString)
        }
        val result = pluginHackDataAccess.getClassInfoInSurface(resolver, operation, temporalSurface, entityRef, reason)
        TemporalSurfaceCachedDataAccess.dalAccessedConcreteClassInfo(entityRef, result, reason)
        result
    }
  }

  private def recorder = {
    if (Settings.traceTemporalSurfaceCommands) {
      EvaluationContext.scenarioStack.findPluginTag(HasRecorderTag).flatMap(_.recorderTag).map(_.recorder).get
    } else NoCommandRecorder
  }

}
@entity private[impl] object pluginHackDataAccess {
  @scenarioIndependent @node def getItemKeysInSurface(
      callback: TemporalSurfaceCachedDataAccess,
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface): Seq[AnyRef] =
    callback.getItemKeysCallback(operation, sourceTemporalitySurface)

  @scenarioIndependent @node def getItemDataInSurface(
      callback: TemporalSurfaceCachedDataAccess,
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface,
      itemTemporalitySurface: LeafTemporalSurface): Map[operation.ItemKey, operation.ItemData] =
    callback.getItemDataCallback(operation, sourceTemporalitySurface, itemTemporalitySurface)

  @scenarioIndependent @node def getSingleItemDataInSurface(
      callback: TemporalSurfaceCachedDataAccess,
      operation: TemporalSurfaceQuery,
      sourceTemporalitySurface: LeafTemporalSurface)(itemKey: operation.ItemKey): AnyRef =
    callback.getSingleItemDataCallback(operation, sourceTemporalitySurface)(itemKey)

  @scenarioIndependent @node def getClassInfoInSurface(
      resolver: TemporalContextEntityResolver,
      operation: TemporalSurfaceQuery,
      temporalSurface: TemporalSurface,
      entityRef: EntityReference,
      reason: EntityReferenceQueryReason): ClassInfo =
    resolver.loadClassInfo(entityRef)

  if (Settings.traceDalAccessOrTemporalSurfaceCommands) {
    // param 2 is the surface
    val plugin = new TemporalSurface_SurfaceSpecificTracePlugin(2)
    getItemKeysInSurface_info.setPlugin(plugin)
    getItemDataInSurface_info.setPlugin(plugin)
    getSingleItemDataInSurface_info.setPlugin(plugin)
    getClassInfoInSurface_info.setPlugin(plugin)
  }
  getItemKeysInSurface_info.setCacheable(false)
  getItemDataInSurface_info.setCacheable(false)
  getSingleItemDataInSurface_info.setCacheable(false)
  getClassInfoInSurface_info.setCacheable(false)
}
