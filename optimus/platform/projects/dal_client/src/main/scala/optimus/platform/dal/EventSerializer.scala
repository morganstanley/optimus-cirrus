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
import msjava.base.util.uuid.MSUuid
import msjava.slf4jutils.scalalog.getLogger
import optimus.entity.EntityInfoRegistry
import optimus.entity.IndexInfo
import optimus.graph.PropertyInfo
import optimus.platform._
import optimus.platform.internal.IgnoreSyncStacksPlugin
import optimus.platform.internal.TemporalSource
import optimus.platform.pickling.AbstractPickledOutputStream
import optimus.platform.pickling.PickledMapWrapper
import optimus.platform.pickling.PickledOutputStream
import optimus.platform.pickling.PropertyMapOutputStream
import optimus.platform.temporalSurface._
import optimus.platform.temporalSurface.advanced.TemporalContextUtils
import optimus.platform.storable._
import optimus.platform.versioning.TransformerRegistry

import scala.collection.concurrent
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

// This is hack to work around the fact that we have a sync stack when doing DALImpl.TemporalContext(...)
// because of the (eventual) call to FlatTemporalContext.factory.create, which is @si @node.
// TODO (OPTIMUS-13717): remove this workaround
@entity private[dal] object NoSyncStackTemporalSurfaceFactoryWorkaround {
  @node private[dal] def createTemporalContext(vt: Instant, tt: Instant) = DALImpl.TemporalContext(vt, tt)
  IgnoreSyncStacksPlugin.installIfNeeded(createTemporalContext_info)
}

sealed trait BusinessEventSerializer extends StorableSerializer {
  private val log = getLogger(this)

  /**
   * will either return the upcasting target event info or else throw
   */
  private def getUpcastingTargetEventInfo(data: SerializedBusinessEvent): EventInfo = {
    log.info(s"doing EventUpcasting for ${data.className}")
    val init: Option[EventInfo] = None
    upcastDomain
      .flatMap { concrete =>
        data.types.foldLeft(init)(checkForNewTarget(concrete))
      }
      .getOrElse(
        throw new EventUpcastingException(s"upcasting failed for ${data.className}, types:${data.types.toList}"))
  }

  private def getInfo(ser: SerializedBusinessEvent) =
    try {
      EntityInfoRegistry.getInfo(ser.className).asInstanceOf[EventInfo]
    } catch {
      case ex: ClassNotFoundException if upcastDomain.isDefined =>
        getUpcastingTargetEventInfo(ser)
      // XXX CANNOT DO THIS. JVM/CLASSLOADER-WIDE STATE CANNOT DEPEND ON RUNTIME ENVIRONMENT!!!
      // EntityInfoRegistry.registry.update(ser.className, tInfo)
    }

  @async protected def getBusinessEvent(tc: HasTTContext)(
      ser: SerializedBusinessEvent,
      props: Map[String, Any] = Map.empty,
      persisted: Boolean = true,
      forcePickle: Boolean = false): BusinessEvent = {
    val info = getInfo(ser)
    val properties = if (props.isEmpty) ser.properties else props

    // XXX What should PickledMapWrapper have?
    val fakeTemporalContext =
      NoSyncStackTemporalSurfaceFactoryWorkaround.createTemporalContext(
        ser.validTime,
        tc.ttContext.getTTForEvent(ser.className))
    val versioned = version(ser.className, properties, fakeTemporalContext)
    val in = new PickledMapWrapper(versioned, fakeTemporalContext, ser.id)
    val eventInfo = if (persisted) DSIEventInfo(ser.versionId, ser.lockToken, ser.tt) else LocalDALEventInfo

    handleDeserializationExceptions(ser.className, properties, versioned, ser.id) {
      info.createUnpickled(in, forcePickle, eventInfo, ser.id)
    }
  }

  def upcastDomain: Option[String] =
    EvaluationContext.env.config.runtimeConfig.getString(RuntimeProperties.DsiEventUpcastingProperty)

  def checkForNewTarget(
      concreteDomain: String)(existingTarget: Option[EventInfo], possibleTarget: String): Option[EventInfo] = {
    try {
      val storableInfo = EntityInfoRegistry.getInfo(possibleTarget)
      val eventInfo = storableInfo.asInstanceOf[EventInfo]
      val useNewTarget = eventInfo.upcastDomain.exists(d => d.id == concreteDomain) &&
        existingTarget.forall { t =>
          t.runtimeClass.isAssignableFrom(eventInfo.runtimeClass)
        }

      if (useNewTarget) Some(eventInfo) else existingTarget
    } catch {
      case ex: ClassNotFoundException => existingTarget
    }
  }

  @async protected def doSerializeBusinessEvent(
      evt: BusinessEvent,
      entityReferences: collection.Map[Entity, EntityReference],
      cmid: Option[MSUuid] = None): SerializedBusinessEvent = {
    val info = evt.dal$eventInfo

    // we need to use this value inside SerializedBusinessEvent and GPB proto, so the broker can have event types information
    val types: Seq[String] = evt.$info.baseTypes.iterator.map { _.runtimeClass.getName }.toIndexedSeq
    val className = evt.getClass.getName
    val versionedProperties = {
      val properties = PropertyMapOutputStream.pickledStorable(evt, entityReferences).asInstanceOf[Map[String, Any]]
      val afterTransforms = TransformerRegistry.versionToWrite(className, properties, 0, TemporalSource.loadContext).get
      require(afterTransforms.isDefinedAt(0) && afterTransforms.size == 1, "events should only be written at slot 0")
      TransformerRegistry.executeForcedWriteTransform(className, afterTransforms(0), TemporalSource.loadContext)
    }

    val skeys = evt.$info.indexes flatMap {
      _.asInstanceOf[IndexInfo[BusinessEvent, _]].entityToSerializedKey(evt, entityReferences)
    }

    val cmref = cmid.map(c => CmReference(c.asBytes))

    // TODO (OPTIMUS-13716): slot support for events, should not hard-code slot 0
    SerializedBusinessEvent(
      id = evt.dal$eventRef,
      cmid = cmref,
      className = className,
      properties = versionedProperties,
      keys = skeys,
      types = types,
      validTime = evt.validTime,
      versionId = info.vid,
      appEventId = null,
      tt = null,
      lockToken = info.lockToken,
      isCancel = false,
      slot = 0,
      vrefOpt = None
    )
  }
}

object EventSerializer extends BusinessEventSerializer {

  @async
  def deserializeBusinessEvent(tc: HasTTContext)(ser: SerializedBusinessEvent): BusinessEvent = {
    val ret = getBusinessEvent(tc)(ser)

    ret.dal$loadTT = tc.ttContext
    ret.dal$cmid_=(ser.cmid.map { cmr: CmReference =>
      new MSUuid(cmr.data, true)
    })
    ret
  }

  def deserializeAppEvent(ser: SerializedAppEvent): ApplicationEvent = ApplicationEvent(ser.id, ser.tt)

  // TODO (OPTIMUS-13716): slot support for events, this will need to return MultiSlotSerializedBusinessEvent
  @async def serializeBusinessEvent(
      evt: BusinessEvent,
      entityReferences: collection.Map[Entity, EntityReference],
      cmid: Option[MSUuid] = None): SerializedBusinessEvent = doSerializeBusinessEvent(evt, entityReferences, cmid)
}

private[dal] final case class SerializedWithEref(se: Option[SerializedEntity], eref: EntityReference)
private object SerializedWithEref {
  def apply(se: SerializedEntity, eref: EntityReference): SerializedWithEref = SerializedWithEref(Some(se), eref)
  def apply(eref: EntityReference): SerializedWithEref = SerializedWithEref(None, eref)
}

private[optimus] object ContainedEventSerializer extends BusinessEventSerializer {
  import SerializedContainedEvent._
  import DALImpl._

  @entity private[optimus] class ContainedEntityDeserializer(ents: Map[EntityReference, ContainedEntity]) {

    private def getContained(eref: EntityReference): ContainedEntity =
      ents.getOrElse(eref, throw new IllegalArgumentException(s"Couldn't find $eref in EntRefs(${ents.size}): $ents"))

    @async private def deserRefProperty(prop: Any): Any = prop match {
      case r: TemporaryReference =>
        getContained(r) match {
          case AppliedHeapEntity(se) => deserContainedSe(se)
          case UniqueHeapEntity(se)  => deserContainedSe(se)
          case StoredEntity(_, _) => throw new IllegalArgumentException("StoredEntity can not have TemporaryReference!")
        }
      case r: FinalTypedReference =>
        getContained(r) match {
          case AppliedHeapEntity(se) =>
            log.info(s"For StoredReference ($r) found AppliedHeap entry, thus deserializing SerializedEntity")
            deserContainedSe(se)
          case UniqueHeapEntity(se) =>
            log.info(s"For StoredReference ($r) found UniqueHeap entry, thus deserializing SerializedEntity")
            deserContainedSe(se)
          case StoredEntity(vt, tt) =>
            // TODO (OPTIMUS-46195): Make them lazily load with LazyPickledReferences
            loadEntityWithGivenContext(r, DALImpl.TemporalContext(vt, tt))
        }
      case seq: Seq[_]  => seq.aseq.map(deserRefProperty)
      case set: Set[_]  => set.aseq.map(deserRefProperty)
      case m: Map[_, _] => m.aseq.map { case (k, v) => deserRefProperty(k) -> deserRefProperty(v) }
      case o            => o
    }

    // This is @node to avoid duplicate deserialization for the same entity.
    @node private def deserContainedSe(se: SerializedEntity): Entity = {
      // Use of .aseq is intentional here.
      val restoredSe = ContainedEntitySerializer.restoreLinkageProperties(se)
      val props = restoredSe.properties.aseq.map { case (k: String, propVal) =>
        k -> deserRefProperty(propVal)
      }
      val storageInfo = getContained(se.entityRef) match {
        case AppliedHeapEntity(_) => AppliedStorageInfo
        case UniqueHeapEntity(_)  => UniqueStorageInfo
        case StoredEntity(_, _)   => throw new IllegalArgumentException("StoredEntity can not have TemporaryReference!")
      }
      ContainedEntitySerializer.deserialize(
        data = restoredSe.copySerialized(properties = props),
        storageInfo = storageInfo
      )
    }

    @node def deserEnts: Map[SerializedWithEref, Entity] = {
      // Use of .aseq is intentional here.
      ents.aseq.map { case (k: EntityReference, v: ContainedEntity) =>
        v match {
          case AppliedHeapEntity(se) => SerializedWithEref(se, k) -> deserContainedSe(se)
          case UniqueHeapEntity(se)  => SerializedWithEref(se, k) -> deserContainedSe(se)
          case StoredEntity(vt, tt)  =>
            // TODO (OPTIMUS-46195): Make them lazily load with LazyPickledReferences
            SerializedWithEref(k) -> loadEntityWithGivenContext(k, DALImpl.TemporalContext(vt, tt))
        }
      }
    }
  }

  object ContainedEntitySerializer extends EntitySerialization {

    def restoreLinkageProperties(
        multiSlot: MultiSlotSerializedEntity
    ): SerializedEntity = restoreLinkageProperties(multiSlot.someSlot)

    def restoreLinkageProperties(
        se: SerializedEntity
    ): SerializedEntity =
      se.linkages
        .map { lm =>
          // Flatten linkages into properties.
          val linkageProps = lm.map { case (prop, linkageSet) =>
            prop.propertyName -> linkageSet.map(_.link).toSeq
          }
          val newProps = se.properties ++ linkageProps
          se.copy(properties = newProps, linkages = None)
        }
        .getOrElse(se)

    @async def serialize[S <: Entity](
        s: S,
        entityRefs: Map[Entity, EntityReference]
    ): SerializedEntity = {
      val se = serializeImpl(s, None, createOutputStream(s, mutable.Map(entityRefs.toSeq: _*), allowMutation = true))
      restoreLinkageProperties(se)
    }

    @async def deserialize(
        data: SerializedEntity,
        storageInfo: StorageInfo
    ): Entity = {
      val temporary: Boolean = data.entityRef.isTemporary
      val loadCtx = TemporalSource.loadContext
      val ret = getEntity(
        data = data,
        loadContext = loadCtx,
        storageInfo = storageInfo,
        instanceMap = Map.empty,
        temporary = temporary,
        // Eager unpickling instead of Lazy - see optimus.platform.pickling.ReflectivePicklingImpl.unpickleCreate
        forcePickle = true
      )
      if (!temporary) setCmid(ret, data.cmid)
      else ret
    }
  }

  class EventVisitor extends AbstractPickledOutputStream {

    override def forceHydration = false
    private[this] var currentPropertyInfo: PropertyInfo[_] = _
    private val entityVisitor = new EntityVisitor

    def apply(evt: BusinessEvent): Unit = evt.pickle(this)

    override def writeEntity(entity: Entity): Unit = entityVisitor.apply(entity)

    override def writePropertyInfo(info: PropertyInfo[_]): Unit = {
      super.writePropertyInfo(info)
      currentPropertyInfo = info
    }

    def getResults: Map[Entity, EntityReference] = entityVisitor.getResults
  }

  class EntityVisitor extends AbstractPickledOutputStream {

    override def forceHydration = false
    private[this] var currentPropertyInfo: PropertyInfo[_] = _
    private[this] val entfields: TrieMap[Entity, EntityReference] =
      concurrent.TrieMap.empty[Entity, EntityReference]

    private def getEref(e: Entity): EntityReference =
      if (e.dal$entityRef ne null) e.dal$entityRef
      else EntityReference.freshTemporary

    def apply(ent: Entity): Unit = {
      if (!entfields.contains(ent) && !ent.$isModule) entfields += ent -> getEref(ent)
      ent.pickle(this)
    }

    override def writeEntity(entity: Entity): Unit = apply(entity)

    override def writePropertyInfo(info: PropertyInfo[_]): Unit = {
      super.writePropertyInfo(info)
      currentPropertyInfo = info
    }

    def getResults: Map[Entity, EntityReference] = entfields.result().toMap
  }

  @async private[dal] def loadEntityWithGivenContext(
      eref: EntityReference,
      tc: TemporalContext
  ): Entity = EvaluationContext.entityResolver.findByReference(eref, tc)

  @async private def getContainedEntity(
      e: Entity,
      eref: EntityReference,
      serializedEntity: Option[SerializedEntity] = None
  ): ContainedEntity = {
    TemporalContextUtils.loadTemporalCoordinates(e) match {
      case t: EntityTemporalInformation =>
        require(
          eref.isInstanceOf[FinalTypedReference],
          "EntityReference for DAL loaded entities should be FinalTypedReference")
        StoredEntity(t.vt, t.tt)
      case UniqueEntity =>
        require(eref.isTemporary, "EntityReference for unique entities should be TemporaryReference")
        require(serializedEntity.nonEmpty, "SerializedEntity can't be empty for Unique Heap")
        UniqueHeapEntity(serializedEntity.get)
      case AppliedEntity =>
        require(eref.isTemporary, "EntityReference for applied entities should be TemporaryReference")
        require(serializedEntity.nonEmpty, "SerializedEntity can't be empty for Applied Heap")
        AppliedHeapEntity(serializedEntity.get)
      case _ => throw new IllegalArgumentException(s"unexpected result!")
    }
  }

  @async private def getSerializedMessage(
      evt: BusinessEvent,
      erefMap: Map[Entity, EntityReference]
  ): (SerializedBusinessEvent, Map[EntityReference, SerializedEntity], Set[EntityReference]) = {
    // dalLoadedEntities are needed as a separate list of TemporalErefs for nested Entities,
    // heapEntities will specifically be Serialized
    val (heapEntities, dalLoadedEntities) = erefMap.partition { case (_, v) => v.isTemporary }

    val erefToSEMap = heapEntities.aseq.map { case (k, v) =>
      v -> ContainedEntitySerializer.serialize(k, erefMap)
    }

    (doSerializeBusinessEvent(evt, erefMap), erefToSEMap, dalLoadedEntities.values.toSet)
  }

  @async private def replaceRefs(
      prop: Any,
      erefMap: Map[EntityReference, Entity]
  ): Any = prop match {
    case r: EntityReference => erefMap(r)
    case seq: Seq[_]        => seq.aseq.map(replaceRefs(_, erefMap))
    case set: Set[_]        => set.aseq.map(replaceRefs(_, erefMap))
    case m: Map[_, _]       => m.aseq.map { case (k, v) => replaceRefs(k, erefMap) -> replaceRefs(v, erefMap) }
    case o                  => o
  }

  // Gives back Business Event and Map of temporary Entities with erefs to persist later
  @async def deserialize[T <: BusinessEvent with ContainedEvent](
      serMsg: SerializedContainedEvent
  ): (T, Map[EntityReference, Entity]) = {

    val ser = serMsg.sbe
    val erefToContainedEntityMap = serMsg.entityMap
    val deser = ContainedEntityDeserializer(erefToContainedEntityMap)
    val erefMap = deser.deserEnts.map { case (serializedWithEref, entity) => serializedWithEref.eref -> entity }

    val evtProps = ser.properties.aseq.map { case (k, v) => k -> replaceRefs(v, erefMap) }

    val persisted = ser.tt != null
    val ret = getBusinessEvent(TemporalSource.loadContext)(ser, evtProps, persisted, forcePickle = true)

    if (persisted) {
      ret.dal$cmid_=(ser.cmid.map { cmr: CmReference =>
        new MSUuid(cmr.data, true)
      })
    }

    (ret.asInstanceOf[T], erefMap)
  }

  @async def serialize[E <: BusinessEvent with ContainedEvent](evt: E): SerializedContainedEvent = {

    if (evt.dal$eventRef eq null) evt.dal$eventRef = BusinessEventReference.fresh

    // Deep Hydration
    val evtVisitor = new EventVisitor
    evtVisitor.apply(evt)
    val erefMap = evtVisitor.getResults

    // Serialization
    val (message, heapEntityMap, dalEntityRefs) = getSerializedMessage(evt, erefMap)

    // Post-Serialization processing for transformed shapes which could not be captured during deep hydration
    val entityMap = erefMap.apar.map { case (k, v) =>
      v -> getContainedEntity(k, v, heapEntityMap.get(v))
    }
    val transformedRefsMap =
      heapEntityMap.values.apar.flatMap { se =>
        se.properties.values.apar.collect {
          // We have no way to get the tt and vt for the transformed entities
          // where we only get erefs as part of propertyMap
          case v: FinalTypedReference if !dalEntityRefs.contains(v) =>
            val e = loadEntityWithGivenContext(v, TemporalSource.loadContext)
            v -> getContainedEntity(e, v)
        }
      }.toMap ++ message.properties.values.apar.collect {
        // We have no way to get the tt and vt for the transformed entities
        // where we only get erefs as part of propertyMap
        case v: FinalTypedReference if !dalEntityRefs.contains(v) =>
          val e = loadEntityWithGivenContext(v, TemporalSource.loadContext)
          v -> getContainedEntity(e, v)
      }

    SerializedContainedEvent(message, entityMap ++ transformedRefsMap)
  }
}
