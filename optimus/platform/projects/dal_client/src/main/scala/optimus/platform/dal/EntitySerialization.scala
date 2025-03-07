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

import java.{util => jutil}
import msjava.base.util.uuid.MSUuid
import msjava.slf4jutils.scalalog.getLogger
import optimus.entity._
import optimus.graph.DiagnosticSettings
import optimus.graph.OGTrace
import optimus.graph.diagnostics.sampling.Cardinality
import optimus.platform._
import optimus.platform.AsyncImplicits._
import optimus.platform.dsi.UUIDReferenceGenerator
import optimus.platform.internal.TemporalSource
import optimus.platform.pickling._
import optimus.platform.storable._
import optimus.platform.temporalSurface.impl.FlatTemporalContext
import optimus.platform.versioning.TransformerRegistry

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.collection.immutable

class SerializationException extends Exception
class DeserializationException(message: String) extends Exception(message)
class SerializationClassMismatchException(val expected: Manifest[_], val actual: Manifest[_])
    extends SerializationException {
  override def toString = s"expected $expected, got $actual"
}

trait EntitySerialization extends StorableSerializer {
  private val log = getLogger[EntitySerialization]

  def upcastDomain: Option[String] =
    EvaluationContext.env.config.runtimeConfig.getString(RuntimeProperties.DsiEntityUpcastingProperty)

  /**
   * will either return the upcasting target entity info or else throw
   */
  private def getUpcastingTargetEntityInfo(data: SerializedEntity): ClassEntityInfo = {
    log.info(s"doing EntityUpcasting for ${data.className}")
    val init: Option[ClassEntityInfo] = None
    upcastDomain
      .flatMap { concrete =>
        data.types.foldLeft(init)(checkForNewTarget(concrete))
      }
      .getOrElse(
        throw new EntityUpcastingException(s"upcasting failed for ${data.className}, types:${data.types.toList}"))
  }

  def checkForNewTarget(concreteDomain: String)(
      existingTarget: Option[ClassEntityInfo],
      possibleTarget: String): Option[ClassEntityInfo] = {
    try {
      val entityInfo = getEntityInfoByName(possibleTarget)
      val useNewTarget = entityInfo.upcastDomain.exists(d => d.id == concreteDomain) &&
        existingTarget.forall { t =>
          t.runtimeClass.isAssignableFrom(entityInfo.runtimeClass)
        }

      if (useNewTarget) Some(entityInfo) else existingTarget
    } catch {
      case _: ClassNotFoundException => existingTarget
    }
  }

  abstract class InliningOutputStream(
      val rootEntity: Entity,
      val entityRefs: mutable.Map[Entity, EntityReference],
      protected val inlineBuf: mutable.Buffer[SerializedEntity],
      visiting: mutable.Set[Entity],
      val allowMutation: Boolean)
      extends PropertyMapOutputStream(entityRefs) {
    // NB NOT THREADSAFE
    private[this] var currentField: String = _

    val childToParentProperties: Seq[LinkageType] = rootEntity.$info.linkages

    def inlinedEntities: Seq[SerializedEntity]

    override def writeFieldName(k: String): Unit = {
      currentField = k
      super.writeFieldName(k)
    }

    override def write[T](data: T, pickler: Pickler[T]): Unit = {
      try {
        super.write(data, pickler)
      } catch {
        case pe: PicklingException =>
          throw new PicklingException(
            s"Exception while pickling field $currentField in entity of type ${rootEntity.$info.runtimeClass.getName}",
            Some(pe))
      }
    }

    override def writeEntity(e: Entity): Unit = {
      e match {
        case ie: InlineEntity =>
          if (ie.inlineRoot eq null) ie.inlineRoot = rootEntity
          else if (ie.inlineRoot ne rootEntity) throw new SerializationException

          if (!visiting.contains(ie)) {
            if (getEntityRef(ie) eq null) {
              val newRef = UUIDReferenceGenerator.generateTemporaryEntityReference()
              if (allowMutation)
                ie.dal$entityRef = newRef
              entityRefs += ie -> newRef
            }
            val os: InliningOutputStream =
              new InliningOutputStream(rootEntity, entityRefs, inlineBuf, visiting, allowMutation) {
                def inlinedEntities: Seq[SerializedEntity] = Nil
              }
            visiting += ie
            val ser = serializeImpl(ie, None, os)
            require(ser.entities.size == 1, "Multi-schema writes cannot be used in conjunction with entity inlining.")
            inlineBuf += ser.someSlot
          }
          super.writeEntity(e)

        case _ =>
          super.writeEntity(e)
      }
    }
  }

  protected def createOutputStream(
      re: Entity,
      entityRefs: mutable.Map[Entity, EntityReference],
      allowMutation: Boolean
  ): InliningOutputStream =
    new InliningOutputStream(re, entityRefs, mutable.Buffer(), mutable.Set.empty, allowMutation) {
      def inlinedEntities: Seq[SerializedEntity] = inlineBuf
    }

  private[optimus] def getBaseTypes(entityInfo: ClassEntityInfo): Seq[String] =
    entityInfo.baseTypes.iterator.map { _.runtimeClass.getName }.toIndexedSeq

  private[platform] def getEntityInfoByName(entityName: String): ClassEntityInfo =
    EntityInfoRegistry.getClassInfo(entityName)

  @async protected def serializeImpl[S <: Entity](
      s: S,
      cmid: Option[MSUuid],
      out: InliningOutputStream
  ): MultiSlotSerializedEntity = {
    val entityRefs = out.entityRefs

    val skeys = s.$info.asInstanceOf[ClassEntityInfo].indexes flatMap {
      _.asInstanceOf[IndexInfo[S, _]].entityToSerializedKey(s, entityRefs)
    }

    val entityInfo = s.$info.asInstanceOf[ClassEntityInfo]
    val types: Seq[String] = getBaseTypes(entityInfo)

    out.writeStartObject()
    s.pickle(out)
    out.writeEndObject()

    val className = s.getClass.getName
    val versionedProperties = {
      /*
       * There are a few options for TemporalContext here. This is my best guess about the correct one.
       * 1) You could use load context of the entity being serialized. But this is clearly bad, because
       *    (a) it might be a temporary entity, and (b) there's no reason to think that the time at which
       *    Fred is loaded should have any bearing on the temporality with which I execute versioning to
       *    e.g. load Bob or Dave.
       * 2) You could use the VT & TT of the write. This in some ways makes most sense because it ties
       *    the temporality of your data in a conceptually pleasing way. But you don't have the TT of your
       *    write on the client side, so the best you could do is now-ish which is obviously bad. Also,
       *    it's conceivable that you don't always *want* to tie the temporalities: maybe your versioning
       *    goes off and reads e.g. currency refdata, which you refresh once a week in your temporal surface,
       *    and that's what you read in your versioning code. If that's true, you really want to pull your
       *    VT&TT for versioning from your temporal surface instead.
       * 3) You could use the VT & TT from the temporal surface. This is basically the only real alternative
       *    to (2). It's the option I've gone for here.
       * Please feel free to change this in consideration of the above.
       */
      val afterTransforms = TransformerRegistry
        .versionToWrite(
          className,
          out.value.asInstanceOf[Map[String, Any]],
          entityInfo.slot,
          TemporalSource.loadContext)
        .get
      afterTransforms.aseq.map { case (slot, props) =>
        (slot, TransformerRegistry.executeForcedWriteTransform(className, props, TemporalSource.loadContext))
      }
    }

    val cmRef = cmid.map(cmid => CmReference.apply(cmid.asBytes))

    val ses = versionedProperties.map { case (slot, props) =>
      require(
        skeys.forall { skey =>
          // NB there's not much we can do about indexed defs because the
          // property map doesn't match up with the blob property map so this
          // requirement only works in some cases
          skey.properties.forAll { (prop, value) =>
            val pValue = props.get(prop)
            pValue.isEmpty || optimus.scalacompat.Eq.eql(pValue.get, value) ||
            (skey.indexed && !skey.unique && (pValue.get match {
              case it: Iterable[_] => it.exists(_ == value)
              case it: Array[_]    => jutil.Arrays.asList(it).asScala.contains(value)
              case _               => false
            }))
          }
        },
        "Indexed and keyed properties should not be mutated by versioning code"
      )
      val (properties, linkages) = {
        val linkages = (out.childToParentProperties map { case l @ EntityLinkageProperty(prop, _) =>
          l -> (props(prop).asInstanceOf[Iterable[EntityReference]].toSet map SerializedEntity.EntityLinkage)
        }).toMap
        val properties = props -- (linkages.keys map { _.propertyName })
        (properties, if (s.$info.linkages.isEmpty) None else Some(linkages))
      }

      SerializedEntity(
        entityRef = out.getEntityRef(s),
        cmid = cmRef,
        className = s.getClass.getName,
        properties = properties,
        keys = skeys,
        types = types,
        inlinedEntities = out.inlinedEntities,
        linkages = linkages,
        slot = slot
      )
    }
    MultiSlotSerializedEntity(ses.toSeq)
  }

  @async protected def getEntity(
      data: SerializedEntity,
      loadContext: TemporalContext,
      storageInfo: StorageInfo,
      instanceMap: InlineEntityHolder,
      temporary: Boolean = false,
      forcePickle: Boolean = false
  ): Entity = {
    val info =
      try {
        getEntityInfoByName(data.className)
      } catch {
        case _: ClassNotFoundException if upcastDomain.isDefined =>
          getUpcastingTargetEntityInfo(data)
        case ex: ClassNotFoundException =>
          log.error(s"Cannot find class ${data.className}")
          throw ex
        // DO NOT DO THIS. JVM/CLASSLOADER-WIDE STATE CANNOT DEPEND ON RUNTIME ENVIRONMENT!!!
        // EntityInfoRegistry.registry.update(data.className, tInfo)
      }
    val klassName = info.runtimeClass.getName

    val childToParentProperties = {
      // we figure out any linkages sent by the broker. These will be present when the
      // childToParent sets sent during persistence were non-empty.
      if (data.linkages.isDefined) {
        data.linkages.get.map { case (lp, links) => (lp.propertyName, links.map(_.permRef).toList) }
      } else Map.empty
    }

    val withC2P = data.properties ++ childToParentProperties

    val versioned = version(klassName, withC2P, loadContext)

    if (DiagnosticSettings.samplingProfilerStatic)
      storageInfo match {
        case dsi: DSIStorageInfo => OGTrace.countDistinct(Cardinality.versionedEntityRef, dsi.versionedRef.longHash)
        case _                   => // we don't care about the other cases
      }

    val in = new PickledMapWrapper(versioned, loadContext, data.entityRef, instanceMap)
    handleDeserializationExceptions(klassName, withC2P, versioned, data.entityRef) {
      info.createUnpickled(in, forcePickle, storageInfo, if (temporary) null else data.entityRef)
    }
  }

  protected def setCmid(ret: Entity, cmid: Option[CmReference]): Entity = {
    ret.dal$cmid_=(cmid.map { cmr: CmReference =>
      new MSUuid(cmr.data, true)
    })
    ret
  }
}

object EntitySerializer extends EntitySerialization {

  @async def serialize(s: Entity): MultiSlotSerializedEntity = serialize(s, mutable.Map.empty)

  @async private[optimus] def serialize(
      s: Entity,
      entityRefs: mutable.Map[Entity, EntityReference]
  ): MultiSlotSerializedEntity = serialize(s, entityRefs, None, allowMutation = true)

  // It's very annoying that EntitySerializer::serialize returns a MultiSlotSerializedEntity rather than a SerializedEntity.
  // Here is the reasoning.
  // - in order to do multi-slot writes, we have to be able to write multiple versions of a SerializedEntity, not just one, from a single
  //   client api DAL.put/DAL.upsert
  // - that means the versioning code needs to give us something which takes a single entity and spits out multiple (at various versions)
  // - at the moment, the versioning code is invoked during serialization so the two options are to have serialization return a
  //   Seq[SerializedEntity] or to invoke verisoning after serialization is done
  // - the second of these is preferable. However, serialization also takes the linkages out of the properties map. That screws with
  //   versioning because versioning is entirely shape-based and doesn't know about childToParent
  // - making versioning care about childToParent -- i.e. ignore childToParent in its shape evaluation -- is non-trivial because on read
  //   the broker has already shoved all the childToParent properties back into the properties map. So on read, the runtime has to expect
  //   to see a childToParent property, whereas on write it would have to expect not to with the above change.
  // We should do something about this when the path is clearer. Maybe EntitySerializer::serialize should not strip childToParent properties
  // from the entity, instead leaving those to be stripped elsewhere after versioning.
  @async private[optimus] def serialize[S <: Entity](
      s: S,
      entityRefs: mutable.Map[Entity, EntityReference],
      cmid: Option[MSUuid],
      allowMutation: Boolean
  ): MultiSlotSerializedEntity = serializeImpl(s, cmid, createOutputStream(s, entityRefs, allowMutation))

  private[optimus /*platform*/ ] def serializePersistent(s: Entity): PersistentEntity = {
    require(s.dal$entityRef ne null)

    s.dal$storageInfo match {
      case DSIStorageInfo(lt, vt, tt, vref) =>
        new PersistentEntity(serialize(s).someSlot, vref, lt, vt, TimeInterval(tt))
    }
  }

  @async
  private[optimus] def deserializeTemporary(
      data: SerializedEntity,
      loadContext: TemporalContext,
      instanceMap: InlineEntityHolder,
      storageInfo: StorageInfo,
      entityRef: EntityReference
  ): Entity = {
    val ret = getEntity(data, loadContext, storageInfo, instanceMap)
    setCmid(ret, data.cmid)
  }

  @async
  private[optimus] final def deserializeTemporary(
      data: SerializedEntity,
      loadContext: TemporalContext,
      instanceMap: InlineEntityHolder
  ): Entity = deserializeTemporary(data, loadContext, instanceMap, AppliedStorageInfo, data.entityRef)

  @async
  private[optimus] def deserialize(
      data: SerializedEntity,
      loadContext: TemporalContext,
      storageInfo: StorageInfo
  ): Entity = {

    // the inline entities may refer to each other so they all need a copy of the full map - to resolve the chicken and
    // egg problem we use a mutable map. Note that they don't actually retrieve anything from the map until LPRs are
    // resolved (later on), so we can defer adding anything to that map until they are all deserialized
    val inlineHolder = if (data.inlinedEntities.nonEmpty) {
      val inlineMap = new mutable.HashMap[EntityReference, Entity]()
      // important to always use the same holder instance in all of the inlined entities - that is what works around the
      // deserialization bug (see InlineEntityHolder class comment)
      val inlineHolder = InlineEntityHolder(inlineMap)
      inlineMap ++= {
        data.inlinedEntities.map { ser =>
          // TODO (OPTIMUS-10933): We need to eliminate the assignment of dal$XXXX here, however, we're going to decommission inline entity soon
          val d = deserializeTemporary(ser, loadContext, inlineHolder, AppliedStorageInfo, ser.entityRef)
          (ser.entityRef, d)
        }
      }
      inlineHolder
    } else {
      // Lots of entities don't have inlined entities and having a singleton saves tones of memory
      // compared to a new mutable.HashMap even when empty
      InlineEntityHolder.empty
    }

    deserializeTemporary(data, loadContext, inlineHolder, storageInfo, data.entityRef)
  }

  @async
  final def deserialize(data: SerializedEntity, loadContext: TemporalContext): Entity =
    deserialize(data, loadContext, AppliedStorageInfo)

  // use temporal context hydrate instead
  @scenarioIndependent @node def hydrate[A](pe: PersistentEntity, tc: TemporalContext): A = {
    val storageInfo = new DSIStorageInfo(pe.lockToken, pe.vtInterval, pe.txInterval.from, pe.versionedRef)
    val entity = tc.deserialize(pe, storageInfo)
    entity.asInstanceOf[A]
  }

  @scenarioIndependent @node private[optimus] def hydrateWithFlatTemporalContext[A](pe: PersistentEntity): A =
    hydrate[A](pe, FlatTemporalContext(pe.vtInterval.from, pe.txInterval.from, Some("hydrateWithFlatTemporalContext")))

}
