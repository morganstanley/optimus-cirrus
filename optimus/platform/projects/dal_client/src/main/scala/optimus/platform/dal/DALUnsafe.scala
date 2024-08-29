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
import optimus.platform._
import optimus.platform.annotations.deprecating
import optimus.platform.annotations.parallelizable
import optimus.platform.internal.TemporalSource
import optimus.platform.storable.Entity
import optimus.platform.storable.EntityReference
import optimus.platform.storable.EntityReferenceHolder
import optimus.platform.storable.EntityVersionHolder
import optimus.platform.temporalSurface.operations.EntityReferenceQueryReason

trait DALUnsafe {
  def resolver: ResolverImpl

  object unsafe {
    private[this] def uuidToEref(uuid: MSUuid): EntityReference = {
      val bs = uuid.asBytes()
      // There is an MSUuid.UIDSIZE available, but (a) not from here and (b) not used consistently within
      // the MSUuid code itself.
      // An even larger mystery is why we need the assertion here at all.  What it the source of the condition
      // we're trying to detect, and why is throwing an AssertionError the best way to detect it
      // Mysteries abound.
      assert(bs.length == 16)
      EntityReference(bs)
    }

    private[this] def assignUuid(e: Entity, uuid: MSUuid): Unit = {
      if (uuid == null)
        throw new NullPointerException(f"Cannot upsert $e with null uuid.")

      val ref = uuidToEref(uuid)
      val existing = e.dal$entityRef

      if ((existing ne null) && (existing != ref))
        throw new IllegalArgumentException(f"Entity $e already has different assigned reference $existing.")

      if (existing eq null)
        e.dal$entityRef = ref
    }

    def upsertWithUuid(e: Entity, uuid: MSUuid, vt: Instant): Unit = {
      assignUuid(e, uuid)
      DALImpl.upsert(e, vt)
    }

    def upsertWithUuid(e: Entity, uuid: MSUuid): Unit = {
      assignUuid(e, uuid)
      DALImpl.upsert(e)
    }

    def invalidateDoNotUse[E <: Entity: Manifest](holder: EntityVersionHolder[E]): Unit = {
      DALImpl.unsafeInvalidate(holder)
    }

    def invalidateDoNotUse[E <: Entity: Manifest](holder: EntityReferenceHolder[E]): Unit = {
      DALImpl.unsafeInvalidate(holder)
    }

    @node def getByUuid[E <: Entity: Manifest](uuid: MSUuid): E = {
      val ref = uuidToEref(uuid)
      val actualType = manifest[E].runtimeClass.asSubclass(classOf[Entity])
      val temporalContext = TemporalSource.loadContext
      val ent = resolver
        .findByReferenceWithType(ref, temporalContext, actualType, false, EntityReferenceQueryReason.Unknown)
      ent match {
        case e: E => e
        case _ =>
          throw new LinkResolutionException(
            ref,
            temporalContext,
            s"Exception type $actualType but got ${ent.getClass} for entity with uuid $uuid and entity payload = $ent"
          )
      }
    }

    @deprecating(
      "DO NOT USE. This is going to be removed soon and is incompatible with dal partitioning. " +
        "Use getByUuidOption instead.")
    @node def unsafeGetByUuidOption(uuid: MSUuid): Option[Entity] = {
      val ref = uuidToEref(uuid)
      resolver.findByReferenceOption(ref, TemporalSource.loadContext)
    }

    @node def getByUuidOption[E <: Entity: Manifest](uuid: MSUuid): Option[E] = {
      val ref = uuidToEref(uuid)
      val actualType = manifest[E].runtimeClass.asSubclass(classOf[Entity])
      val temporalContext = TemporalSource.loadContext
      getEntityOptByUuid[E](uuid, actualType, ref, temporalContext)
    }

    @node def getByUuidOptionForClass[E <: Entity: Manifest](uuid: MSUuid, actualType: Class[E]): Option[E] = {
      val ref = uuidToEref(uuid)
      val temporalContext = TemporalSource.loadContext
      getEntityOptByUuid[E](uuid, actualType, ref, temporalContext)
    }

    @parallelizable
    def uuidOf(e: Entity): MSUuid = {
      val ref = e.dal$entityRef
      if (ref eq null) {
        null
      } else {
        new MSUuid(ref.data, true)
      }
    }
  }

  @node private def getEntityOptByUuid[E <: Entity: Manifest](
      uuid: MSUuid,
      actualType: Class[_ <: Entity],
      ref: EntityReference,
      temporalContext: TemporalContext) = {
    val entOpt = resolver
      .findByReferenceWithTypeOption(ref, temporalContext, actualType, false, EntityReferenceQueryReason.Unknown)
    entOpt.map { ent =>
      ent match {
        case e: E => e
        case _ =>
          throw new LinkResolutionException(
            ref,
            temporalContext,
            s"Exception type $actualType but got ${ent.getClass} for entity with uuid $uuid and entity payload = $ent"
          )
      }
    }
  }
}
