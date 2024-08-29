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

import scala.annotation.nowarn
import optimus.platform._
import optimus.platform.annotations.deprecating
import optimus.platform.storable._
import msjava.slf4jutils.scalalog.getLogger
import java.time.Instant
import optimus.platform.versioning.{Shape, TransformerRegistry}
import optimus.platform.dsi.bitemporal.{NoWriteShapeException, EntityClassQuery}
import optimus.platform.bitemporal.{EntityBitemporalSpace, EntityRectangle}
import optimus.core.CoreAPI
import optimus.platform.AsyncImplicits._

trait DALSchemas {
  def resolver: ResolverImpl
  private val log = getLogger(this)

  new java.util.LinkedList
  object schemaVersions {
    @deprecating("To be replaced by classpath registration")
    private[optimus] def create(className: SerializedEntity.TypeRef, schemaVersion: Int): Unit = {
      create(className, Set(schemaVersion))
    }

    /**
     * Create schema version
     */
    @deprecating("To be replaced by classpath registration")
    private[optimus] def create(className: SerializedEntity.TypeRef, schemaVersions: Set[Int]): Unit = {
      val alreadyExistingSlots = resolver.createSchemaVersions(className, schemaVersions)
      if (alreadyExistingSlots.nonEmpty)
        log.warn(
          s"The following slots already existed for class name ${className}: ${alreadyExistingSlots.mkString(",")}. The slot creation operation still completed successfully.")
    }

    @async
    def getActiveSlots(className: SerializedEntity.TypeRef): Set[Int] = {
      resolver.getSchemaVersions(className)
    }

    @entersGraph
    def versionAll[E <: Entity](
        companion: EntityCompanionBase[E],
        targetSchemaVersion: Int,
        beforeTxTime: Instant,
        entitledOnly: Boolean = false) = {
      val className = companion.info.runtimeClass.getName
      val targetShape = TransformerRegistry
        .getWriteShapes(className)
        .flatMap(_.get(targetSchemaVersion))
        .getOrElse(throw new NoWriteShapeException(className, targetSchemaVersion))
      val query = EntityClassQuery(className, entitledOnly)
      val references = resolver.findEntitiesNotAtSlot(query, targetSchemaVersion, className, beforeTxTime)
      val versioned = references.get.apar.map { reference =>
        @nowarn("msg=10500 optimus.core.CoreAPI.nodeResult")
        val versionedEntities =
          CoreAPI.nodeResult(versionOneReference(targetSchemaVersion, targetShape, reference, beforeTxTime)).value
        reference -> versionedEntities
      }
      versioned foreach { case (ref, newVersions) =>
        resolver.populateSlot(className, ref, newVersions)
      }
    }

    @entersGraph
    def versionAllKeys[E <: Entity](
        companion: EntityCompanionBase[E],
        targetSchemaVersion: Int,
        keys: Seq[Key[E]],
        beforeTxTime: Instant,
        blobLookup: NodeFunction2[Key[E], Instant, SerializedEntity]) = {
      val className = companion.info.runtimeClass.getName
      val skeys = keys.map { k =>
        val skey = k.toSerializedKey
        require(skey.isKey, "Cannot call versionAllKeys against non-@key property")
        k -> skey
      }.toMap
      val versioned = skeys.apar.map { case (key, skey) =>
        val versioner = asNode { (entityRectangle: EntityRectangle[E]) =>
          blobLookup(key, entityRectangle.vtFrom)
        }
        val space =
          EntityEventModule.EntityOps.getBitemporalSpace[E](skey, QueryTemporality.All, beforeTxTime)
        val versioned = createTargetEntityVersions(targetSchemaVersion, space, beforeTxTime, versioner)
        space.eref -> versioned
      }
      versioned.foreach { case (ref, newVersions) =>
        resolver.populateSlot(className, ref, newVersions)
      }
    }

    // @scenarioIndependent? I think that would need tweaking the loadContext before calling transformer.forwards, which may not be possible
    // at this point in the project dependency chain?
    // INVARIANT: because this acts per entity reference we have an invariant that any given entity ref must either get versioned or not, atomically.
    @node def versionOneReference[E <: Entity](
        targetSchemaVersion: Int,
        targetShape: Shape,
        reference: EntityReference,
        beforeTxTime: Instant) = {
      val versioner = asNode { (entityRectangle: EntityRectangle[E]) =>
        val persistentEntity = entityRectangle.pe
        val sourceShape = Shape.fromProperties(persistentEntity.className, persistentEntity.properties)
        val versionStoreContext =
          FlatTemporalSurfaceFactory.createTemporalContext(entityRectangle.vtFrom, entityRectangle.ttFrom, None)
        val versionedProperties = TransformerRegistry
          .version(persistentEntity.properties, sourceShape, targetShape, versionStoreContext)
          .getOrElse {
            throw new IllegalArgumentException(
              s"No transformers found to version from ${sourceShape} to ${targetShape}")
          }
        persistentEntity.serialized.copy(properties = versionedProperties)
      }
      val space = EntityEventModule.EntityOps.getBitemporalSpace[E](reference, beforeTxTime)
      createTargetEntityVersions(targetSchemaVersion, space, beforeTxTime, versioner)
    }

    @node private def createTargetEntityVersions[E <: Entity](
        targetSchemaVersion: Int,
        space: EntityBitemporalSpace[E],
        beforeTxTime: Instant,
        versioner: NodeFunction1[EntityRectangle[E], SerializedEntity]): Seq[(VersionedReference, SerializedEntity)] = {
      space.all.apar.map { entityRectangle =>
        val versioned = versioner(entityRectangle)
        entityRectangle.vref -> versioned.copySerialized(
          slot = targetSchemaVersion,
          entityRef = space.eref
        ) // just in case
      }
    }
  }
}
