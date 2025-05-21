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
import optimus.exceptions.RTExceptionTrait
import optimus.platform.{HasTTContext, TemporalContext, TimeInterval, ValidTimeInterval}
import optimus.platform.dsi.bitemporal
import optimus.platform.dsi.bitemporal.{DSIQueryTemporality, Query}
import optimus.platform.storable._

/**
 * Base class for DAL exceptions specific to an entity instance.
 */
abstract class BoundDALException(val entity: Entity, message: String, cause: Throwable)
    extends DALException(BoundDALException.createMessage(entity, message), cause) {
  def this(entity: Entity, message: String) = this(entity, message, null)
}

private object BoundDALException {
  def createMessage(entity: Entity, message: String): String = {
    val base = "Error with entity " + entity
    message match {
      case null => base
      case m    => base + ": " + m
    }
  }
}

class AmbiguousKeyException(entity: Entity)
    extends DALException("Entity " + entity + " has multiple keys that resolve to different references")

class EventLinkResolutionException(val ref: BusinessEventReference, val temporalContext: TemporalContext, msg: String)
    extends DALRTException(msg) {
  def this(r: BusinessEventReference, temporalContext: TemporalContext) =
    this(r, temporalContext, s"Invalid event reference: $r@(temporalContext=$temporalContext)")
}

class MissingEventEffectLinkException(val ref: VersionedReference, msg: String) extends DALRTException(msg)

/**
 * Base class for exceptions caused by key or unique index already being used by another persisted entry.
 */
sealed abstract class DuplicateKeyException(entity: Entity, e: bitemporal.DuplicateKeyException)
    extends BoundDALException(entity, e.getMessage, e)

/**
 * The key is already being used by another persisted entity
 */
class DuplicateStoredKeyException(entity: Entity, e: bitemporal.DuplicateStoredKeyException)
    extends DuplicateKeyException(entity, e)

class SecureEntityException(entity: Entity, e: bitemporal.SecureEntityException)
    extends BoundDALException(entity, e.getMessage, e)

/**
 * The unique index is already being used by another persisted entity
 */
class DuplicateUniqueIndexException(entity: Entity, e: bitemporal.DuplicateUniqueIndexException)
    extends DuplicateKeyException(entity, e)

/**
 * Optimistic locking exception - the entity has been updated by someone else and is not up-to-date
 */
class OutdatedVersionException(entity: Entity, e: bitemporal.OutdatedVersionException)
    extends BoundDALException(
      entity,
      "Cannot persist entity because there is an existing version in the datastore with a different lock token. " +
        "This can happen when an entity is updated or invalidated in the datastore after being loaded by the DAL, " +
        "or possibly when saving an entity at a different valid time where a different version of an entity is valid.",
      e
    ) {
  // Should be required, but old brokers may omit reference.
  // require(entity ne null)
}

/**
 * The entity was not found.
 */
class EntityNotFoundException(key: Key[_], temporalContext: TemporalContext)
    extends DALRTException(s"Entity not found with key $key at $temporalContext")

class EntityNotFoundByRefException(ref: EntityReference, temporalContext: TemporalContext)
    extends DALRTException(s"Entity (ref: ${ref}) not found at $temporalContext")

class EntityNotFoundByCmidException(cmid: MSUuid, temporalContext: TemporalContext)
    extends DALRTException(s"Entity (cmid: ${cmid}) not found at $temporalContext")

class EntityNotMappedException(
    ref: EntityReference,
    queryTime: DSIQueryTemporality.At,
    itemTime: DSIQueryTemporality.At,
    query: Query)
    extends DALRTException(
      s"Entity (ref: ${ref}) was valid at queryTime ($queryTime) but not at itemTime ($itemTime). Query was $query")

class EventNotFoundException(key: Key[_], tc: HasTTContext)
    extends DALException(s"Event not found with key $key at tt context $tc")

class EventNotFoundByRefException(beref: BusinessEventReference, tt: Instant)
    extends DALException(s"Event not found with reference ${beref} at tt ${tt}")

class EventNotFoundByCmidException(cmid: MSUuid, tt: Instant)
    extends DALException(s"Event not found with cmid ${cmid} at tt ${tt}")

class EventTransitionException(str: String) extends DALRTException(str)

class SingleItemDataNotFoundForEntityRefException(ref: EntityReference, temporality: DSIQueryTemporality.At)
    extends DALRTException(s"SingleItemData for Entity ($ref) not found at $temporality.")

/**
 * The entity was expected to be valid on validTime in the datastore, but it either does not exist in the datastore or
 * it is not valid at the given time.
 */
class EntityNotValidAtValidTimeException(msg: String)
    extends DALException(msg
      + " This may be caused by trying to save an existing entity at a past date without also saving the entities it links to, or by an entity having been deleted since the persist block began.") {
  def this(ref: EntityReference, validTime: Instant) = this("The entity " + ref + " is not valid at " + validTime + ".")
}

/**
 * Update or invalidate called on an entity that was not loaded from the DAL.
 */
class UnsavedEntityUpdateException(entity: Entity)
    extends BoundDALException(
      entity,
      "Cannot update or invalidate an entity which was not loaded from the DAL: " + entity)

class KeyConflictException(e1: Entity, e2: Entity)
    extends DALException(s"Cannot persist multiple entities with same key: $e1, $e2")
class ReplaceKeyConflictException(val existing: Entity, val updated: Entity)
    extends DALException(s"Cannot replace $existing with $updated as keys are not equivalent")

class DALTransactionConflictException(entity: Entity, msg: String) extends BoundDALException(entity, msg)

class DALEventConflictException(msg: String) extends DALException(msg)

class GeneralDALException(message: String) extends DALException(message)

/**
 * Exception thrown when lead writer completely halts
 */
class WriteLeaderHaltException(message: String) extends DALException(message)

/**
 * Exception returned by the DSI - normally an unexpected, DSI-specific error
 */
abstract class DSIException(e: bitemporal.DSIException) extends DALException(e.getMessage, e)

/**
 * Exception returned by the DSI - normally an unexpected, DSI-specific error
 */
class UnboundDSIException(e: bitemporal.DSIException) extends DSIException(e)

/**
 * Exception thrown by dal client when read tt is ahead of LSQT
 */
class TTTooFarAheadException(e: bitemporal.DSIException) extends DSIException(e)

/**
 * Exception returned by the DSI - normally an unexpected, DSI-specific error
 */
class BoundDSIException(entity: Entity, e: bitemporal.DSIException) extends DSIException(e)

/**
 * The schema of the loaded entity does not match the current definition.
 */
class IncompatibleVersionException(message: String, cause: Throwable) extends DALRTException(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * ambiguous link resolution - e.g. try to resolve an entity over an interval
 */
class AmbiguousLinkResolutionException(entityRef: EntityReference)
    extends DALException("Entity Reference " + entityRef + " maybe resolved to multiple different versions")

class PersistTransientEntityException(val entity: Entity)
    extends DALException("Cannot persist non-@stored entity: " + entity)

class PersistModuleEntityException(val entity: Entity)
    extends DALRTException(
      "Cannot directly persist module (i.e. object) entity: " + entity + " (modules can only be persisted as a reference from another entity)")

/**
 * History API Exceptions
 */
/**
 * trying to load an event version for a tt where it doesn't exist
 */
class EventVersionNotFoundByRefException(beref: BusinessEventReference, vid: Int, vt: Instant, tt: Instant)
    extends DALRTException(
      s"Event version not found with beref: ${beref} vid: ${vid} vt:${vt} at tt:${tt}, event version is not valid at the given tt possibly event version has been modified")

/**
 * Create/Invalidate Business events could not be loaded from DAL as entity was modified without using events
 */
class EventCantBeLoadedUsingNoneEventHandle(val msg: String) extends DALRTException(msg)

/**
 * out of bound entity read
 */
class OutOfBoundEntityRead(val eref: EntityReference, val rtt: Instant)
    extends DALException(s"history for entity: ${eref} is available until this read transaction time:${rtt}")

/**
 * out of bound entity read
 */
class OutOfRectEntityRead(val eref: EntityReference, val vtInterval: ValidTimeInterval, val ttInterval: TimeInterval)
    extends DALException(
      s"can not read entity : ${eref} beyond the rectangle coordinates vtInterval: ${vtInterval} ttInterval: ${ttInterval}")

/**
 * out of bound event read
 */
class OutOfBoundEventRead(val beref: BusinessEventReference, val rtt: Instant)
    extends DALException(s"history for event: ${beref} is available until this read transaction time:${rtt}")

/**
 * application forbidden e.g. using dangerous api without explicit permissions
 */
class ApplicationNotPermissioned(message: String) extends DALException(message)

class EntityUpcastingException(message: String) extends DALException(message) with RTExceptionTrait

class EventUpcastingException(message: String) extends DALException(message)

/**
 * Exception for reading data at TxTime far ahead of latest safe query time..
 */
class ReadFarAheadOfLsqtException(msg: String) extends DALException(msg)

/**
 * Request timeout exception if request takes more time configured 'maxWaitingTime'..
 */
class DalTimedoutException(timeout: Int) extends DALException(s"DAL request timed out after $timeout millis")

/* Command-to-result correlation exceptions */
class MissingResultException extends DALException("The broker failed to return an expected result")

class UnexpectedResultException extends DALException("The broker returned an unexpected result")

class ZappUpgradeException(message: String) extends DALException(message)
