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
package optimus.platform.dsi.bitemporal

import msjava.slf4jutils.scalalog.getLogger
import optimus.exceptions.RTExceptionTrait
import optimus.platform.EvaluationContext
import optimus.platform.dal.config.DalEnv
import optimus.platform.dsi.versioning.VersioningKey
import optimus.platform.dsi.versioning.VersioningRedirectionInfo
import optimus.platform.storable.BusinessEventReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.SerializedEntity
import optimus.platform.storable.SerializedKey
import optimus.platform.storable._
import org.apache.commons.codec.binary.Hex

import java.net.URLEncoder
import java.time.Duration
import java.time.Instant
import scala.util.control.NonFatal

object MessageContainsDALEnv {
  private val log = getLogger[MessageContainsDALEnv]

  private var detectedEnv: Option[String] = None
  private[bitemporal] def tryDetectEnv: Option[String] =
    try {
      detectedEnv.orElse {
        val env = Option(EvaluationContext.scenarioStackOrNull)
          .flatMap(s => Option(s.env))
          .flatMap(e => Option(e.config))
          .flatMap(c => Option(c.runtimeConfig))
          .flatMap(rc => Option(rc.env))
        if (env.isDefined && env.get.nonEmpty) detectedEnv = env
        env
      }
    } catch {
      case NonFatal(ex) =>
        log.warn(s"Exception occurred while extracting env from the runtime. Will set empty env", ex)
        None
    }
}

trait MessageContainsDALEnv extends Exception {
  import MessageContainsDALEnv._

  private val prefix = {
    val detectedEnv = tryDetectEnv

    val toPrefix = (txt: String) => s"[${txt}] "

    detectedEnv.map(toPrefix).getOrElse {
      log.debug(
        s"exception ${this.getClass.getName} is initialized outside of an evaluation context, will not be able to display DAL env")
      ""
    }
  }

  override def getMessage = prefix + super.getMessage

}

/**
 * A base exception for DSI-related errors.
 */
abstract class DSIException(msg: String, cause: Throwable)
    extends RuntimeException(msg, cause)
    with MessageContainsDALEnv {
  def this() = this(null, null)
  def this(message: String) = this(message, null)
}

// Some exceptions can point to state that needs to be read by the client.  See comment on ErrorResult.
trait ReadAfterException { this: DSIException =>
  def readAfter: Instant
}

/**
 * Catch-all exception for general DSI exceptions.
 */
class DSISpecificError(message: String, cause: Throwable) extends DSIException(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * A transient exception for DSI-related errors, suggesting that if the request is retried, it may succeed.
 */
class DsiSpecificTransientError(message: String) extends DSIException(message)

object DsiSpecificTransientError {
  val UnderlyingDatabaseTimeout = "Query timeout on underlying database"
}

trait TransmissionError extends Exception

abstract class TransientTransmissionError(msg: String) extends DsiSpecificTransientError(msg) with TransmissionError

class ConnectUnderlyingTimeoutException(msg: String) extends TransientTransmissionError(msg)

class AssertValidException(msg: String) extends DSIException(msg) {
  def this(ref: EntityReference, validTime: Instant) = this(s"Entity ${ref} is not valid at time ${validTime}.")
}

class KeyResolutionException(message: String, val key: SerializedKey) extends DSIException(message)

class UnsupportedSessionException(message: String) extends DSIException(message)

/**
 * Indicates that the bitemporal space for an entity was empty when viewed at the given read transaction time
 *
 * @param rtt
 *   the read transaction time at which the bitemporal space was viewed
 */
class EmptyBitemporalSpaceException(rtt: Instant)
    extends DSISpecificError(s"Entity bitemporal space is empty at read transaction time $rtt")
    with RTExceptionTrait

/**
 * Base exception for duplicate key or unique index.
 */
sealed abstract class DuplicateKeyException protected (
    val ref: EntityReference,
    val readAfter: Instant,
    val inputEntityRef: Option[EntityReference],
    val key: Option[SerializedKey],
    msg: String)
    extends DSIException(
      s"${msg} is currently used on another entity with reference: ${ref}. Input EntityRef: ${inputEntityRef} SerializedKey: ${key} ")
    with ReadAfterException

object DuplicateKeyException {
  def extractClientRef(refOpt: Option[EntityReference]): Option[EntityReference] = refOpt.map { ref =>
    ref match {
      case f: FinalReference => f.unresolvedReference.getOrElse(ref)
      case _                 => ref
    }
  }
}

// readAfter enforces causality on the client so that the conflicting entity may be read.
// See description in OPTIMUS-2857
class DuplicateStoredKeyException private (
    ref: EntityReference,
    readAfter: Instant,
    inputEntityRef: Option[EntityReference],
    key: Option[SerializedKey])
    extends DuplicateKeyException(ref, readAfter, inputEntityRef, key, "Key")

class SecureEntityException(val className: String, val entityOrPackageName: String)
    extends DSIException(
      s"Entities of class '$className' can only be accessed via secure channels due to the configuration for " +
        s"'$entityOrPackageName'. However you are connecting to Optimus DAL with an insecure transport.")

object DuplicateStoredKeyException {
  import DuplicateKeyException.extractClientRef

  def apply(
      existingRef: EntityReference,
      readAfter: Instant,
      inputRef: Option[EntityReference] = None,
      key: Option[SerializedKey] = None): DuplicateStoredKeyException = {
    new DuplicateStoredKeyException(existingRef, readAfter, extractClientRef(inputRef), key)
  }
}

class DuplicateUniqueIndexException(
    ref: EntityReference,
    readAfter: Instant,
    inputEntityRef: Option[EntityReference] = None,
    key: Option[SerializedKey] = None)
    extends DuplicateKeyException(ref, readAfter, inputEntityRef, key, "Unique Index")

object DuplicateUniqueIndexException {
  import DuplicateKeyException.extractClientRef

  def apply(
      existingRef: EntityReference,
      readAfter: Instant,
      inputRef: Option[EntityReference],
      key: Option[SerializedKey]): DuplicateUniqueIndexException = {
    new DuplicateUniqueIndexException(existingRef, readAfter, extractClientRef(inputRef), key)
  }
}

/*
 * There is a version existing at a later VT than the VT in the request. This occurs when a heap entity is stored using DAL.put
 */
class EntityExistAtLaterVtException(val ref: EntityReference, val writeVt: Instant, existingVt: Instant)
    extends DSIException(
      s"Cannot put a new version for entity reference ${ref} at valid time: ${writeVt} because there exists a valid version at ${existingVt}")

/**
 * There is another entity of different class name from the class hierarchy with the same key
 */
class DuplicateKeyInClassHierarchyException(message: String) extends DSIException(message)

/**
 * Broker is not able to deserialize object from underlying storage
 */
class BrokerDeserializationException(message: String) extends DSIException(message)

/**
 * Broker is not able to serialize the object in the underlying storage format
 */
class BrokerSerializationException(message: String) extends DSIException(message)

/**
 * * Two transactions with same appevent reference received on the broker side.
 */
class DuplicateAppEventReferenceException(message: String) extends DSIException(message)

/**
 * Broker rejected the request because it could never be served, e.g. the zone is capped and has 0 limit
 */
class UnserveableRequestException(message: String, cause: Throwable) extends DSIException(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * A newer version of the entity has been saved.
 */
final class OutdatedVersionException(val ref: EntityReference)
    extends DSIException(s"Stale lock token for entity reference $ref")

/**
 * A newer version of the event has been saved.
 */
final class OutdatedEventVersionException(val ref: BusinessEventReference, val expected: Long, val provided: Long)
    extends DSIException(
      s"Cannot update event with ref $ref due to stale lock token. Expected $expected, got $provided.") {
  require(ref ne null)
}

/**
 * The entity is being written at a slot which has not yet been created
 */
final class NonExistentSchemaVersionException(
    val ref: EntityReference,
    val className: SerializedEntity.TypeRef,
    val slots: Set[Int])
    extends DSIException(
      s"Cannot write entity ${ref} of type ${className} at non-existent schema versions ${slots.mkString(",")}")

/**
 * The entity should have slot payload if it is in writeSlots of StoredClassMetadata.
 */
final class MissingSlotPayloadException(
    val ref: EntityReference,
    val className: SerializedEntity.TypeRef,
    val slots: Set[Int])
    extends DSIException(
      s"Cannot write entity ${ref} of type ${className} due to missing payload in slot ${slots.mkString(",")}")

/**
 * The existing entity was not found.
 */
class EntityNotFoundException(msg: String = "") extends DSIException(msg)

/**
 * The existing business event was not found.
 */
class BusinessEventNotFoundException extends DSIException

/**
 * The bitemporal space of proposed unique index overlaps with an existing unique index.
 */
class UniqueIndexSpaceOverlapException(message: String) extends DSIException(message)

/**
 * Thrown when a command is sent to a non-lead broker.
 */
class NonLeaderBrokerException(message: String) extends DSIException(message)

/**
 * Thrown when a read command is sent to a write-only broker.
 */
class NonReaderBrokerException(message: String) extends DSIException(message)

/**
 * Thrown when the override-write entitlement does not permit writing a particular type
 */
class OverrideWriteEntitlementCheckFailed private (message: String, cause: Throwable)
    extends DSIException(message, cause)
object OverrideWriteEntitlementCheckFailed {
  private def fromMessage(message: String, cause: Option[Throwable] = None): OverrideWriteEntitlementCheckFailed =
    new OverrideWriteEntitlementCheckFailed(message, cause.orNull)

  def fromClassNames(
      action: String,
      classNames: Set[SerializedEntity.TypeRef],
      cause: Option[Throwable] = None): OverrideWriteEntitlementCheckFailed =
    fromMessage(
      s"You do not have permission to $action the following write-overridden class(es): ${classNames.mkString(", ")}")
}

/**
 * Exception thrown when entitlement check fails.
 */
class EntitlementCheckFailed private[bitemporal] (message: String, cause: Throwable)
    extends Exception(message, cause)
    with RTExceptionTrait {
  private def this(msg: String) = this(msg, null)
}

object EntitlementCheckFailed {
  private val log = getLogger[EntitlementCheckFailed]

  def fromMessage(
      message: String,
      maybeCause: Option[Throwable] = None,
      enrichMsg: Boolean = true): EntitlementCheckFailed = {
    val enrichingInfo = if (enrichMsg) {
      MessageContainsDALEnv.tryDetectEnv.map(e => s"[$e] ").getOrElse("")
    } else ""
    new EntitlementCheckFailed(s"$enrichingInfo$message", maybeCause.orNull)
  }

  def fromEref(
      message: String,
      eref: EntityReference,
      action: String,
      maybeCause: Option[Exception] = None,
      failedRuleMessage: Option[String] = None): EntitlementCheckFailed = {
    new EntitlementCheckFailed(constructMessage(message, action, Left(eref), failedRuleMessage), maybeCause.orNull)
  }

  def fromClassNames(
      message: String,
      classNames: Seq[String],
      action: String,
      maybeCause: Option[Exception] = None,
      failedRuleMessage: Option[String] = None): EntitlementCheckFailed = {
    new EntitlementCheckFailed(
      constructMessage(message, action, Right(classNames), failedRuleMessage),
      maybeCause.orNull)
  }

  // Converting the Base64 encoded eref to hex for putting in the URL with alpha characters
  def base64ToHex(eref: EntityReference) = {
    Hex.encodeHexString(eref.data)
  }

  def hexToBase64(str: String) = {
    EntityReference(Hex.decodeHex(str.toCharArray))
  }

  def constructMessage(
      originalMessage: String,
      action: String,
      erefOrType: Either[EntityReference, Seq[String]],
      failedRuleMessage: Option[String] = None) = {
    if (failedRuleMessage.isEmpty) {
      val paramWithValue = erefOrType match {
        case Left(eref)       => s"eref=${base64ToHex(eref)}"
        case Right(typeNames) => s"hierarchy=${URLEncoder.encode(typeNames.mkString(","), "UTF-8")}"
      }

      val env = MessageContainsDALEnv.tryDetectEnv.map(e => DalEnv.apply(e).mode).getOrElse("")

      if (env != "") {
        val url = s"http://dalent$env/&action=$action&$paramWithValue"
        val hyperlink = s"<a href=$url>$url</a>"
        s"[$env] $originalMessage. To verify which roles are entitled for given entity check: $hyperlink"
      } else {
        log.warn(s"Unable to find env needed for dalent. Link would be skipped in exception message...")
        originalMessage
      }
    } else {
      s"$originalMessage. Check the details of the failed entitlement rule: ${failedRuleMessage.get}"
    }
  }
}

/**
 * Exception thrown when registration checker encounters semantics violation
 */
class RegistrationCheckFailed(message: String) extends Exception(message) with RTExceptionTrait

/**
 * Exception thrown when a versioning satellite is launched with an invalid or non-existent configuration
 */
class InvalidVersioningConfigurationException
    extends DSIException("Versioning satellites require a valid versioning configuration specifier")

/**
 * Exception thrown when range query can't be handled properly (e.g. too complex entitlement rule)
 */
class RangeQueryException(message: String) extends DSIException(message)

/**
 * Exception thrown when no versioning servers are currently available to handle an incomplete write request
 */
class NoVersioningServersException(val instance: String, val key: VersioningKey)
    extends DSIException(s"No versioning servers registered in ${instance} for versioning key ${key.id}")

/**
 * Exception thrown when a client write does not contain sufficient payloads in its write slots (and thus requires
 * assistance from versioning satellites)
 */
class IncompleteWriteRequestException(val redirectionInfo: VersioningRedirectionInfo)
    extends DSIException(
      s"Cannot execute incomplete write command, server side versioning is required${if (redirectionInfo.classNames.isEmpty) ""
        else s" to produce ${redirectionInfo}"}")

/**
 * Exception thrown when a client write requires a shape that is not registered with the TransformerRegistry
 */
class NoWriteShapeException(val className: String, val targetSlot: Int)
    extends DSIException(
      s"The TransformerRegistry contains no write shape for the target schema version ${className}@${targetSlot}. Consider registering one with TransformerRegistry.registerWriteShape.")

/**
 * Exception thrown when a client dispatches a message containing an old style DSI request to a versioning satellite
 */
class MissingVersioningRequestException
    extends DSIException(
      "Message is missing the actual versioning request, please make sure to use the appropriate protocol version")

/**
 * Exception thrown when a client erroneously dispatches a read request to a versioning satellite
 */
class InvalidVersioningRequestException
    extends DSIException("Versioning satellites do not service either read requests or raw DSI requests")

/**
 * Exception thrown when the write broker does not provide sufficient redirection information for incomplete writes
 */
class InvalidRedirectionInfoException
    extends DSIException(
      "DSI server indicated incomplete writes, but failed to provide sufficient versioning redirection information")

/**
 * Exception thrown when a client versioning request contains commands that are not versionable
 */
class UnversionableCommandException(val command: Command)
    extends DSIException(s"Received unversionable command ${command}")

/**
 * Exception thrown when a client's transformer code attempts to write to the DAL
 */
class IllegalTransformerOperationException
    extends DSIException("Transformer code used in server side versioning is not allowed to write to the DAL")

/**
 * Exception thrown when transformation failed for the slot
 */
class SlotTransformationFailedException(message: String, val causes: Seq[Throwable])
    extends DSIException(s"$message underlyingCause:${causes.map(_.getMessage).mkString(";")}")

/**
 * Exception thrown when transformation failed for a shape
 */
class TransformerNotFoundException(message: String) extends DSIException(message)

/**
 * Exception thrown when write command is executed on a read only DSI
 */
class WriteCommandExecutedInReadOnlyDsiException(message: String) extends DSIException(message)

/**
 * Exception thrown when PubSub command receives some error from PubSub broker
 */
class PubSubException(val streamId: String, message: String) extends DSIException(message)

/**
 * Exception thrown when PubSub command receives a DsiSpecificTransientError from PubSub broker
 */
class PubSubTransientException(streamId: String, message: String) extends PubSubException(streamId, message)

/**
 * Exception thrown when PubSub command is executed on a DSI which doesn't support it
 */
class PubSubCommandUnsupportedException(streamId: String, message: String) extends PubSubException(streamId, message)

/**
 * Thrown when the NS stream couldn't be started on the pubsub broker
 */
class PubSubStreamCreationException(streamId: String, message: String) extends PubSubException(streamId, message)

/**
 * Thrown when CreatePubSubStream command has subscriptions across multiple partitions. "partitionToSubMap" contains
 * enough details for client to figure out and retry.
 */
class CreatePubSubStreamMultiPartitionException(streamId: String, val partitionToSubMap: Map[String, Seq[Int]])
    extends PubSubException(streamId, "CreatePubSubStream across multiple partitions not allowed.")

/**
 * Thrown when the NS stream couldn't be closed on the pubsub broker
 */
class PubSubStreamClosureException(streamId: String, message: String) extends PubSubException(streamId, message)

/**
 * Thrown when the subscription change request failed on the pubsub broker
 */
class PubSubSubscriptionChangeException(val changeRequestId: Int, streamId: String, message: String)
    extends PubSubException(streamId, message)

/**
 * Thrown when ChangeSubscription command has subscriptions across multiple partitions. "partitionToSubMap" contains
 * enough details for client to figure out and retry.
 */
class ChangeSubscriptionMultiPartitionException(
    streamId: String,
    val changeRequestId: Int,
    val partitionToSubMap: Map[String, Seq[Int]])
    extends PubSubException(streamId, "ChangeSubscription across multiple partitions not allowed.")

sealed abstract class MessagesException(message: String, cause: Throwable = null) extends DSIException(message, cause)

/**
 * Thrown when the messages could not publish on the broker
 */
class MessagesPublishException(message: String, cause: Throwable = null) extends MessagesException(message, cause)

class StreamsACLsCommandException(message: String) extends MessagesException(message)

/**
 * Exception thrown when Messages stream command receives some error
 */
sealed abstract class MessagesStreamException(val streamId: String, message: String) extends MessagesException(message)

/**
 * Thrown when the messages stream could not be created on the broker
 */
class MessagesStreamCreationException(streamId: String, message: String)
    extends MessagesStreamException(streamId, message)

/**
 * Thrown when the messages stream could not be closed on the broker
 */
class MessagesStreamClosureException(streamId: String, message: String)
    extends MessagesStreamException(streamId, message)

/**
 * Thrown when the messages subscription change request failed on the broker
 */
class MessagesSubscriptionChangeException(streamId: String, val changeRequestId: Int, message: String)
    extends MessagesStreamException(streamId, message)

/**
 * Thrown when the client receives the onDisconnect callback
 */
class MessagesStreamDisconnectException(message: String) extends MessagesException(message)

/**
 * Exception thrown when client receives a transient from messages broker
 */
class MessagesTransientException(streamId: String, message: String) extends MessagesStreamException(streamId, message)

class ServiceDiscoveryUnsupportedException(message: String) extends RuntimeException(message)

class ServiceDiscoveryFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * Thrown when the client receives the onDisconnect callback
 */
class PubSubStreamDisconnectedException(message: String) extends RuntimeException(message)

/**
 * Thrown when the expression couldn't be validated during entitlement check or similar situations
 */
class UnsupportedDalSubscriptionException(message: String) extends RuntimeException(message)

/**
 * Thrown during entitlement check
 */
class PreCheckEntitlementException(message: String) extends RuntimeException(message)

/**
 * Exception thrown when entity payload is not found.
 */
class EntityPayloadNotFoundException(msg: String) extends Exception(msg)

/**
 * Thrown when a priql query is failed.
 */
class DSIQueryException(msg: String, cause: Throwable) extends DSIException(msg, cause) {
  def this(msg: String) = this(msg, null)
}

/**
 * Broker is unable to do the encryption of protected data
 */
class EncryptionException(msg: String) extends RuntimeException(msg)

final class TemporaryReferenceException(val ref: StorableReference)
    extends RuntimeException(s"Failed to assign final reference from temporary ref ${ref}")

// DAL notification specified exceptions
class RetryableProxySubException(msg: String) extends DSIException(msg)
class NonRetryableProxySubException(msg: String) extends DSIException(msg)

/**
 * A session establishment exception
 */
class SessionEstablishmentException(msg: String, cause: Throwable) extends DSIException(msg, cause) {
  def this(msg: String) = this(msg, null)
  def this() = this(null, null)
}

class ReadAtApproximateNowException(
    cmd: ReadOnlyCommand,
    readTxTime: Instant,
    wallClockTime: Instant,
    tolerance: Duration)
    extends DSIException(
      s"ReadOnlyCommand was rejected by DAL client, reading at ${readTxTime}, too close to wall-clock time ${wallClockTime} with tolerance of ${tolerance}: ${cmd}")

/* TemporalContext exceptions */
class TemporalContextException(msg: String) extends DSIException(msg)

class NoClientTemporalContextException extends TemporalContextException("No client TemporalContext was given.")

class DDCRequestSendingException(msg: String) extends DSIException(msg)

class ProtocolViolationException(msg: String) extends DSIException(msg)
