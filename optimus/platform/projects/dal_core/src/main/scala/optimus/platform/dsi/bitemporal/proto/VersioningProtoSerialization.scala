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
package optimus.platform.dsi.bitemporal.proto

import com.google.protobuf.ByteString
import msjava.slf4jutils.scalalog._
import optimus.platform.ImmutableArray
import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.dsi.versioning._
import optimus.platform.storable.SerializedEntity
import scala.jdk.CollectionConverters._

trait VersioningProtoSerialization extends BasicProtoSerialization {
  implicit val slotRedirectionInfoSerializer: SlotRedirectionInfoSerializer.type = SlotRedirectionInfoSerializer
  implicit val versioningRedirectionInfoSerializer: VersioningRedirectionInfoSerializer.type =
    VersioningRedirectionInfoSerializer
  implicit val versioningResultSerializer: VersioningResultSerializer.type = VersioningResultSerializer
}

object SlotRedirectionInfoSerializer
    extends WriteCommandProtoSerialization
    with ProtoSerializer[SlotRedirectionInfo, SlotRedirectionInfoProto] {
  /* private def protoToPathElement(proto: PathElementProto): (PathHashT, Int, Int) = {
    val pathHash = PathHashRepr(ImmutableArray.wrapped(proto.getClasspathHash.toByteArray))
    (pathHash, proto.getSourceSlotNumber, proto.getDestinationSlotNumber)
  }

  private def pathElementToProto(pathHash: PathHashT, sourceSlotNumber: Int, destSlotNumber: Int): PathElementProto = {
    val builder = PathElementProto.newBuilder
    builder.setSourceSlotNumber(sourceSlotNumber)
    builder.setDestinationSlotNumber(destSlotNumber)
    builder.setClasspathHash(ByteString.copyFrom(pathHash.hash.rawArray))
    builder.build()
  } */
  override def deserialize(proto: SlotRedirectionInfoProto): SlotRedirectionInfo = ??? /* {
    require(proto.getVersioningPathCount > 0, "Cannot deserialize SlotRedirectionInfoProto with empty path")
    val className = proto.getSourceClassName

    val path = proto.getVersioningPathList.asScala.map(protoToPathElement(_))

    val (lastHash, lastSource, destSlotKeys) = {
      val (hash, source, target) = path.last
      val allTargets =
        proto.getAdditionalDestinationSlotNumbersList.asScala.map(_.intValue).toSet + target
      (hash, SlotMetadata.Key(className, source), allTargets.map(SlotMetadata.Key(className, _)))
    }

    val intermediates = path.tail.dropRight(1).toSeq.map { case (hash, source, target) =>
      (hash, SlotMetadata.Key(className, source), SlotMetadata.Key(className, target))
    }

    SlotRedirectionInfo(className, (lastHash, lastSource, destSlotKeys), intermediates)
  } */

  override def serialize(sri: SlotRedirectionInfo): SlotRedirectionInfoProto = ??? /* {
    val builder = SlotRedirectionInfoProto.newBuilder
    builder.setSourceClassName(sri.sourceSlot.className)
    val pathBuilder = Seq.newBuilder[PathElementProto]
    sri.intermediateSlots.foreach { case (ph, ssk, dsk) =>
      pathBuilder += pathElementToProto(ph, ssk.slotNumber, dsk.slotNumber)
    }
    val destinationHash = sri.destinationSlots._1
    val destinationSlotNumbers = sri.destinationSlots._3.map(_.slotNumber).toSeq.sorted
    require(destinationSlotNumbers.nonEmpty, "Cannot serialize SlotRedirectionInfo with empty destination")
    pathBuilder += pathElementToProto(destinationHash, sri.sourceSlot.slotNumber, destinationSlotNumbers.head)
    val path = pathBuilder.result()
    builder.addAllVersioningPath(path.asJava)
    builder.addAllAdditionalDestinationSlotNumbers((destinationSlotNumbers.tail.map(Integer.valueOf(_))).asJava)
    builder.build()
  } */
}

object VersioningRedirectionInfoSerializer
    extends VersioningProtoSerialization
    with ProtoSerializer[VersioningRedirectionInfo, VersioningRedirectionInfoProto] {
  override def deserialize(proto: VersioningRedirectionInfoProto): VersioningRedirectionInfo = ??? /* {
    val slotRedirections = proto.getSlotRedirectionInfosList.asScala.map(fromProto(_))
    val sriMap: Map[SerializedEntity.TypeRef, Set[SlotRedirectionInfo]] = slotRedirections.toSet.groupBy(_.className)
    VersioningRedirectionInfo(sriMap)
  } */

  override def serialize(cmd: VersioningRedirectionInfo): VersioningRedirectionInfoProto = ??? /* {
    val builder = VersioningRedirectionInfoProto.newBuilder
    val sriProtos = cmd.slotRedirections.values.flatMap(_.map(toProto(_)))
    builder.addAllSlotRedirectionInfos(sriProtos.toSeq.asJava)
    builder.build()
  } */
}

object VersioningResultSerializer
    extends CommandProtoSerializationBase
    with ProtoSerializer[VersioningResult, VersioningResultProto]
    with VersioningProtoSerialization {

  private[this] val log = getLogger(this)

  override def serialize(result: VersioningResult): VersioningResultProto = ??? /* {
    result match {
      case v: VersioningValidResult =>
        val validResult =
          VersioningValidResultProto.newBuilder.addAllVersionedCommands(v.versionedCommands.map(toProto(_)).asJava)
        VersioningResultProto.newBuilder.setValidResult(validResult).build

      case e: VersioningErrorResult =>
        val errorResult = e.error match {
          case ex: NoWriteShapeException =>
            VersioningErrorResultProto.newBuilder
              .setType(VersioningErrorResultProto.Type.NO_WRITE_SHAPE)
              .setClassName(ex.className)
              .setTargetSlot(ex.targetSlot)
          case ex: MissingVersioningRequestException =>
            VersioningErrorResultProto.newBuilder
              .setType(VersioningErrorResultProto.Type.MISSING_REQUEST)
          case ex: InvalidVersioningRequestException =>
            VersioningErrorResultProto.newBuilder
              .setType(VersioningErrorResultProto.Type.UNSUPPORTED_REQUEST)
          case ex: UnversionableCommandException =>
            VersioningErrorResultProto.newBuilder
              .setType(VersioningErrorResultProto.Type.UNVERSIONABLE_WRITE_CMD)
              .setCommand(toProto(ex.command))
          case ex: Throwable =>
            log.error("An internal error occurred during versioning result serialization", ex)
            VersioningErrorResultProto.newBuilder
              .setType(VersioningErrorResultProto.Type.INTERNAL)
        }
        errorResult.setMessage(e.error.getMessage)
        VersioningResultProto.newBuilder.setErrorResult(errorResult).build

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported result in VersioningResultProto serialization: ${result}.")
    }
  } */

  override def deserialize(proto: VersioningResultProto): VersioningResult = ??? /* {
    if (proto.hasValidResult) {
      VersioningValidResult(proto.getValidResult.getVersionedCommandsList.asScala.map(fromProto(_)))
    } else if (proto.hasErrorResult) {
      val errorResult = proto.getErrorResult
      val throwable = errorResult.getType match {
        case VersioningErrorResultProto.Type.NO_WRITE_SHAPE =>
          new NoWriteShapeException(errorResult.getClassName, errorResult.getTargetSlot)
        case VersioningErrorResultProto.Type.MISSING_REQUEST     => new MissingVersioningRequestException
        case VersioningErrorResultProto.Type.UNSUPPORTED_REQUEST => new InvalidVersioningRequestException
        case VersioningErrorResultProto.Type.UNVERSIONABLE_WRITE_CMD =>
          new UnversionableCommandException(fromProto(errorResult.getCommand))
        case _ => new DSISpecificError(errorResult.getMessage)
      }
      VersioningErrorResult(throwable)
    } else
      throw new UnsupportedOperationException(
        s"Unsupported proto result in VersioningResult deserialization: ${proto}.")
  } */
}
