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

import optimus.platform.dsi.bitemporal._
import optimus.platform.dsi.bitemporal.proto.Dsi._
import optimus.platform.util.Log
import scala.jdk.CollectionConverters._
import java.time.Instant

private[proto] trait EnumerateCommandProtoSerialization extends GetCommandProtoSerialization {
  implicit val enumerateKeysSerializer: EnumerateKeysSerializer.type = EnumerateKeysSerializer
  implicit val enumerateKeysResultSerializer: EnumerateKeysResultSerializer.type = EnumerateKeysResultSerializer
  implicit val partialEnumerateKeysResultSerializer: PartialEnumerateKeysResultSerializer.type =
    PartialEnumerateKeysResultSerializer
  implicit val enumerateIndicesSerializer: EnumerateIndicesSerializer.type = EnumerateIndicesSerializer
  implicit val enumerateIndicesResultSerializer: EnumerateIndicesResultSerializer.type =
    EnumerateIndicesResultSerializer
  implicit val partialEnumerateIndicesResultSerializer: PartialEnumerateIndicesResultSerializer.type =
    PartialEnumerateIndicesResultSerializer
}

object EnumerateKeysSerializer
    extends EnumerateCommandProtoSerialization
    with ProtoSerializer[EnumerateKeys, EnumerateKeysProto]
    with Log {

  override def deserialize(proto: EnumerateKeysProto): EnumerateKeys = ??? /* {
    // TODO (OPTIMUS-13040): This code is copy-pasted other places :(
    val temporality =
      if (proto.hasValidTime && proto.hasTxTime)
        DSIQueryTemporality.At(fromProto(proto.getValidTime), fromProto(proto.getTxTime))
      else if (proto.hasValidTime) {
        val readTT =
          if (proto.hasReadTxTime)
            fromProto(proto.getReadTxTime)
          else {
            log.error("Deserializing DSIQueryTemporality. ValidTime without readTxTime")
            patch.MilliInstant.now
          }
        DSIQueryTemporality.ValidTime(fromProto(proto.getValidTime), readTT)
      } else if (proto.hasTxTime)
        DSIQueryTemporality.TxTime(fromProto(proto.getTxTime))
      else {
        val readTT =
          if (proto.hasReadTxTime)
            fromProto(proto.getReadTxTime)
          else {
            log.error("Deserializing DSIQueryTemporality. All without readTxTime")
            patch.MilliInstant.now
          }
        DSIQueryTemporality.All(readTT)
      }

    new EnumerateKeys(proto.getTypeName, proto.getPropertyNameList.asScala.toSeq, temporality)
  } */

  override def serialize(req: EnumerateKeys): EnumerateKeysProto = ??? /* {
    import DSIQueryTemporality._

    val bld = EnumerateKeysProto.newBuilder
      .setTypeName(req.typeName)
      .addAllPropertyName(req.propNames.asJava)

    val builder = req.temporality match {
      case All(readTT)           => bld.setReadTxTime(toProto(readTT))
      case TxTime(tx)            => bld.setTxTime(toProto(tx))
      case ValidTime(vt, readTT) => bld.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case At(vt, tx)            => bld.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case TxRange(_) =>
        throw new UnsupportedOperationException("TxRange is not supported for EnumerateKeys serialization.")
      case BitempRange(_, _, _) =>
        throw new UnsupportedOperationException("BitempRange is not supported for EnumerateKeys serialization.")
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException("OpenVtTxRange is not supported for EnumerateKeys serialization.")
    }

    builder.build
  } */
}

object EnumerateKeysResultSerializer
    extends EnumerateCommandProtoSerialization
    with ProtoSerializer[EnumerateKeysResult, EnumerateKeysResultProto]
    with Log {

  override def deserialize(proto: EnumerateKeysResultProto): EnumerateKeysResult = ??? /* {
    new EnumerateKeysResult(proto.getSerializedKeyList.asScala.iterator.map(fromProto(_)).toIndexedSeq)
  } */
  override def serialize(res: EnumerateKeysResult): EnumerateKeysResultProto = ??? /* {
    val builder = EnumerateKeysResultProto.newBuilder
      .addAllSerializedKey((res.keys map { toProto(_) }).asJava)

    builder.build
  } */
}

object EnumerateIndicesSerializer
    extends EnumerateCommandProtoSerialization
    with ProtoSerializer[EnumerateIndices, EnumerateIndicesProto]
    with Log {

  override def deserialize(proto: EnumerateIndicesProto): EnumerateIndices = ??? /* {
    // TODO (OPTIMUS-13040): duplicate from fromProto(proto: EnumerateIndicesProto)
    val temporality =
      if (proto.hasValidTime && proto.hasTxTime)
        DSIQueryTemporality.At(fromProto(proto.getValidTime), fromProto(proto.getTxTime))
      else if (proto.hasValidTime) {
        val readTT =
          if (proto.hasReadTxTime)
            fromProto(proto.getReadTxTime)
          else {
            log.error("Deserializing DSIQueryTemporality. ValidTime without readTxTime")
            patch.MilliInstant.now
          }
        DSIQueryTemporality.ValidTime(fromProto(proto.getValidTime), readTT)
      } else if (proto.hasTxTime)
        DSIQueryTemporality.TxTime(fromProto(proto.getTxTime))
      else {
        val readTT =
          if (proto.hasReadTxTime)
            fromProto(proto.getReadTxTime)
          else {
            log.error("Deserializing DSIQueryTemporality. All without readTxTime")
            patch.MilliInstant.now
          }
        DSIQueryTemporality.All(readTT)
      }

    new EnumerateIndices(proto.getTypeName, proto.getPropertyNameList.asScala.toSeq, temporality)
  } */

  override def serialize(req: EnumerateIndices): EnumerateIndicesProto = ??? /* {
    // TODO (OPTIMUS-13040): duplicate from toProto(req: EnumerateKeys)
    import DSIQueryTemporality._

    val bld = EnumerateIndicesProto.newBuilder
      .setTypeName(req.typeName)
      .addAllPropertyName(req.propNames.asJava)

    val builder = req.temporality match {
      case All(readTT)           => bld.setReadTxTime(toProto(readTT))
      case TxTime(tx)            => bld.setTxTime(toProto(tx))
      case ValidTime(vt, readTT) => bld.setValidTime(toProto(vt)).setReadTxTime(toProto(readTT))
      case At(vt, tx)            => bld.setTxTime(toProto(tx)).setValidTime(toProto(vt))
      case TxRange(_) =>
        throw new UnsupportedOperationException("TxRange is not supported for EnumerateIndices serialization.")
      case BitempRange(_, _, _) =>
        throw new UnsupportedOperationException("BitempRange is not supported for EnumerateIndices serialization.")
      case OpenVtTxRange(_) =>
        throw new UnsupportedOperationException("OpenVtTxRange is not supported for EnumerateIndices serialization.")
    }

    builder.build
  } */
}

object EnumerateIndicesResultSerializer
    extends EnumerateCommandProtoSerialization
    with ProtoSerializer[EnumerateIndicesResult, EnumerateIndicesResultProto] {

  override def deserialize(proto: EnumerateIndicesResultProto): EnumerateIndicesResult = ??? /* {
    // TODO (OPTIMUS-13040): duplicate from fromProto(proto: EnumerateKeysResultProto)
    new EnumerateIndicesResult(proto.getSerializedKeyList.asScala.iterator.map(fromProto(_)).toIndexedSeq)
  } */

  override def serialize(res: EnumerateIndicesResult): EnumerateIndicesResultProto = ??? /* {
    // TODO (OPTIMUS-13040): duplicate from toProto(res: EnumerateKeysResult)
    val builder = EnumerateIndicesResultProto.newBuilder
      .addAllSerializedKey((res.keys map { toProto(_) }).asJava)
    builder.build
  } */
}

object PartialEnumerateIndicesResultSerializer
    extends EnumerateCommandProtoSerialization
    with ProtoSerializer[PartialEnumerateIndicesResult, PartialEnumerateIndicesResultProto] {

  override def deserialize(proto: PartialEnumerateIndicesResultProto): PartialEnumerateIndicesResult = ??? /* {
    val serializedResults = proto.getSerializedKeyList.asScala
    PartialEnumerateIndicesResult(serializedResults map { fromProto(_) } toSeq, proto.getIsLast())
  } */

  override def serialize(result: PartialEnumerateIndicesResult): PartialEnumerateIndicesResultProto = ??? /* {
    PartialEnumerateIndicesResultProto.newBuilder
      .addAllSerializedKey((result.keys map { toProto(_) }).asJava)
      .setIsLast(result.isLast)
      .build
  } */
}

object PartialEnumerateKeysResultSerializer
    extends EnumerateCommandProtoSerialization
    with ProtoSerializer[PartialEnumerateKeysResult, PartialEnumerateKeysResultProto] {

  override def deserialize(proto: PartialEnumerateKeysResultProto): PartialEnumerateKeysResult = ??? /* {
    val serializedResults = proto.getSerializedKeyList.asScala
    PartialEnumerateKeysResult(serializedResults map { fromProto(_) } toSeq, proto.getIsLast())
  } */

  override def serialize(result: PartialEnumerateKeysResult): PartialEnumerateKeysResultProto = ??? /* {
    PartialEnumerateKeysResultProto.newBuilder
      .addAllSerializedKey((result.keys map { toProto(_) }).asJava)
      .setIsLast(result.isLast)
      .build
  } */
}
