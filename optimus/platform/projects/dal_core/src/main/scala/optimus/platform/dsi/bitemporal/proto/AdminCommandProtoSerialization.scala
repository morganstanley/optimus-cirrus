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
import optimus.platform.TimeInterval
import optimus.platform.util.Log

import scala.jdk.CollectionConverters._

private[proto] trait AdminCommandProtoSerialization extends QueryProtoSerialization {
  implicit val systemCommandResultSerializer: SystemCommandResultSerializer.type = SystemCommandResultSerializer
  implicit val systemCommandSerializer: SystemCommandSerializer.type = SystemCommandSerializer
  implicit val obliterateSerializer: ObliterateSerializer.type = ObliterateSerializer
}

object SystemCommandResultSerializer
    extends AdminCommandProtoSerialization
    with ProtoSerializer[SystemCommandResult, SystemResultProto] {

  override def deserialize(proto: SystemResultProto): SystemCommandResult = ??? /* {
    new SystemCommandResult(proto.getReply)
  } */

  override def serialize(system: SystemCommandResult): SystemResultProto = ??? /* {
    SystemResultProto.newBuilder
      .setReply(system.reply)
      .build
  } */
}

object SystemCommandSerializer extends AdminCommandProtoSerialization with ProtoSerializer[SystemCommand, SystemProto] {

  override def deserialize(proto: SystemProto): SystemCommand = ??? /* {
    new SystemCommand(proto.getCommand)
  } */

  override def serialize(system: SystemCommand): SystemProto = ??? /* {
    SystemProto.newBuilder
      .setCommand(system.cmd)
      .build
  } */
}

object ObliterateSerializer
    extends AdminCommandProtoSerialization
    with ProtoSerializer[Obliterate, ObliterateProto]
    with Log {

  override def deserialize(proto: ObliterateProto): Obliterate = ??? /* {
    val ttBefore = if (proto.hasTtBefore) Some(fromProto(proto.getTtBefore)) else None

    if (ttBefore.isDefined && ttBefore.get.isBefore(TimeInterval.Infinity))
      throw new UnsupportedOperationException(
        s"Obliteration with a beforeTT time is not supported. beforeTT was set to ${ttBefore.get}")

    if (proto.hasEntityClassQuery)
      new Obliterate(fromProto(proto.getEntityClassQuery))
    else if (proto.getReferenceQueryList().asScala.nonEmpty)
      new Obliterate(proto.getReferenceQueryList().asScala.map { case r: ReferenceQueryProto => fromProto(r) })
    else if (proto.hasSerializedKeyQuery)
      new Obliterate(fromProto(proto.getSerializedKeyQuery))
    else if (proto.hasEventClassQuery)
      new Obliterate(fromProto(proto.getEventClassQuery))
    else if (proto.hasEventReferenceQuery)
      new Obliterate(fromProto(proto.getEventReferenceQuery))
    else if (proto.hasEventKeyQuery)
      new Obliterate(fromProto(proto.getEventKeyQuery))
    else
      throw new UnsupportedOperationException
  } */

  override def serialize(command: Obliterate): ObliterateProto = ??? /* {
    val builder = ObliterateProto.newBuilder
    require(command.queries.nonEmpty, "Serialization of Obliterate with empty queries is not supported")

    command.queries.head match {
      case q: EntityClassQuery => builder.setEntityClassQuery(toProto(q)).build
      case q: ReferenceQuery =>
        builder.addAllReferenceQuery(command.queries.collect { case r: ReferenceQuery => toProto(r) }.asJava).build
      case q: SerializedKeyQuery      => builder.setSerializedKeyQuery(toProto(q)).build
      case q: EventClassQuery         => builder.setEventClassQuery(toProto(q)).build
      case q: EventReferenceQuery     => builder.setEventReferenceQuery(toProto(q)).build
      case q: EventSerializedKeyQuery => builder.setEventKeyQuery(toProto(q)).build
      case o => throw new UnsupportedOperationException(s"Obliteration with query ${o} is not supported")
    }
  } */
}
