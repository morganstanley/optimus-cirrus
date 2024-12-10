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
package optimus.platform.dsi.prc.cache

import optimus.platform.dsi.bitemporal.ReadOnlyCommand
import optimus.platform.dsi.bitemporal.ResolvedClientAppIdentifier
import optimus.platform.dsi.bitemporal.proto.ClientAppIdentifierSerializer
import optimus.platform.dsi.bitemporal.proto.CommandProtoSerialization
import optimus.platform.dsi.bitemporal.proto.CommandSerializer
import optimus.platform.dsi.bitemporal.proto.Prc.PrcUserOptionsProto
import optimus.platform.dsi.bitemporal.proto.ProtoSerializer
import optimus.platform.dsi.prc.session.PrcClientSessionSerializer

import scala.jdk.CollectionConverters._

private[optimus] object PrcUserOptsSerializer
    extends CommandProtoSerialization
    with ProtoSerializer[PrcUserOptions, PrcUserOptionsProto] {
  override def serialize(value: PrcUserOptions): PrcUserOptionsProto = ??? /* value match {
    case options: CommandsPrcUserOptions =>
      val cmdType = options match {
        case _: ClientCommandsPrcUserOptions   => PrcUserOptionsProto.UserOptionsType.COMMANDS
        case _: DualReadCommandsPrcUserOptions => PrcUserOptionsProto.UserOptionsType.DUAL_READ_COMMANDS
      }

      val (keys, cmds) = options.prcKeyUserOptsList
        .map(u => NonTemporalPrcKeySerializer.serialize(u.key) -> CommandSerializer.serialize(u.command))
        .unzip
      val builder = PrcUserOptionsProto
        .newBuilder()
        .setUserOptionsType(cmdType)
        .addAllKey(keys.asJava)
        .addAllCommand(cmds.asJava)
        .setPrcClientSessionInfo(PrcClientSessionSerializer.serialize(options.prcClientSessionInfo))
        .setClientAppIdentifier(ClientAppIdentifierSerializer.serialize(options.resolvedClientAppIdentifier.underlying))
      options.establishCommand.foreach(e => builder.setEstablishSession(toProto(e)))
      builder.build()
    case RawValuePrcUserOptions =>
      PrcUserOptionsProto.newBuilder().setUserOptionsType(PrcUserOptionsProto.UserOptionsType.RAW_VALUE).build()
    case lsqt: LsqtQueryPrcUserOptions =>
      val key = NonTemporalPrcKeySerializer.serialize(lsqt.key)
      PrcUserOptionsProto
        .newBuilder()
        .setUserOptionsType(PrcUserOptionsProto.UserOptionsType.LSQT)
        .addKey(key)
        .build()
  } */

  /* private def mkCommandsPrcUserOptions(typ: PrcUserOptionsProto.UserOptionsType, proto: PrcUserOptionsProto) = {
    require(proto.getCommandCount == proto.getKeyCount, "Each key option should be mapped to a command.")
    val keys = proto.getKeyList.asScala.map(NonTemporalPrcKeySerializer.deserialize)
    val cmds = proto.getCommandList.asScala.map(CommandSerializer.deserialize)
    val prcKeyOpts = keys zip cmds map { case (k, c) =>
      require(c.isInstanceOf[ReadOnlyCommand], s"expected read only command in PRC request, but got $c")
      NonTemporalPrcKeyUserOpts(k, c.asInstanceOf[ReadOnlyCommand])
    }
    val clientInfo = PrcClientSessionSerializer.deserialize(proto.getPrcClientSessionInfo)
    val establishOpt = if (proto.hasEstablishSession) Some(fromProto(proto.getEstablishSession)) else None
    val cai = ResolvedClientAppIdentifier(ClientAppIdentifierSerializer.deserialize(proto.getClientAppIdentifier))

    typ match {
      case PrcUserOptionsProto.UserOptionsType.DUAL_READ_COMMANDS =>
        DualReadCommandsPrcUserOptions(
          prcKeyOpts,
          clientInfo,
          establishOpt,
          cai
        )
      case PrcUserOptionsProto.UserOptionsType.COMMANDS | PrcUserOptionsProto.UserOptionsType.UNKNOWN =>
        ClientCommandsPrcUserOptions(
          prcKeyOpts,
          clientInfo,
          establishOpt,
          cai
        )
      case _ =>
        throw new IllegalArgumentException(s"Shouldn't be building CommandsPrcUserOptions for type $typ ")
    }
  } */

  override def deserialize(proto: PrcUserOptionsProto): PrcUserOptions = ??? /* proto.getUserOptionsType match {
    // for backwards-compatibility, deserialize the default value (UNKNOWN) as COMMANDS since clients did not
    // initially serialize the user options type at all
    case PrcUserOptionsProto.UserOptionsType.DUAL_READ_COMMANDS | PrcUserOptionsProto.UserOptionsType.COMMANDS |
        PrcUserOptionsProto.UserOptionsType.UNKNOWN =>
      mkCommandsPrcUserOptions(proto.getUserOptionsType, proto)
    case PrcUserOptionsProto.UserOptionsType.RAW_VALUE =>
      RawValuePrcUserOptions
    case PrcUserOptionsProto.UserOptionsType.LSQT =>
      require(proto.getKeyList.size() == 1, "Should have exactly one command keys for an LSQT user option")
      val key = proto.getKeyList.asScala.map(NonTemporalPrcKeySerializer.deserialize).head
      LsqtQueryPrcUserOptions(key.context)
  } */
}
