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

import com.google.protobuf.ByteString
import optimus.dsi.base.RefHolder
import optimus.platform.dal.prc.NormalizedCacheableQuery
import optimus.platform.dal.prc.NormalizedEntityReferenceQuery
import optimus.platform.dal.prc.NormalizedEventReferenceQuery
import optimus.platform.dal.prc.NormalizedNonCacheableCommand
import optimus.platform.dal.prc.NormalizedPrcKeyComponent
import optimus.platform.dsi.bitemporal.Context
import optimus.platform.dsi.bitemporal.DefaultContextType
import optimus.platform.dsi.bitemporal.UniqueContextType
import optimus.platform.dsi.bitemporal.proto.BusinessEventReferenceSerializer
import optimus.platform.dsi.bitemporal.proto.CommandSerializer
import optimus.platform.dsi.bitemporal.proto.ContextSerializer
import optimus.platform.dsi.bitemporal.proto.Dsi.ContextProto
import optimus.platform.dsi.bitemporal.proto.Prc.NonTemporalPrcKeyProto
import optimus.platform.dsi.bitemporal.proto.Prc.NormalizedCacheableQueryProto
// import optimus.platform.dsi.bitemporal.proto.Prc.NormalizedCacheableQueryProto.QueryType
import optimus.platform.dsi.bitemporal.proto.Prc.NormalizedNonCacheableCommandProto

import optimus.platform.dsi.bitemporal.proto.ProtoSerializer
import optimus.platform.storable.TypedBusinessEventReference

private[optimus] trait NonTemporalPrcKeySerialization {
  final def fromProto(proto: NonTemporalPrcKeyProto): NonTemporalPrcKey = NonTemporalPrcKeySerializer.deserialize(proto)
  final def toProto(value: NonTemporalPrcKey): NonTemporalPrcKeyProto = NonTemporalPrcKeySerializer.serialize(value)
}

private[optimus] object NonTemporalPrcKeySerializer extends ProtoSerializer[NonTemporalPrcKey, NonTemporalPrcKeyProto] {
  override def serialize(value: NonTemporalPrcKey): NonTemporalPrcKeyProto = ??? /* {
    val contextProto: ContextProto = value.context.contextType match {
      case DefaultContextType | UniqueContextType => ContextSerializer.serialize(value.context)
      case _ =>
        throw new IllegalArgumentException(s"Context ${value.context} is not supported.")
    }
    val builder = NonTemporalPrcKeyProto.newBuilder
      .setContext(contextProto)
      .setVersion(
        Option(NonTemporalPrcKeyProto.Version.forNumber(value.version))
          .getOrElse(throw new IllegalArgumentException(s"Version ${value.version} is not supported."))
      )
    value.normalizedKeyComponent match {
      case query: NormalizedCacheableQuery => builder.setQuery(NormalizedCacheableQuerySerialzer.serialize(query))
      case c: NormalizedNonCacheableCommand =>
        builder.setNonCacheableCommand(
          NormalizedNonCacheableCommandProto.newBuilder().setCommand(CommandSerializer.serialize(c.command)).build())
    }
    builder.build()
  } */

  override def deserialize(proto: NonTemporalPrcKeyProto): NonTemporalPrcKey = ??? /* {
    require(
      proto.hasQuery ^ proto.hasNonCacheableCommand,
      "The nontemporal prc key must contain either a cacheable or a noncacheable component")
    val normalizedKeyComponent: NormalizedPrcKeyComponent =
      if (proto.hasQuery) NormalizedCacheableQuerySerialzer.deserialize(proto.getQuery)
      else NormalizedNonCacheableCommand(CommandSerializer.deserialize(proto.getNonCacheableCommand.getCommand))

    val context: Context = ContextSerializer.deserialize(proto.getContext)

    val version = proto.getVersion match {
      case NonTemporalPrcKeyProto.Version.SIMPLE_PROTO | NonTemporalPrcKeyProto.Version.NORMALIZED_QUERY_PROTO =>
        proto.getVersion.getNumber
      case NonTemporalPrcKeyProto.Version.UNKNOWN =>
        throw new IllegalArgumentException("unknown PRC key version")
    }

    NonTemporalPrcKey(normalizedKeyComponent, context, version)
  } */
}

object NormalizedCacheableQuerySerialzer
    extends ProtoSerializer[NormalizedCacheableQuery, NormalizedCacheableQueryProto] {
  override def deserialize(proto: NormalizedCacheableQueryProto): NormalizedCacheableQuery = ??? /* {
    proto.getQueryType match {
      case NormalizedCacheableQueryProto.QueryType.ENTITY_REFERENCE =>
        NormalizedEntityReferenceQuery(new RefHolder(proto.getEntityReference.toByteArray))
      case NormalizedCacheableQueryProto.QueryType.EVENT_REFERENCE =>
        val bevref = BusinessEventReferenceSerializer.deserialize(proto.getEventReference)
        bevref match {
          case t: TypedBusinessEventReference => NormalizedEventReferenceQuery(t)
          case o =>
            throw new IllegalArgumentException(s"Unsupported untyped business event reference: $o")
        }
      case NormalizedCacheableQueryProto.QueryType.UNKNOWN =>
        throw new IllegalArgumentException("Cannot deserialize unknown query type")
    }
  } */

  override def serialize(query: NormalizedCacheableQuery): NormalizedCacheableQueryProto = ??? /* {
    val builder = NormalizedCacheableQueryProto.newBuilder
    query match {
      case NormalizedEntityReferenceQuery(entityRefBytes) =>
        builder
          .setQueryType(QueryType.ENTITY_REFERENCE)
          .setEntityReference(ByteString.copyFrom(entityRefBytes.data))
      case NormalizedEventReferenceQuery(ref) =>
        builder
          .setQueryType(QueryType.EVENT_REFERENCE)
          .setEventReference(BusinessEventReferenceSerializer.serialize(ref))
    }
    builder.build()
  } */
}
