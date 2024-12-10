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
import optimus.platform.storable.Entity

import scala.jdk.CollectionConverters._

private[optimus /*platform*/ ] trait QueryProtoSerialization extends BasicProtoSerialization {
  implicit val referenceQuerySerializer: ReferenceQuerySerializer.type = ReferenceQuerySerializer
  implicit val eventReferenceQuerySerializer: EventReferenceQuerySerializer.type = EventReferenceQuerySerializer
  implicit val serializedKeyQuerySerializer: SerializedKeyQuerySerializer.type = SerializedKeyQuerySerializer
  implicit val eventSerializedKeyQuerySerializer: EventSerializedKeyQuerySerializer.type =
    EventSerializedKeyQuerySerializer
  implicit val linkageQuerySerializer: LinkageQuerySerializer.type = LinkageQuerySerializer
  implicit val entityClassQuerySerializer: EntityClassQuerySerializer.type = EntityClassQuerySerializer
  implicit val eventClassQuerySerializer: EventClassQuerySerializer.type = EventClassQuerySerializer
  implicit val entityCmReferenceQuerySerializer: EntityCmReferenceQuerySerializer.type =
    EntityCmReferenceQuerySerializer
  implicit val eventCmReferenceQuerySerializer: EventCmReferenceQuerySerializer.type = EventCmReferenceQuerySerializer
}

object ReferenceQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[ReferenceQuery, ReferenceQueryProto] {

  override def serialize(query: ReferenceQuery): ReferenceQueryProto = ??? /* {
    val builder = ReferenceQueryProto.newBuilder()
    builder.setEntityReference(toProto(query.ref))
    builder.setEntitledOnly(query.entitledOnly)
    query.className.foreach(cn => builder.setClassName(cn))
    builder.build()
  } */

  override def deserialize(proto: ReferenceQueryProto): ReferenceQuery = ??? /* {
    val entitledOnly = if (proto.hasEntitledOnly) proto.getEntitledOnly else false
    val classNameOpt = if (proto.hasClassName) Option(proto.getClassName) else None
    ReferenceQuery(fromProto(proto.getEntityReference), entitledOnly, classNameOpt)
  } */
}

object EventReferenceQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[EventReferenceQuery, EventReferenceQueryProto] {

  override def serialize(query: EventReferenceQuery): EventReferenceQueryProto = ??? /* {
    EventReferenceQueryProto.newBuilder
      .setEventReference(toProto(query.ref))
      .build()
  } */

  override def deserialize(proto: EventReferenceQueryProto): EventReferenceQuery = ??? /* {
    EventReferenceQuery(fromProto(proto.getEventReference))
  } */
}

object SerializedKeyQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[SerializedKeyQuery, SerializedKeyQueryProto] {

  override def serialize(query: SerializedKeyQuery): SerializedKeyQueryProto = ??? /* {
    SerializedKeyQueryProto.newBuilder
      .setSerializedKey(toProto(query.key))
      .addAllRefs((query.refs.map(toProto(_))).asJava)
      .setEntitledOnly(query.entitledOnly)
      .build
  } */

  override def deserialize(proto: SerializedKeyQueryProto): SerializedKeyQuery = ??? /* {
    val refs = proto.getRefsList().asScala.map(fromProto(_)).toSet
    val entitledOnly = proto.getEntitledOnly
    SerializedKeyQuery(fromProto(proto.getSerializedKey), refs, entitledOnly)
  } */
}

object EventSerializedKeyQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[EventSerializedKeyQuery, EventSerializedKeyQueryProto] {

  override def serialize(query: EventSerializedKeyQuery): EventSerializedKeyQueryProto = ??? /* {
    EventSerializedKeyQueryProto.newBuilder
      .setSerializedKey(toProto(query.key))
      .setEntitledOnly(query.entitledOnly)
      .build
  } */

  override def deserialize(proto: EventSerializedKeyQueryProto): EventSerializedKeyQuery = ???
}

object LinkageQuerySerializer extends QueryProtoSerialization with ProtoSerializer[LinkageQuery, LinkageQueryProto] {

  override def serialize(query: LinkageQuery): LinkageQueryProto = ??? /* {
    LinkageQueryProto.newBuilder
      .setParentClassName(query.parentTypeStr)
      .setParentPropertyName(query.parentPropertyName)
      .setChildEntityReference(toProto(query.childEntityRef))
      .build
  } */

  override def deserialize(proto: LinkageQueryProto): LinkageQuery = ??? /* {
    LinkageQuery(
      parentTypeStr = proto.getParentClassName,
      parentPropertyName = proto.getParentPropertyName,
      childEntityRef = fromProto(proto.getChildEntityReference))
  } */
}

object EntityClassQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[EntityClassQuery, EntityClassQueryProto] {

  override def serialize(query: EntityClassQuery): EntityClassQueryProto = ??? /* {
    EntityClassQueryProto.newBuilder
      .setClassName(query.classNameStr)
      .setEntitledOnly(query.entitledOnly)
      .build
  } */

  override def deserialize(proto: EntityClassQueryProto): EntityClassQuery = ??? /* {
    val entitledOnly = if (proto.hasEntitledOnly) proto.getEntitledOnly else false
    EntityClassQuery(proto.getClassName, entitledOnly)
  } */
}

object EventClassQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[EventClassQuery, EventClassQueryProto] {

  override def serialize(query: EventClassQuery): EventClassQueryProto = ??? /* {
    EventClassQueryProto.newBuilder
      .setClassName(query.classNameStr)
      .setEntitledOnly(query.entitledOnly)
      .build
  } */

  override def deserialize(proto: EventClassQueryProto): EventClassQuery = ??? /* {
    val entitledOnly = if (proto.hasEntitledOnly) proto.getEntitledOnly else false
    EventClassQuery(proto.getClassName, entitledOnly)
  } */
}

object EntityCmReferenceQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[EntityCmReferenceQuery, EntityCmReferenceQueryProto] {
  override def serialize(query: EntityCmReferenceQuery): EntityCmReferenceQueryProto = ??? /* {
    val builder = EntityCmReferenceQueryProto.newBuilder()
    builder.setCmRef(toProto(query.cmid))
    builder.setClassName(query.clazzName)
    builder.build
  } */

  override def deserialize(proto: EntityCmReferenceQueryProto): EntityCmReferenceQuery = ??? /* {
    val className =
      if (proto.hasClassName) proto.getClassName
      else {
        // NB we hardcode this as an indication that the client did not send a className. We don't believe this ever
        // happens but it gives us a way to make the classname mandatory at the API level and maintain backwards
        // compatibility from a time when it did not used to be mandatory at the API level and therefore may not be set
        classOf[Entity].getName
      }
    EntityCmReferenceQuery(fromProto(proto.getCmRef), className)
  } */
}

object EventCmReferenceQuerySerializer
    extends QueryProtoSerialization
    with ProtoSerializer[EventCmReferenceQuery, EventCmReferenceQueryProto] {
  override def serialize(query: EventCmReferenceQuery): EventCmReferenceQueryProto = ??? /* {
    val builder = EventCmReferenceQueryProto.newBuilder()
    builder.setCmRef(toProto(query.cmid))
    builder.setClassName(query.clazzName)
    builder.build
  } */

  override def deserialize(proto: EventCmReferenceQueryProto): EventCmReferenceQuery = ??? /* {
    val className =
      if (proto.hasClassName) proto.getClassName
      else {
        // NB we hardcode this as an indication that the client did not send a className. We don't believe this ever
        // happens but it gives us a way to make the classname mandatory at the API level and maintain backwards
        // compatibility from a time when it did not used to be mandatory at the API level and therefore may not be set
        classOf[Entity].getName
      }
    EventCmReferenceQuery(fromProto(proto.getCmRef), className)
  } */
}
