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
package optimus.dsi.serialization.json

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import optimus.dsi.base.RefHolder
import optimus.dsi.serialization.ReferenceTransformer
import optimus.dsi.serialization.TypedReferenceAwareTransformer
import optimus.dsi.serialization.json.JacksonObjectMapper.readJsonMap
import optimus.entity.EntityLinkageProperty
import optimus.platform.dsi.bitemporal.BrokerDeserializationException
import optimus.platform.dsi.bitemporal.DateTimeSerialization
import optimus.platform.pickling.ImmutableByteArray
import optimus.platform.storable.CmReference
import optimus.platform.storable.EntityReference
import optimus.platform.storable.FinalReference
import optimus.platform.storable.SerializedEntity
import optimus.platform.storable.SerializedEntity.EntityLinkage
import optimus.platform.storable.SerializedKey
import optimus.platform.storable.StorableReference
import optimus.platform.storable.TemporaryReference

import java.time.Instant
import scala.collection.immutable
import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

trait JsonSerializationBase {
  import DateTimeSerialization._

  import scala.reflect.runtime.universe._
  def throwMissingKeyException(map: Map[String, Any], key: String) =
    throw new BrokerDeserializationException(s"Cannot get ${key} in Json map ${map}")

  def throwBadTypeException(
      map: Map[String, Any],
      key: String,
      typ: String,
      rootCause: Option[Throwable] = None
  ) = {
    val badType = new BrokerDeserializationException(s"$key in Json map $map is not of type $typ")
    rootCause.foreach(badType.addSuppressed)
    throw badType
  }

  protected def get[T](map: Map[String, Any], key: String): T = {
    try {
      map.getOrElse(key, throwMissingKeyException(map, key)).asInstanceOf[T]
    } catch {
      case NonFatal(e) => throwBadTypeException(map, key, weakTypeTag[T].toString, Some(e))
    }
  }
  protected def getOption[T](map: Map[String, Any], key: String): Option[T] = {
    try {
      map.get(key).asInstanceOf[Option[T]]
    } catch {
      case NonFatal(e) => throwBadTypeException(map, key, s"Option[${weakTypeTag[T]}]", Some(e))
    }
  }

  protected def getTypedOrElse[T](map: Map[String, Any], key: String, els: T): T = {
    try {
      map
        .get(key)
        .map(TypedRefAwareJsonTypeTransformer.deserialize(_).asInstanceOf[T])
        .getOrElse(els)
    } catch {
      case NonFatal(e) => throwBadTypeException(map, key, weakTypeTag[T].toString, Some(e))
    }
  }

  protected def getOrElse[T](map: Map[String, Any], key: String, els: T): T = {
    try {
      map.getOrElse(key, els).asInstanceOf[T]
    } catch {
      case NonFatal(e) => throwBadTypeException(map, key, weakTypeTag[T].toString, Some(e))
    }
  }
  protected def getIntOrLongAsLong(map: Map[String, Any], key: String): Long = {
    val f = map.getOrElse(key, throwMissingKeyException(map, key))
    f match {
      case x: Int  => x.toLong
      case x: Long => x
      case o       => throwBadTypeException(map, key, s"Int or Long, but got $o")
    }
  }
  protected def getIntOrLongAsLongMaybe(map: Map[String, Any], key: String): Option[Long] =
    if (map.contains(key)) Some(getIntOrLongAsLong(map, key))
    else None

  protected def getBytes(map: Map[String, Any], key: String): Array[Byte] = {
    map.getOrElse(key, throwMissingKeyException(map, key)) match {
      case x: Array[Byte]        => x
      case x: ImmutableByteArray => x.data
      case o                     => throwBadTypeException(map, key, s"Array[Byte] or ImmutableByteArray, but got $o")
    }
  }
  protected def getInstant(map: Map[String, Any], key: String): Instant = {
    toInstant(getIntOrLongAsLong(map, key))
  }
  protected def getInstantMaybe(map: Map[String, Any], key: String): Option[Instant] = {
    if (map.contains(key))
      Some(getInstant(map, key))
    else
      None
  }
  protected def putInstant(map: mutable.Map[String, Any], key: String, t: Instant) = {
    map += key -> fromInstant(t)
  }
  protected def refHolder(map: Map[String, Any], key: String): RefHolder = {
    RefHolder(StorableReference.fromString(get[String](map, key)).data)
  }
  object Labels {
    val Props = "props"
    val TTFrom = "txFrom"
    val TTTo = "txTo"
    val VTFrom = "vtFrom"
    val VTTo = "vtTo"
    val TTFshort = "t1"
    val TTTshort = "t2"
    val VTFshort = "v1"
    val VTTshort = "v2"
    val VRef = "vr"
    val Ref = "er"
    val BRef = "ber"
    val Refs = "ers"
    val KeyHash = "kh"
    val LockToken = "lt"
    val TimeSliceCount = "tsc"
    val TimeSliceNumber = "n"
    val UpdateAppEvent = "uae"
    val VersionCount = "vc"
    val VersionNumber = "vn"
    val BVRef = "bevr"
    val VT = "vt"
    val TT = "tt"
    val Id = "_id"
    val Indexed = "indexed"
    val Space = "space"
    val In = "$in"
    val Exists = "$exists"
    val Nin = "$nin"
    val Set = "$set"
    val Unset = "$unset"
    val AddToSet = "$addToSet"
    val Keys = "keys"
    val Key = "key"
    val ClassName = "cn"
    val Types = "tps"
    val TypesUpdated = "tu"
    val CmId = "cmid"
    val LinkedTypes = "lts"
    val LinkedTypesUpdated = "ltu"
    val KeyDefType = "kt"
    val IndexDefTypes = "it"
    val UniqueIndexDefTypes = "ut"
    val LinkageDefTypes = "lt" // this is in the scope of linkedTypes doc

    val ClassNameId = "cnid"
    val TypesId = "tid"
    val RefTypeId = "reftid"
    val KeyTypeId = "kt"
    val Version = "v"

    val Type = "type"
    val Data = "data"
    val Hash = "hash"

    val ParentRefLabel = "pr"
    val ParentPropNameLabel = "pn"
    val ParentTypeLabel = "pt"
    val ChildTypeLabel = "ct"
    val ChildTypeLabelId = "ctid"
    val ParentTypeLabelId = "ptid"
    val ParentRefClassIdLabel = "prcid"
    val ChildRefLabel = "cr"

    val Versions = "vs"
    val MigratedToNewStructure = "mg"

    val EvtId = "ei"
    val EvtVid = "ev"
    val EvtType = "evt"
    val EvtTypeId = "evtid"
    val CreateAppEvent = "ca"

    val Cancelled = "c"
    val AppEventId = "aid"

    val Tombstone = "tmb"
    val KeyOps = "k"
    val KeyOpsExtension = "kx"
    val TypesKeys = "tks"
    val TypesValues = "tvs"

    val ValidTime = "vt"
    val VersionId = "vid"
    val Slot = "s"
    val Inlined = "il"

    val HasLinkages = "hl"
    val EntityLinkages = "links"
    val EntityLinkagePropertyType = "elpt"
    val EntityLinkageTemporaryRefs = "elt"
    val EntityLinkageFinalRefs = "elf"

    val Unique = "u"
    val RefFilter = "refFilter"
    val IsInfiniteTxtoAtCreation = "iittac"
    val SlotNumber = "slot"
    val Properties = "properties"
    val RefTypeIds = "refTypeIds"

    val fcIndex = "forceCreateIndex"

    val streamId = "streamId"
    val offset = "offset"

    // this is to capture information in an entity timeslice if it is created by an advance operation e.g. revert
    val AdvanceOp = "ao"
  }
}

trait JsonSerialization[T] extends JsonSerializationBase {
  def toJson(p: T): String = JacksonObjectMapper.mapper.writeValueAsString(toJsonMap(p))
  def toJsonBytes(p: T): Array[Byte] = JacksonObjectMapper.mapper.writeValueAsBytes(toJsonMap(p))
  def toJsonPrettyPrint(p: T): String =
    JacksonObjectMapper.mapper.writerWithDefaultPrettyPrinter.writeValueAsString(toJsonMap(p))
  def toJsonNode(p: T): JsonNode = {
    JacksonObjectMapper.mapper.valueToTree(p)
  }
  def fromJson(json: String): T = fromJsonMap(readJsonMap(json))
  def fromJsonBytes(bytes: Array[Byte]): T = fromJsonMap(
    JacksonObjectMapper.mapper.readValue(bytes, classOf[Map[String, Any]]))
  def fromJsonNode[V](json: JsonNode, v: Class[V]): V = JacksonObjectMapper.mapper.treeToValue(json, v)

  def toJsonMap(p: T): Map[String, Any]
  def fromJsonMap(map: Map[String, Any]): T
}

abstract class SerializedKeySerializationBase extends JsonSerialization[SerializedKey] {
  def keyToJsonString(skey: SerializedKey): String = {
    JacksonObjectMapper.mapper.writeValueAsString(toJsonMap(skey))
  }
  def jsonStringToKey(s: String): SerializedKey = {
    fromJsonMap(JacksonObjectMapper.mapper.readValue(s, classOf[Map[String, Any]]))
  }
}

class SerializedEntitySerialization(
    serializedKeySerialization: SerializedKeySerializationBase,
    excludeNonUniqueKeys: Boolean,
    verboseEntityRef: Boolean,
    includeLinkages: Boolean
) extends JsonSerialization[SerializedEntity] {

  def toJsonMap(p: SerializedEntity): Map[String, Any] = {
    val keys = p.keys
    val inlined = if (p.inlinedEntities != null) p.inlinedEntities.map(toJson) else Nil
    val filteredKeys = if (excludeNonUniqueKeys) keys.filter(_.unique) else keys
    val serKeys =
      if (filteredKeys.nonEmpty) filteredKeys.map(k => serializedKeySerialization.keyToJsonString(k))
      else Nil
    val m = immutable.ListMap.newBuilder[String, Any]
    val eref =
      if (verboseEntityRef) TypedRefAwareJsonTypeTransformer.serialize(p.entityRef)
      else p.entityRef.toString
    m += (Labels.Ref -> eref)

    p.cmid.foreach(cmid => m += Labels.CmId -> cmid)
    val linkages = if (includeLinkages) p.linkages.map { links =>
      val m = immutable.ListMap.newBuilder[String, Any]
      links.foreach { case (property, entities) =>
        val (temps, finals) = entities.map(_.link).partition(_.isTemporary)
        m += (property.propertyName -> Map(
          Labels.EntityLinkagePropertyType -> property.typeName,
          Labels.EntityLinkageTemporaryRefs -> temps.map(_.toString),
          Labels.EntityLinkageFinalRefs -> finals.map(f => (f.toString, f.getTypeId)).toMap
        ))
      }
      m.result()
    }
    else None
    m ++= List(
      Labels.ClassName -> p.className,
      Labels.Keys -> serKeys,
      Labels.Data -> TypedRefAwareJsonTypeTransformer.serialize(p.properties),
      Labels.Types -> TypedRefAwareJsonTypeTransformer.serialize(p.types),
      Labels.Inlined -> inlined,
      Labels.HasLinkages -> p.linkages.isDefined,
      Labels.Slot -> p.slot
    )
    if (includeLinkages) m += Labels.EntityLinkages -> TypedRefAwareJsonTypeTransformer.serialize(linkages)
    m.result()
  }

  def fromJsonMap(map: Map[String, Any]): SerializedEntity = {
    val keysSeq = getOrElse[Seq[String]](map, Labels.Keys, Seq.empty)
    val keys = keysSeq.map(serializedKeySerialization.jsonStringToKey)
    val types = getTypedOrElse[Seq[String]](map, Labels.Types, Seq.empty)
    val inlinedStrings = getOrElse[Seq[String]](map, Labels.Inlined, Nil)
    val inlined = inlinedStrings.map(fromJson)
    val props = getTypedOrElse[Map[String, Any]](map, Labels.Data, Map.empty[String, Any])
    val encodedEref: Any = get[Any](map, Labels.Ref)
    val eref: EntityReference = encodedEref match {
      case e: EntityReference => e
      case s: String          => EntityReference.fromString(s)
      case o =>
        val encoded = TypedRefAwareJsonTypeTransformer.deserialize(o)
        encoded match {
          case er: EntityReference => er
          case _                   => throw new IllegalArgumentException(s"Couldn't resolve EntityReference from $o")
        }
    }
    val className = get[String](map, Labels.ClassName)
    val hasLinkages = getOrElse[Boolean](map, Labels.HasLinkages, false)
    val linkages: Option[SerializedEntity.LinkageMap] =
      if (hasLinkages) {
        if (includeLinkages) {
          val linkages = getTypedOrElse[Map[String, Any]](map, Labels.EntityLinkages, Map.empty[String, Any])
          val links = linkages.asInstanceOf[Map[String, Map[String, Any]]]
          val resolvedLinkages = links.map { case (propName, values) =>
            val temps = get[Iterable[String]](values, Labels.EntityLinkageTemporaryRefs)
            val finals = get[Map[String, Int]](values, Labels.EntityLinkageFinalRefs)
            val linkages = temps.map(EntityReference.temporaryFromString).toSet ++
              finals.map { case (eref, typeId) => EntityReference.fromString(eref, Option(typeId)) }.toSet
            EntityLinkageProperty(propName, values(Labels.EntityLinkagePropertyType).asInstanceOf[String]) ->
              linkages.map(EntityLinkage.apply)
          }
          Some(resolvedLinkages)
        } else Some(Map.empty[EntityLinkageProperty, Set[SerializedEntity.EntityLinkage]])
      } else None
    val slot = getOrElse[Int](map, Labels.Slot, 0)
    val cmid = getOption[String](map, Labels.CmId).map(CmReference.fromString)
    SerializedEntity(
      entityRef = eref,
      cmid = cmid,
      className = className,
      properties = props,
      keys = keys,
      types = types,
      inlinedEntities = inlined,
      linkages = linkages,
      slot = slot
    )
  }
}

object PropertySerialization {

  def serialize(thing: Any, ordered: Boolean = false): String = {
    serialize(thing, if (ordered) OrderedTypedRefAwareJsonTypeTransformer else TypedRefAwareJsonTypeTransformer)
  }
  def serialize(thing: Any, transformer: StringTypeTransformerOps): String = {
    val value = transformer.serialize(thing)
    JacksonObjectMapper.mapper.writeValueAsString(value)
  }
  def serializeBytes(thing: Any, transformer: StringTypeTransformerOps): Array[Byte] = {
    val value = transformer.serialize(thing)
    JacksonObjectMapper.mapper.writeValueAsBytes(value)
  }
  def deserialize[T](
      encodedProps: String,
      transformer: StringTypeTransformerOps = TypedRefAwareJsonTypeTransformer): T = {
    val propsMap = JacksonObjectMapper.mapper.readValue(encodedProps, classOf[Any])
    transformer.deserialize(propsMap).asInstanceOf[T]
  }
  def propertiesToJsonString(
      map: Map[String, Any],
      transformer: StringTypeTransformerOps = TypedRefAwareJsonTypeTransformer): String = {
    serialize(map, transformer)
  }
  def propertiesToJsonBytes(
      map: Map[String, Any],
      transformer: StringTypeTransformerOps = TypedRefAwareJsonTypeTransformer): Array[Byte] = {
    serializeBytes(map, transformer)
  }

  // Note that these deal with Seq not Array since AbstractTypeTransformerOps.serialize has handling for Seq
  def sortedPropertyValuesToJsonString(vals: Seq[Any]): String = serialize(vals, ordered = true)
  def sortedPropertyValuesFromJsonString(encodedProps: String): Seq[Any] =
    deserialize[Seq[Any]](encodedProps, OrderedJsonTypeTransformer)

  def propertiesToJsonString(seq: Seq[(String, Any)]): String = {
    serialize(seq, TypedRefAwareJsonTypeTransformer)
  }
  def propertiesToJsonString(seq: Seq[(String, Any)], transformer: StringTypeTransformerOps): String = {
    serialize(seq, transformer)
  }
  def fromEncodedProps(encodedProps: String, ordered: Boolean = false): Map[String, Any] = {
    val propsMap = JacksonObjectMapper.mapper.readValue(encodedProps, classOf[Map[String, Any]])
    val map = TypedRefAwareJsonTypeTransformer.deserialize(propsMap).asInstanceOf[Map[String, Any]]
    if (ordered) SortedMap(map.toSeq: _*) else map
  }
  def fromByteArrayEncodedProps(encodedProps: Array[Byte], ordered: Boolean = false): Map[String, Any] = {
    val propsMap = JacksonObjectMapper.mapper.readValue(encodedProps, classOf[Map[String, Any]])
    val map = TypedRefAwareJsonTypeTransformer.deserialize(propsMap).asInstanceOf[Map[String, Any]]
    if (ordered) SortedMap(map.toSeq: _*) else map
  }
  def fromEncodedProps(encodedProps: String, transformer: StringTypeTransformerOps): Map[String, Any] = {
    val propsMap = JacksonObjectMapper.mapper.readValue(encodedProps, classOf[Map[String, Any]])
    transformer.deserialize(propsMap).asInstanceOf[Map[String, Any]]
  }

  def seqFromEncodedProps(encodedProps: String): Seq[(String, Any)] = {
    val propsSeq = deserialize[Seq[Seq[Any]]](encodedProps)
    val seq = propsSeq map { x: Seq[Any] =>
      (x.head.asInstanceOf[String], x.tail.head)
    }
    seq.toList // materialize any streams
  }

}

object JacksonObjectMapper {
  val mapper = {
    val map = new ObjectMapper
    map.registerModule(DefaultScalaModule)
    map
  }

  def readJsonMap(data: String) = JacksonObjectMapper.mapper.readValue(data, classOf[Map[String, Any]])
}

object TypedRefAwareJsonTypeTransformer extends StringTypeTransformerOps with TypedReferenceAwareTransformer[String]

object OrderedTypedRefAwareJsonTypeTransformer
    extends StringTypeTransformerOps
    with TypedReferenceAwareTransformer[String]
    with Ordered

object JsonTypeTransformer extends StringTypeTransformerOps with ReferenceTransformer[String]

object OrderedJsonTypeTransformer extends StringTypeTransformerOps with ReferenceTransformer[String] with Ordered

trait Ordered {
  self: StringTypeTransformerOps =>
  override protected def toJavaMap(m: Map[String, Any]): java.util.Map[String, Any] = {
    val output = new java.util.TreeMap[String, Any]()
    m.foreach { case (key, value) => output.put(key, serialize(value)) }
    output
  }

  override protected def toJavaSet[T](s: Set[T]) = new java.util.TreeSet[T](s.asJava)
}
