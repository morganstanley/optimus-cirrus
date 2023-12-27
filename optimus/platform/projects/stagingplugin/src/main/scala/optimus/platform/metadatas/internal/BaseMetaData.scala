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
package optimus.platform.metadatas.internal

import optimus.config.spray.json.DefaultJsonProtocol

sealed trait BaseMetaData {
  val fullClassName: String
  val packageName: String
  val isTrait: Boolean
  val parentNames: List[String]

  def localClassName: String = {
    if (packageName.isEmpty) fullClassName
    else fullClassName.substring(packageName.length() + 1)
  }

  def flags: Byte
  def slotNumber: Int
  def explicitSlotNumber: Boolean
}

case class EntityBaseMetaData(
    fullClassName: String,
    packageName: String,
    isStorable: Boolean,
    isAbstract: Boolean,
    isObject: Boolean,
    isTrait: Boolean,
    slotNumber: Int,
    explicitSlotNumber: Boolean,
    parentNames: List[String],
    piiElementMap: Map[String, String])
    extends BaseMetaData {

  def flags: Byte = {
    var flag = 0
    if (isStorable) flag |= ClassMetaData.flagIsStorable
    if (isAbstract) flag |= ClassMetaData.flagIsAbstract
    if (isObject) flag |= ClassMetaData.flagIsObject
    if (isTrait) flag |= ClassMetaData.flagIsTrait
    flag |= ClassMetaData.flagIsEntity
    flag.toByte
  }
}

case class MetaBaseMetaData(
    fullClassName: String,
    packageName: String,
    owner: String,
    catalogClass: String,
    isEntity: Boolean,
    isStorable: Boolean,
    isObject: Boolean,
    isTrait: Boolean,
    parentNames: List[String])
    extends BaseMetaData {

  override def flags: Byte = {
    var flag = 0
    if (isStorable) flag |= ClassMetaData.flagIsStorable
    if (isEntity) flag |= ClassMetaData.flagIsEntity
    if (isObject) flag |= ClassMetaData.flagIsObject
    if (isTrait) flag |= ClassMetaData.flagIsTrait
    flag |= ClassMetaData.flagIsMeta
    flag.toByte
  }

  def slotNumber: Int = -1
  def explicitSlotNumber: Boolean = false
}

case class EmbeddableBaseMetaData(
    fullClassName: String,
    packageName: String,
    isTrait: Boolean,
    isObject: Boolean,
    parentNames: List[String])
    extends BaseMetaData {

  def flags: Byte = {
    var flag = 0
    if (isTrait) flag |= ClassMetaData.flagIsTrait
    if (isObject) flag |= ClassMetaData.flagIsObject
    flag |= ClassMetaData.flagIsStorable
    flag |= ClassMetaData.flagIsEmbeddable
    flag.toByte
  }
  def slotNumber: Int = -1
  def explicitSlotNumber: Boolean = false
}

case class EventBaseMetaData(
    fullClassName: String,
    packageName: String,
    isTrait: Boolean,
    isObject: Boolean,
    parentNames: List[String])
    extends BaseMetaData {

  def flags: Byte = {
    var flag = 0
    if (isTrait) flag |= ClassMetaData.flagIsTrait
    if (isObject) flag |= ClassMetaData.flagIsObject
    flag |= ClassMetaData.flagIsStorable
    flag |= ClassMetaData.flagIsEvent
    flag.toByte
  }
  def slotNumber: Int = -1
  def explicitSlotNumber: Boolean = false
}

private[optimus] object MetaDataFiles {
  val entityMetaDataFileName = "entity.json"
  val embeddableMetaDataFileName = "embeddable.json"
  val eventMetaDataFileName = "event.json"
  val storedEntityMetaDataFileName = "stored.json"
  val metaSquaredDataFileName = "meta.json"

  val metadataFileNames: List[String] =
    MetaDataFiles.entityMetaDataFileName ::
      MetaDataFiles.storedEntityMetaDataFileName ::
      MetaDataFiles.embeddableMetaDataFileName ::
      MetaDataFiles.eventMetaDataFileName ::
      MetaDataFiles.metaSquaredDataFileName ::
      Nil

  val fileCharset = "UTF-8"
}

private[optimus] object ClassMetaData {
  val flagIsStorable: Byte = 0x1
  val flagIsAbstract: Byte = 0x2
  val flagIsObject: Byte = 0x4
  val flagIsTrait: Byte = 0x8
  val flagIsEntity: Byte = 0x10
  val flagIsEmbeddable: Byte = 0x20
  val flagIsEvent: Byte = 0x40
  // just in time, last bit of the signed java Byte
  val flagIsMeta: Byte = -0x80

  class MetaDataBuilder[T <: BaseMetaData](raw: Map[String, T]) {
    // mutable data used for performance to build result
    // the result is immutable
    // there is no multi-threaded access in this method
    // and the mutable data does not escape the method
    import scala.collection.mutable
    private val dataBuilder = new mutable.HashMap[String, ClassMetaData]()

    def build: Map[String, ClassMetaData] = {
      // first pass: build metadata and resolve parents recursively
      raw.values.foreach(name => getOrBuild(name.fullClassName))

      // second pass: compute children (we can do this non-recursively since the full parent hierarchy is known already)
      val children = dataBuilder.map { case (k, _) => (k, new mutable.HashSet[ClassMetaData]()) }
      dataBuilder.values.foreach { child =>
        child.parents.foreach { p =>
          children(p.fullClassName) += child
        }
      }

      // third pass: set children into their parents
      dataBuilder.values.foreach { meta =>
        meta._children = children(meta.fullClassName).toSet
      }

      dataBuilder.toMap
    }

    // interned parent sets help to reduce memory usage since many classes have the same set of parents
    private val parentsInterner = new mutable.HashMap[Set[String], Set[ClassMetaData]]()

    private def getOrBuild(className: String): ClassMetaData = {
      dataBuilder.getOrElseUpdate(
        className, {
          val base = raw(className)
          val meta = ClassMetaData(base)
          dataBuilder.put(meta.fullClassName, meta)

          // recursively resolve parents (since number of parents is in practice small, this uses little stack)
          val parentNames = base.parentNames.filter(raw.keySet).toSet
          meta._parents = parentsInterner.getOrElseUpdate(parentNames, parentNames.map(name => getOrBuild(name)))
          meta
        }
      )
    }
  }

  def buildMetaData[T <: BaseMetaData](raw: Map[String, T]): Map[String, ClassMetaData] =
    new MetaDataBuilder(raw).build

  def apply[T <: BaseMetaData](baseMetaData: T): ClassMetaData = {
    val slotNumber = baseMetaData match {
      case e: EntityBaseMetaData => e.slotNumber
      case _                     => -1
    }
    val explicitSlotNumber = baseMetaData match {
      case e: EntityBaseMetaData => e.explicitSlotNumber
      case _                     => false
    }
    val catalogingInfo: Option[(String, String)] = baseMetaData match {
      case e: MetaBaseMetaData => Some((e.owner, e.catalogClass))
      case _                   => None
    }
    val piiElementMap = baseMetaData match {
      case e: EntityBaseMetaData => e.piiElementMap
      case _                     => Map.empty[String, String]
    }
    new ClassMetaData(
      baseMetaData.fullClassName,
      baseMetaData.flags,
      slotNumber,
      explicitSlotNumber,
      catalogingInfo,
      piiElementMap)
  }
}

private[optimus] class ClassMetaData private (
    val fullClassName: String,
    private val flags: Byte,
    val slotNumber: Int,
    val explicitSlotNumber: Boolean,
    val catalogingInfo: Option[(String, String)],
    val piiElementMap: Map[String, String]
) {

  def packageName: String = {
    packageIndex match {
      case -1 => ""
      case i  => fullClassName.substring(0, i)
    }
  }
  def localClassName: String = {
    packageIndex match {
      case -1 => fullClassName
      case i  => fullClassName.substring(i + 1)
    }
  }
  def isStorable: Boolean = 0 != (flags & ClassMetaData.flagIsStorable)
  def isAbstract: Boolean = 0 != (flags & ClassMetaData.flagIsAbstract)
  def isObject: Boolean = 0 != (flags & ClassMetaData.flagIsObject)
  def isTrait: Boolean = 0 != (flags & ClassMetaData.flagIsTrait)
  def isEntity: Boolean = 0 != (flags & ClassMetaData.flagIsEntity)
  def isEmbeddable: Boolean = 0 != (flags & ClassMetaData.flagIsEmbeddable)
  def isEvent: Boolean = 0 != (flags & ClassMetaData.flagIsEvent)
  def isMeta: Boolean = 0 != (flags & ClassMetaData.flagIsMeta)
  def isStorableConcreteEntity: Boolean = {
    val setFlags = ClassMetaData.flagIsEntity | ClassMetaData.flagIsStorable
    val clearedFlags = ClassMetaData.flagIsAbstract | ClassMetaData.flagIsTrait
    (flags & setFlags) == setFlags && (flags & clearedFlags) == 0
  }

  // Metadata can be either an entity or an embeddable or an event
  require(isEmbeddable || isEntity || isEvent || isMeta)
  require(!(isEvent && isEmbeddable))
  require(!(isEvent && isEntity))
  require(!(isEmbeddable && isEntity))

  private def packageIndex = fullClassName.lastIndexOf('.')

  private var _parents: Set[ClassMetaData] = Set.empty
  def parents: Set[ClassMetaData] = _parents
  private var _children: Set[ClassMetaData] = Set.empty
  def children: Set[ClassMetaData] = _children

  def allChildren: Set[ClassMetaData] = children.foldLeft(Set.empty[ClassMetaData]) { case (found, child) =>
    val currentChildren = child.allChildren
    (found + child) ++ currentChildren
  }
  def allParents: Set[ClassMetaData] = parents.foldLeft(Set.empty[ClassMetaData]) { case (found, parent) =>
    (found + parent) ++ parent.allParents
  }

  def flagsToString: String =
    (if (isStorable) "@storable " else "") +
      (if (isAbstract) "abstract " else "") +
      (if (isTrait) "trait " else "") +
      (if (isObject) "object " else "") +
      (if (!isObject && !isTrait) "class " else "")

  override def toString =
    s"ClassMetaData[ package $packageName $flagsToString @stored @entity ${if (explicitSlotNumber) s"(schemaVersion=$slotNumber)"
      else ""} $localClassName parents = ${parents map (_.fullClassName)} children = ${children map (_.fullClassName)}]"

  override def hashCode: Int = fullClassName.hashCode
  override def equals(that: Any): Boolean = {
    that match {
      case other: ClassMetaData => this.fullClassName == other.fullClassName
      case _                    => false
    }
  }
}

// implicit values used for spray json
object MetaJsonProtocol extends DefaultJsonProtocol {
  import optimus.config.spray.json._
  implicit val entityBaseMetaDataProtocol: RootJsonFormat[EntityBaseMetaData] = jsonFormat10(EntityBaseMetaData.apply)
  implicit val embeddableBaseMetaDataProtocol: RootJsonFormat[EmbeddableBaseMetaData] =
    jsonFormat5(EmbeddableBaseMetaData.apply)
  implicit val eventBaseMetaDataProtocol: RootJsonFormat[EventBaseMetaData] = jsonFormat5(EventBaseMetaData.apply)
  implicit val metaBaseMetaDataProtocol: RootJsonFormat[MetaBaseMetaData] = jsonFormat9(MetaBaseMetaData.apply)
  implicit object BaseMetaDataFormat extends RootJsonFormat[BaseMetaData] {
    def write(b: BaseMetaData): JsValue = {
      b match {
        case md: EntityBaseMetaData     => md.toJson
        case md: EmbeddableBaseMetaData => md.toJson
        case md: MetaBaseMetaData       => md.toJson
        case md: EventBaseMetaData      => md.toJson
      }
    }
    def read(value: JsValue): BaseMetaData =
      deserializationError("Should not call Json.convertTo[BaseMetaData], please use subclasses instead")
  }
}
