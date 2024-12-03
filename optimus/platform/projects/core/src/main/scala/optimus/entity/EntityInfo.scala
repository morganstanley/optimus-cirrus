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
package optimus.entity

import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.config.EmptyNodeConfig
import optimus.config.NodeCacheConfigs
import optimus.config.OptimusConfigParsingException
import optimus.config.PerPropertyConfigGroup
import optimus.graph.DiagnosticSettings
import optimus.graph.PropertyInfo
import optimus.graph.diagnostics.DbgObjectSupport
import optimus.platform.UpcastDomain
import optimus.platform.pickling.PickledInputStream
import optimus.platform.pickling.ReflectiveEntityPickling
import optimus.platform.pickling.UnsafeFieldInfo
import optimus.platform.relational.tree.MemberDescriptor
import optimus.platform.storable._
import optimus.platform.util.ReflectUtils

import java.lang.reflect.Field
import scala.collection.immutable
import scala.collection.mutable
import scala.collection.{concurrent => c}
import scala.reflect.runtime.universe.TypeTag
import scala.util.Try
import scala.util.hashing.MurmurHash3

trait OptimusInfo {
  def runtimeClass: Class[_]
  // TODO (OPTIMUS-47350): warning! not the API to use if you're looking for cached constructor PropertyInfos...
  def properties: collection.Seq[PropertyInfo[_]]

  OptimusInfo.registry.put(this, ())

  /** the TypeTag for the class or module represented by this OptimusInfo */
  private[optimus] final lazy val typeTag: TypeTag[_] = ReflectUtils.typeTagForClass(runtimeClass)

  /** if typeTag represents a class, returns the TypeTag of companion module of that class, else returns typeTag */
  private[optimus] final lazy val moduleTypeTag: TypeTag[_] = {
    val sym = typeTag.tpe.typeSymbol
    if (sym.isModuleClass) typeTag
    else ReflectUtils.mkTypeTag(sym.asClass.companion.typeSignature, typeTag.mirror)
  }

  private[optimus] def applyConfig(): Unit = {
    val entityName = runtimeClass.getName
    val appliedPropertyNames = mutable.Set[String]()
    // find all entity configs that could apply to any properties on this entity upfront (rather than calling
    // mergedNodeConfig for each property, because that would repeat this work every time)
    val entityConfigs = NodeCacheConfigs.entityConfigs.flatMap(_.configForEntity(entityName))
    if (entityConfigs.nonEmpty) {
      for (p <- properties) {
        // n.b. we merge and then apply (instead of just applying each in turn) because merging isn't equivalent to
        // repeated application in the presence of DontCache and custom cache rules -
        // see OptconfMergeRegexTests#mergeDontCacheWithRegexCustomCache
        val merged = entityConfigs.flatMap(_.configForNode(p.name)).foldLeft(EmptyNodeConfig.emptyNodeConfig)(_ merge _)
        if (merged != EmptyNodeConfig.emptyNodeConfig) {
          merged.apply(p)
          appliedPropertyNames += p.name
        }
      }
    }

    // warn for all nodes that don't exist in runtime but exist in optconf
    for (p <- entityConfigs.collect { case p: PerPropertyConfigGroup => p }) {
      val config = p.propertyToConfig
      val nonAppliedPropertyNames = config.keySet -- appliedPropertyNames
      val nodeNames = nonAppliedPropertyNames
        .map { nodeName =>
          if (!config.getOrElse(nodeName, EmptyNodeConfig.emptyNodeConfig).isPgoGen)
            nodeName
          else ""
        }
        .filter(_.nonEmpty)
        .mkString(",")
      if (nodeNames.nonEmpty) {
        val optconfPaths = NodeCacheConfigs.getOptconfProviderPaths.mkString(",")
        val message =
          s"Nodes $nodeNames on entity $entityName not found at runtime when applying optconfs: $optconfPaths."
        val e = new OptimusConfigParsingException(message)
        if (DiagnosticSettings.throwOnOptconfParsingFailure)
          throw e
        else if (DiagnosticSettings.warnOnOptconfParsingFailure)
          OptimusInfo.log.warn(message, e)
      }
    }
  }

  val propertyMetadata: collection.Map[String, PropertyInfo[_]] = {
    val props = properties
    if (props.isEmpty)
      immutable.Map.empty[String, PropertyInfo[_]]
    else {
      val m = new mutable.HashMap[String, PropertyInfo[_]]()
      val it = props.iterator
      while (it.hasNext) {
        // the same PropertyInfo instances are used by the class that originally defined the property and any subclasses
        // which inherit it, but the former should always be the owner. Usually the superclass will get here first
        // but sometimes user code affects initialization order such that a subclass gets here first, in which case
        // when the super class gets here it needs to overwrite the entityInfo.
        val p = it.next()
        if ((p.entityInfo eq null) || runtimeClass.isAssignableFrom(p.entityInfo.runtimeClass))
          p.setEntityInfo(this)

        m.put(p.name(), p)
      }
      applyConfig()
      m
    }
  }

  /**
   * expected to be better distributed than runtimeClass.getName.hashCode()
   */
  final val entityClassHash = MurmurHash3.stringHash(runtimeClass.getName)
}

object OptimusInfo {
  private[optimus] val log = getLogger(getClass)
  private[optimus] val registry = c.TrieMap.empty[OptimusInfo, Unit]
}

trait StorableInfo extends OptimusInfo {
  type BaseType <: Storable
  type PermRefType <: StorableReference
  def runtimeClass: Class[_]
  def indexes: collection.Seq[IndexInfo[_, _]]
  def keys: collection.Seq[IndexInfo[_, _]]
  def linkages: collection.Seq[LinkageType] = collection.Seq.empty

  def storedProperties: Iterable[PropertyInfo[_]] = properties collect {
    case s: PropertyInfo[_] if s.isStored => s
  }

  def deserializePermReference(rep: String): PermRefType

  protected[optimus] def unsafeFieldInfo: collection.Seq[UnsafeFieldInfo]

  def createUnpickled(is: PickledInputStream, forceUnpickle: Boolean): BaseType
  final def createUnpickled(is: PickledInputStream): BaseType = createUnpickled(is, forceUnpickle = false)
}

abstract class EntityInfo(props: collection.Seq[PropertyInfo[_]], val parents: collection.Seq[ClassEntityInfo])
    extends OptimusInfo
    with StorableInfo {
  import optimus.platform.storable.EntityReference
  type BaseType = Entity
  type PermRefType = EntityReference
  lazy val properties: collection.Seq[PropertyInfo[_]] = props.toVector

  /**
   * Whether or not the Entity is storable.
   */
  val isStorable: Boolean

  lazy val dbgFieldMap: DbgObjectSupport.FieldMap = DbgObjectSupport.setUp(runtimeClass, classOf[EntityImpl])

  def deserializePermReference(rep: String): PermRefType = EntityReference.fromString(rep)
  // TODO (OPTIMUS-0000): This should go away, but old semantics code is using it. Remove after that code is removed.
  val indexes: collection.Seq[IndexInfo[_, _]] = collection.Seq.empty
  lazy val keys: collection.Seq[IndexInfo[_, _]] = indexes filter { _.unique }

  def log: msjava.slf4jutils.scalalog.Logger

  protected[optimus] lazy val unsafeFieldInfo: collection.Seq[UnsafeFieldInfo] =
    ReflectiveEntityPickling.instance.prepareMeta(this)

  def createUnpickled(
      is: PickledInputStream,
      forceUnpickle: Boolean,
      storageInfo: StorageInfo,
      entityRef: EntityReference): BaseType = {
    ReflectiveEntityPickling.instance.unpickleCreate(this, is, forceUnpickle, storageInfo, entityRef)
  }

  override def createUnpickled(is: PickledInputStream, forceUnpickle: Boolean): BaseType =
    createUnpickled(is, forceUnpickle, UniqueStorageInfo, null)

  // generated by '@projected def' to provide accurate return type
  lazy val extraProjectedMembers: List[MemberDescriptor] = {
    val companion = EntityInfoRegistry.getCompanion(runtimeClass)
    val methods = companion.getClass.getMethods.toList.filter(m =>
      m.getParameterCount == 0 && m.getReturnType == classOf[MemberDescriptor] && m.getName.endsWith("$md"))
    methods.map(m => m.invoke(companion).asInstanceOf[MemberDescriptor])
  }

  private[this] lazy val outerAccessorOrNull: Field = Try {
    val f = runtimeClass.getDeclaredField("$outer")
    f.setAccessible(true)
    f
  }.getOrElse(null)

  /**
   * returns the value of the scalac generated $outer field (for inner classes) or null if the field is not present
   * (which could happen if the class is not an inner class, or if it didn't reference members of the outer class)
   */
  final private[optimus] def outerOrNull(entity: Entity): AnyRef = {
    val a = outerAccessorOrNull
    if (a eq null) null else a.get(entity)
  }
}

/**
 * Runtime type information for entity classes.
 *
 * Note: If the properties for this class ever change, you need to update EntityInfoComponent to correctly generate the
 * constructor call.
 */
class ClassEntityInfo(
    clazz: Class[_],
    override val isStorable: Boolean,
    props: collection.Seq[PropertyInfo[_]],
    parents: collection.Seq[ClassEntityInfo] = collection.Seq.empty,
    override val indexes: collection.Seq[IndexInfo[_ <: Storable, _]] = collection.Seq.empty,
    val upcastDomain: Option[UpcastDomain] = None)
    extends EntityInfo(props, parents) {
  lazy val runtimeClass: Class[_ <: Entity] = clazz.asSubclass(classOf[Entity])
  override def toString: String = runtimeClass.getName

  private lazy val parentTypes: Set[ClassEntityInfo] = parents.foldLeft(Set.empty[ClassEntityInfo])(_ union _.baseTypes)

  lazy val baseTypes: Set[ClassEntityInfo] = parentTypes + this

  override lazy val linkages: collection.Seq[LinkageType] = (properties collect {
    case p if p.isChildToParent => EntityLinkageProperty(p.name, runtimeClass.getName)
  }) ++ (parentTypes flatMap { _.linkages })

  lazy val slot: Int = optimus.platform.util.DefaultSlotDiscover.getSlot(runtimeClass)

  override def createUnpickled(is: PickledInputStream, forceUnpickle: Boolean): BaseType =
    createUnpickled(is, forceUnpickle, AppliedStorageInfo, null)

  // Logging API is unable to deal with wrapper classes generated by the scala toolbox compiler
  override val log: Logger = if (runtimeClass.getName.startsWith("__wrapper")) {
    msjava.slf4jutils.scalalog.getLogger("ScalaToolboxProxyEntity")
  } else {
    msjava.slf4jutils.scalalog.getLogger(runtimeClass)
  }
}

/**
 * Runtime type information for entity modules.
 *
 * Note: Calls to this class's constructor are generated by the optimus_storedprops phase; if any constructor args are
 * added, that phase must be changed to match
 *
 * @param runtimeClass
 *   The class of the module
 * @param properties
 *   The properties for the module
 * @param parents
 *   The superclasses of the module
 */
class ModuleEntityInfo(
    val runtimeClass: Class[_],
    val isStorable: Boolean,
    props: collection.Seq[PropertyInfo[_]],
    parents: collection.Seq[ClassEntityInfo] = collection.Seq.empty
) extends EntityInfo(props, parents) {
  override val log: Logger = msjava.slf4jutils.scalalog.getLogger(runtimeClass)
  override val indexes: collection.Seq[IndexInfo[_, _]] = collection.Seq.empty

  override def toString: String = runtimeClass.getName
}
