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
package optimus.config

import msjava.base.io.IOUtils
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.breadcrumbs.Breadcrumbs
import optimus.breadcrumbs.ChainedID
import optimus.breadcrumbs.crumbs.Properties
import optimus.breadcrumbs.crumbs.PropertiesCrumb
import optimus.config.spray.json._
import optimus.core.TPDMask
import optimus.entity.OptimusInfo
import optimus.graph.CacheConfigUtils
import optimus.graph.NodeTaskInfo
import optimus.graph.NodeTrace
import optimus.graph.Settings
import optimus.graph.cache.CacheDefaults
import optimus.graph.cache.Caches
import optimus.graph.cache.NCPolicy
import optimus.graph.cache.NodeCCache
import optimus.graph.cache.PerPropertyCache
import optimus.graph.cache.UNodeCache
import optimus.graph.diagnostics.gridprofiler.GridProfiler.ProfilerOutputStrings
import optimus.platform.annotations.deprecating

import java.util
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.lang.{Boolean => JBoolean}
import java.lang.{Integer => JInteger}
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import scala.annotation.nowarn
import scala.collection.mutable
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import Constants._
import optimus.platform.inputs.registry.ProcessGraphInputs
import optimus.platform.storable.EmbeddableCtrNodeSupport

import java.util.regex.Pattern

/**
 * This interface represent a set of classes that can be converted to/from standard spray-json JsValue We pattern-match
 * against this trait in CacheProtocol to define common write-unsupported message
 */
private[config] sealed trait BasicJsonElement

private[config] sealed trait ConfigOrReference extends BasicJsonElement
private[config] final case class ConfigurationReference(section: String, key: String, sourceLine: String)
    extends ConfigOrReference
private[config] final case class InlineConfiguration(content: JsObject) extends ConfigOrReference

private[optimus] object OptconfProvider {
  val log: Logger = getLogger(OptconfProvider)

  def fromPath(path: String, scopePath: String = null): OptconfProvider = OptconfPath(path, Option(scopePath))
  def fromContent(content: String, path: String = null, scopePath: String = null): OptconfProvider =
    if ((content ne null) && content.nonEmpty) OptconfContent(content, path, Option(scopePath))
    else EmptyOptconfProvider

  def readOptconfFile(path: String): Option[String] = {
    NodeCacheInfo.openOptimusConfigFile(path).map { stream =>
      val configContent =
        try { IOUtils.readBytes(stream) }
        finally { stream.close() }
      log.info(s"Typesafe property settings configuration loaded from: $path")
      new String(configContent, Charset.forName(CONFIG_FILE_CHARSET)).trim
    }
  }
}

private[optimus] sealed trait OptconfProvider {
  def nonEmpty: Boolean
  val optconfPath: String
  def jsonContent: String
  def scopePath: Option[String]
}

private[optimus] case object EmptyOptconfProvider extends OptconfProvider {
  override def nonEmpty: Boolean = false
  override val optconfPath: String = null
  override val jsonContent: String = emptyOptconf
  override def toString: String = "EmptyOptconfProvider"
  override def scopePath: Option[String] = None
}

private[optimus] final case class OptconfPath(optconfPath: String, scopePath: Option[String] = None)
    extends OptconfProvider {
  import OptconfProvider._
  @transient lazy val jsonContent: String = readOptconfFile(optconfPath).getOrElse(emptyOptconf)
  override def nonEmpty: Boolean = optconfPath ne null
}

private[optimus] final case class OptconfContent(
    jsonContent: String,
    optconfPath: String = null,
    scopePath: Option[String] = None)
    extends OptconfProvider {
  override def nonEmpty: Boolean = jsonContent ne null

  // don't log the entire contents
  override def toString: String =
    s"OptconfContent(${if (optconfPath ne null) optconfPath else "[content]"}${scopePath.map(tp => s", $tp").getOrElse("")})"
}

// TODO (OPTIMUS-15654): improve syntax of type safe config to support
//                                                      global value section
// case class GlobalValueHolder[T](value: T) // hold primary types: numerics, boolean and string
private[optimus] object CacheConfig {
  def apply(name: String): CacheConfig =
    CacheConfig(
      name,
      sharable = true,
      getDefaultSize(name),
      evictOnLowMemory = true
    )

  private def getDefaultSize(name: String): Int =
    if (name == UNodeCache.globalCacheName) Settings.cacheSize
    else if (name == UNodeCache.globalSICacheName) Settings.cacheSISize
    else CacheDefaults.DEFAULT_SHARED_SIZE

  def fromCache(cache: NodeCCache): CacheConfig =
    CacheConfig(
      name = cache.getName,
      sharable = cache.sharable,
      cacheSizeLimit = cache.getMaxSize,
      evictOnLowMemory = cache.isEvictOnLowMemory
    )

  private val log = getLogger(this.getClass)
}

private[optimus] final case class CacheConfig(
    name: String,
    sharable: Boolean,
    cacheSizeLimit: Int,
    evictOnLowMemory: Boolean = true
) extends BasicJsonElement {

  def toJsonString: String = {
    val parts = List.newBuilder[String]

    def addToken(name: String, v: Any): Unit =
      v match {
        case _: String => parts += s""""$name": "$v""""
        case _: Int    => parts += s""""$name": $v"""
        case _ => throw new IllegalArgumentException("Should only be putting ints and strings into optconf files")
      }

    addToken("name", name)
    addToken("cacheSize", cacheSizeLimit)
    if (!evictOnLowMemory) addToken("evictOnLowMemory", evictOnLowMemory)

    parts.result().mkString("{ ", ", ", " }")
  }

  def addPerPropertyDefaults(): CacheConfig = this.copy(sharable = false)

  def getOrInstantiateCache: NodeCCache = {
    if (sharable) {
      val c = Caches
        .getSharedCache(name)
        .getOrElse({
          CacheConfig.log.info(s"Created instance of shared cache $name")
          new UNodeCache(name, cacheSizeLimit, evictOnLowMemory)
        })

      if (!(c.getMaxSize == cacheSizeLimit)) {
        CacheConfig.log.warn(s"Cache with same name $name is initialized with different values!")
        Breadcrumbs.info(
          ChainedID.root,
          PropertiesCrumb(
            _,
            Properties.event -> "OverridingExistingCache",
            Properties.name -> name,
            Properties.newCacheSize -> cacheSizeLimit,
            Properties.oldCacheSize -> c.getMaxSize,
            Properties.stackTrace -> Thread.currentThread().getStackTrace.map(_.getMethodName)
          )
        )
        new UNodeCache(name, cacheSizeLimit, evictOnLowMemory)
      } else c
    } else {
      // per property - we dont want to remember this - otherwise we will share the per property caches
      // and a config with a per-property cache is not a shared cache
      CacheConfig.log.info(s"Created instance of per property cache $name")
      new PerPropertyCache(name, cacheSizeLimit, evictOnLowMemory)
    }
  }

  override def toString =
    s"CacheConfig(name = $name, sharable = $sharable, cacheSizeLimit = $cacheSizeLimit, evictOnLowMemory = $evictOnLowMemory)"

  def toDebugString: String = {
    val properties =
      s"name = $name" ::
        s"sharable = $sharable" ::
        s"cacheSizeLimit = $cacheSizeLimit" ::
        s"evictOnLowMemory = $evictOnLowMemory" :: Nil
    s"CacheConfig(${properties.mkString(", ")})"
  }
}

private[optimus] sealed trait CachePolicyConfig {
  val cachePolicy: NCPolicy
  val cache: Option[CacheConfig]
  val scopePath: Option[String] = None
  final def disableCache: Boolean = cachePolicy eq NCPolicy.DontCache
}

final case class ConstructorConfig(cachePolicy: NCPolicy) extends CachePolicyConfig {
  val cache: Option[CacheConfig] = None // we don't support custom cache for constructor nodes
  if (!valid) throw new ConstructorConfigParsingException
  private def valid: Boolean = cachePolicy eq NCPolicy.DontCache
}

// e.g. { "cache": ${Caches.a2}, "gcnative": true, "localSlot": 5,  "cachePolicy": "BasicPolicy", "syncId": 6}
// n.b. we are using Java types in the Options because these fields are used from Java code
// and Scala erases value types as Object so Java would see Option[Int] as Option[Object].
private[optimus] final case class PropertyConfig(
    name: String,
    cachePolicy: NCPolicy,
    cache: Option[CacheConfig],
    gcnative: Option[JBoolean],
    syncId: Option[JInteger],
    localSlot: Option[JInteger],
    twkID: Option[JInteger],
    dependsOnMask: Option[String],
    trackForInvalidation: Option[JBoolean],
    override val scopePath: Option[String] // Currently only applied to cache policy
) extends CachePolicyConfig {
  def merge(that: PropertyConfig): PropertyConfig = {
    new PropertyConfig(
      name = that.name,
      cache = PropertyConfig.mergeScopedCache(this, that),
      cachePolicy = PropertyConfig.mergeScopedCachePolicy(this, that),
      gcnative = that.gcnative,
      syncId = if (syncId == that.syncId) that.syncId else None,
      localSlot = if (localSlot == that.localSlot) that.localSlot else None,
      twkID = if (that.twkID.isDefined) that.twkID else this.twkID,
      dependsOnMask =
        if (that.dependsOnMask.isDefined && that.dependsOnMask.get != "0") that.dependsOnMask else this.dependsOnMask,
      trackForInvalidation =
        if (that.trackForInvalidation.isDefined) that.trackForInvalidation else this.trackForInvalidation,
      scopePath = scopePath
    )
  }
}

private[optimus] object PropertyConfig {
  def apply(
      name: String,
      cachePolicy: NCPolicy,
      cache: Option[CacheConfig],
      gcnative: Option[Boolean],
      syncId: Option[Int],
      localSlot: Option[Int],
      twkID: Option[Int],
      dependsOnTweakBits: Option[String],
      trackForInvalidation: Option[Boolean],
      scopePath: Option[String])
  // dummy implicit to avoid erased signature conflict with the compiler generated apply method
  (implicit dummy: DummyImplicit): PropertyConfig =
    PropertyConfig(
      name: String,
      cachePolicy: NCPolicy,
      cache: Option[CacheConfig],
      // box the value types
      gcnative.map(x => x): Option[JBoolean],
      syncId.map(x => x): Option[JInteger],
      localSlot.map(x => x): Option[JInteger],
      twkID.asInstanceOf[Option[JInteger]],
      dependsOnTweakBits,
      trackForInvalidation.map(x => x): Option[JBoolean],
      scopePath = scopePath
    )

  def mergeScopedCache(p1: PropertyConfig, p2: PropertyConfig): Option[CacheConfig] = {
    val c1 = p1.cache
    val c2 = p2.cache

    (p1.scopePath, p2.scopePath) match {
      case (None, None)       => mergeCache(c1, c2, p2.disableCache)
      case (Some(_), None)    => c2
      case (None, Some(_))    => c1
      case (Some(_), Some(_)) => None // we don't support custom caches in scoped optconfs yet
    }
  }

  def mergeCache(
      cache1: Option[CacheConfig],
      cache2: Option[CacheConfig],
      disabledCache: Boolean): Option[CacheConfig] = {
    if (!disabledCache) {
      cache2.orElse(cache1)
    } else None // last config disables cache so we erase the previous cache config
  }

  // latest policy wins in most cases unless we have some policy defined in first and null in second
  private def pickPolicy(p1: NCPolicy, p2: NCPolicy): NCPolicy = {
    (p1, p2) match {
      case (policy, null) => policy
      case _              => p2
    }
  }

  def mergeCachePolicy(propConf: CachePolicyConfig, otherPropConf: CachePolicyConfig): NCPolicy = {
    (propConf.disableCache, otherPropConf.disableCache) match {
      case (_, true)      => NCPolicy.DontCache
      case (true, false)  => otherPropConf.cachePolicy
      case (false, false) => pickPolicy(propConf.cachePolicy, otherPropConf.cachePolicy)
    }
  }

  private def mergeScopedCachePolicy(propConf: CachePolicyConfig, otherPropConf: CachePolicyConfig): NCPolicy = {
    (propConf.scopePath, otherPropConf.scopePath) match {
      // scoped optconf + global
      case (Some(path1), None) =>
        val currentScopedPolicy = Option(propConf.cachePolicy).getOrElse(NCPolicy.Basic)
        val policyToMergeWith = otherPropConf.cachePolicy // would become global / default policy
        if (policyToMergeWith == null) {
          // TODO (OPTIMUS-74618): scoped optconfs should properly support custom caches
          NCPolicy.composite(null, currentScopedPolicy, path1)
        } else policyToMergeWith.combineWith("", currentScopedPolicy, path1)
      // global + scoped optconf
      case (None, Some(path2)) =>
        val currentPolicy = propConf.cachePolicy // global optconf
        val scopedPolicyToMergeWith = Option(otherPropConf.cachePolicy).getOrElse(NCPolicy.Basic) // scoped optconf
        if (currentPolicy == null) NCPolicy.composite(null, scopedPolicyToMergeWith, path2)
        else currentPolicy.combineWith("", scopedPolicyToMergeWith, path2)
      case (Some(path1), Some(path2)) =>
        if (path1 == path2) mergeCachePolicy(propConf, otherPropConf)
        else {
          val targetPath = propConf.scopePath.getOrElse("")
          val otherTargetPath = otherPropConf.scopePath.getOrElse("")
          val currentPolicy = Option(propConf.cachePolicy).getOrElse(NCPolicy.Basic)
          val policyToMergeWith = Option(otherPropConf.cachePolicy).getOrElse(NCPolicy.Basic)
          if (currentPolicy == null) NCPolicy.composite(null, policyToMergeWith, otherTargetPath)
          else currentPolicy.combineWith(targetPath, policyToMergeWith, otherTargetPath)
        }
      case (None, None) => mergeCachePolicy(propConf, otherPropConf)
    }
  }

  def merge(propConfigs1: Option[PropertyConfig], propConfigs2: Option[PropertyConfig]): PropertyConfig = {
    (propConfigs1, propConfigs2) match {
      case (Some(p1), Some(p2)) => p1 merge p2
      case (Some(p1), None)     => p1
      case (None, Some(p2))     => p2
      case _ =>
        new PropertyConfig(
          "",
          null,
          None,
          None,
          None,
          None,
          None,
          None,
          None,
          None
        ) // should not be the case unless we merge 2 empty optconfs
    }
  }
}

// a config line can be inline config or a reference
// and a config can contain a single config
// e.g.
// { "config": ${Configs.c1}, "tweakConfig" : ${Configs.c2} }
// or
// { "config": {"cachePolicy": "XSPolicy"}, "tweakConfig" : ${Configs.c2} }
// or
// { "config": ${Configs.c1}, "tweakConfig" : ${Configs.c2} }
object EmptyNodeConfig {
  val emptyNodeConfig: NodeConfig = NodeConfig(None, None, None)
}

private[optimus] final case class NodeConfig(
    config: Option[PropertyConfig],
    cacheConfig: Option[CacheConfig],
    tweakConfig: Option[PropertyConfig],
    isPgoGen: Boolean = false,
    scopePath: String = null) {
  def apply(info: NodeTaskInfo): Unit = {
    val cachePolicy =
      if ((info.cachePolicy ne null) && (info.cachePolicy ne NCPolicy.Basic))
        " cachePolicy = " + info.cachePolicy.toString
      else ""
    NodeCacheInfo.log.debug(
      "applying to " + info + " (cacheable:" + info.getCacheable + " cacheName = " + info
        .customCache() + ") Will Apply config = " + config + " cacheConfig = " + cacheConfig + " tweakConfig = " + tweakConfig +
        cachePolicy + " pgoGenerated:" + isPgoGen)
    info.setExternalConfig(this)
  }

  def merge(that: NodeConfig): NodeConfig = {
    if (isEmpty) that
    else if (that.isEmpty) this
    else {
      val mergedPropConfig = PropertyConfig.merge(config, that.config)
      val mergedCacheConfig = PropertyConfig.mergeCache(cacheConfig, that.cacheConfig, mergedPropConfig.disableCache)
      val mergedTweakConfig = PropertyConfig.merge(tweakConfig, that.tweakConfig)
      // scopePath should not merge if different
      NodeConfig(
        Some(mergedPropConfig),
        mergedCacheConfig,
        Some(mergedTweakConfig),
        isPgoGen && that.isPgoGen,
        scopePath)
    }
  }

  def isEmpty: Boolean = config.isEmpty && cacheConfig.isEmpty && tweakConfig.isEmpty
}

private[optimus] object NodeCacheInfo {
  val log: Logger = getLogger(this.getClass)
  val configurationSuffix = s".${ProfilerOutputStrings.optconfExtension}"

  val ClasspathRegex: Regex = "classpath:/(.*)".r
  val ScopePathRegex: Regex = "\\[(.*)](.*)".r

  def openOptimusConfigFile(pathStr: String): Option[InputStream] = Option(pathStr).map {
    case ClasspathRegex(path) => getClass.getClassLoader.getResourceAsStream(path)
    case path                 => Files.newInputStream(Paths.get(path))
  }

  // check file suffix is .optconf
  private[config] def verifyConfigPath(path: String): Option[String] = {
    if ((path ne null) && path.nonEmpty) {
      var br: BufferedReader = null
      try {
        br = new BufferedReader(new InputStreamReader(openOptimusConfigFile(path).get))
        if (!path.endsWith(NodeCacheInfo.configurationSuffix)) {
          val msg = "typesafe config file must have suffix '" + NodeCacheInfo.configurationSuffix + "'"
          throw new IllegalArgumentException(msg)
        }
        Some(path)
      } catch {
        case e: Exception =>
          log.error(s"Cannot read node cache configs from $path", e)
          None
      } finally {
        if (br != null) br.close()
      }
    } else {
      log.warn("Did you mean to pass an empty config path?")
      None
    }
  }
}

@deprecating("Only used in interop")
object InteropNodeCacheConfigs {
  def isEmpty: Boolean = NodeCacheConfigs.isEmpty
  def getRawConfigs: String = NodeCacheConfigs.getRawConfigs
}

private[optimus] object NodeCacheConfigs {
  private val log = getLogger(this.getClass)

  private def reapplyToConstructedEntities(): Unit = {
    NCPolicy.resetCaches()
    // Re-apply the config to already-constructed entities
    // Entities that are not yet constructed will pick up the new config via EntityInfo.propertyMetadata)
    for ((oi, _) <- OptimusInfo.registry)
      oi.applyConfig()

    EmbeddableCtrNodeSupport.reapplyConfig()
  }

  /**
   * The ID for a property that is configured in optconf might already exist because either:
   *   1. this property is in Optimus registry, or was traced in NodeTrace.taskInfos (easy case), OR
   *   1. a superclass was registered as in (1) (slightly harder case)
   *
   * To be safe, let's include all children of any registered properties as also 'registered'. Note: we could be less
   * aggressive about this and only look at child classes of properties if they actually are defined in optconf, but we
   * have all the NodeTaskInfos here anyway so we'll register them upfront [SEE_SUBCLASS_ID_OPTCONF]
   */
  private def allRegisteredTweakables: Map[String, Int] = {
    def propertyToIdMap(props: Iterable[NodeTaskInfo]): Map[String, Int] = {
      val propertyToTwkId = mutable.Map[String, Int]()
      props.foreach { nti =>
        val id = nti.tweakableID
        propertyToTwkId.put(nti.fullName, id)
        val children = nti.matchOnChildren
        if (children ne null) {
          val childIterator = nti.matchOnChildren.iterator
          while (childIterator.hasNext) {
            val child = childIterator.next()
            propertyToTwkId.put(child.fullName, id)
          }
        }
      }
      propertyToTwkId.toMap
    }

    /**
     * We rely on keeping track of tweakables to make sure we don't reset any tweak IDs that are already in the system,
     * since something might have registered a dependency on that ID. Since we're about to reapply optconf to the
     * entities in the registry, make sure we've captured any that have tweakableID already set, so we don't crash
     * later.
     */
    val registryProperties = OptimusInfo.registry.flatMap { case (oi, _) => oi.properties.filter(_.tweakableID > 0) }
    val registeredPropertyToTwkId = propertyToIdMap(registryProperties)
    val tracedPropertyToTwkId = propertyToIdMap(NodeTrace.findAllTweakables)
    val allProps = tracedPropertyToTwkId.keySet ++ registeredPropertyToTwkId.keySet
    allProps.map { prop =>
      // if it's not in the registry it must be in the trace since we combined keys (and we prioritise registry since
      // it's the one we use to reapply optconfs later), but keep getOrElse here otherwise Intellij highlights .get
      prop -> registeredPropertyToTwkId.getOrElse(prop, tracedPropertyToTwkId.getOrElse(prop, 0))
    }.toMap
  }

  /**
   * At this point entityConfigs contain any twkIds and masks set in optconf. This method remaps them to match any IDs
   * that are already allocated in this runtime. Tweak ID and dependency data will then be reapplied to already
   * constructed entities. Note: this mutates the existing settingsConfig
   */
  private def remapTwkIds(settingsConfig: NodeCacheConfigs): NodeCacheConfigs = {
    val remappedEntityConfigs = settingsConfig.entityConfigs.map {
      case perEntityConfig: PerEntityConfigGroup =>
        val entityToConfig = perEntityConfig.entityToConfig

        val propertyToTwkId = mutable.Map[String, JInteger]() // holder for twkIDs defined in optconf
        entityToConfig.foreach { case (entity, props) =>
          props.propertyToConfig.foreach { case (prop, nodeConfig) =>
            val twkId = nodeConfig.config.flatMap(_.twkID)
            if (twkId.isDefined)
              propertyToTwkId.put(s"$entity.$prop", twkId.get)
          }
        }

        /**
         * Get the IDs and dependencies we've registered in this runtime and remap the IDs defined in optconf to match
         * this
         */
        val tracedPropertyToTwkId = allRegisteredTweakables
        val remapped = mutable.Map[Int, Int](1 -> 1) // 1 is reserved
        propertyToTwkId.map { case (prop, twkId) =>
          val remappedIdOpt = tracedPropertyToTwkId.get(prop) // [SEE_SUBCLASS_ID_OPTCONF] - could be a parent ID
          val remappedId =
            if (remappedIdOpt.isDefined) remappedIdOpt.get
            else {
              val newId =
                NodeTaskInfo.nextTweakID(twkId) // make sure we don't overwrite a twkId that was already assigned
              log.debug(s"Configured twkID $twkId for unregistered property $prop, remapping to $newId")
              newId
            }
          // each ID ends up in this map, either remapped to something else or to itself
          remapped.put(twkId, remappedId.asInstanceOf[JInteger])
        }

        /* patch entityConfigs with remapped IDs and masks */
        val newEntityConfigs = mutable.Map[String, PerPropertyConfigGroup]()
        entityToConfig foreach { case (entity, props) =>
          val newProps = mutable.Map[String, NodeConfig]()
          props.propertyToConfig.foreach { case (prop, nodeConfig) =>
            val existingConfigs = nodeConfig.config
            val patched = existingConfigs.map { existingConfig =>
              val configuredId = existingConfig.twkID.getOrElse(-1).asInstanceOf[JInteger] // hacky
              val remappedId = remapped.get(configuredId).asInstanceOf[Option[JInteger]] // hackier

              val encodedMask =
                if (existingConfig.dependsOnMask.isEmpty) None
                else {
                  val configuredMask = TPDMask.stringDecoded(existingConfig.dependsOnMask.get)
                  val configuredDepIds = configuredMask.toIndexes
                  val remappedDepIds = configuredDepIds.flatMap { key =>
                    if (!remapped.contains(key)) {
                      // note that in MergeTraces, the integrity check applies the optconf (and the POGO job will fail if
                      // applying it fails), but there won't be any tweakables registered in this JVM since we're not
                      // actually running any optimus code [SEE_MERGE_TRACES_INTEGRITY_CHECK]
                      log.debug(s"Couldn't find twkId $key")
                      None
                    } else Some(remapped(key))
                  }
                  val remappedMask = TPDMask.fromIndexes(remappedDepIds)
                  Some(TPDMask.stringEncoded(remappedMask.toArray))
                }
              existingConfig.copy(twkID = remappedId, dependsOnMask = encodedMask)
            }
            newProps.put(prop, nodeConfig.copy(config = patched))
          }
          newEntityConfigs.put(entity, PerPropertyConfigGroup(newProps.toMap))
        }

        /* Make sure all IDs are 'reserved' so new tweakables don't get allocated the same one */
        newEntityConfigs.foreach { case (_, props) =>
          props.propertyToConfig.foreach { case (_, nodeConfig) =>
            nodeConfig.config.foreach { config =>
              val twkID = config.twkID
              if (twkID.isDefined) NodeTaskInfo.reserveTweakID(twkID.get) // [SEE_RESERVE_TWEAK_ID]
            }
          }
        }
        PerEntityConfigGroup(newEntityConfigs.toMap)
      // non-PerEntityConfigGroups can't have meaningful tweak ids anyway
      case other => other
    }
    settingsConfig.copy(entityConfigs = remappedEntityConfigs)
  }

  def getOptconfProviderPaths: Seq[String] = getOptconfProviders.map(_.optconfPath).filter(_ ne null)

  // if same cache key in both optconfs, last one wins
  // otherwise add all cache configs
  private def mergeCacheSections(
      cache1: Map[String, CacheConfig],
      cache2: Map[String, CacheConfig]): mutable.Map[String, CacheConfig] = {
    val mergedCacheEntries: mutable.Map[String, CacheConfig] = mutable.Map.empty
    val cache1Keys = cache1.keySet
    val cache2Keys = cache2.keySet
    val commonCacheNames = cache1Keys.intersect(cache2Keys)
    commonCacheNames.foreach(name => mergedCacheEntries(name) = cache1(name)) // add common caches
    cache1Keys
      .diff(cache2Keys)
      .foreach(addUniqueCacheEntry(_, cache1, mergedCacheEntries)) // add unique caches from conf1
    cache2Keys
      .diff(cache1Keys)
      .foreach(addUniqueCacheEntry(_, cache2, mergedCacheEntries)) // add unique caches from conf2
    mergedCacheEntries
  }

  private def addUniqueCacheEntry(
      nodeKey: String,
      nodeToCacheConfig: Map[String, CacheConfig],
      resultingCaches: mutable.Map[String, CacheConfig]): Unit =
    resultingCaches(nodeKey) = nodeToCacheConfig(nodeKey)

  private def mergeConstructorConfig(
      conf1: Map[String, ConstructorConfig],
      conf2: Map[String, ConstructorConfig]): Map[String, ConstructorConfig] = {
    // start by copying the config from optconf2 to respect the order of priority
    val mergedConfig = mutable.Map(conf2.toSeq: _*)
    conf1.foreach { case (className, config) =>
      // case1: class exists in both optconfs
      if (mergedConfig.contains(className)) {
        val constructorConfigOption = mergedConfig.get(className)
        val mergedPolicy = PropertyConfig.mergeCachePolicy(config, constructorConfigOption.get)
        mergedConfig(className) = ConstructorConfig(config.cachePolicy)
      }
      // case2: constructor config doesn't exist in optconf2 so we take all configuration from optconf1
      else {
        // add entire constructor config from optconf1
        mergedConfig(className) = config
        log.debug(s"constructor config for $className doesn't exist in optconf2")
      }
    }
    mergedConfig.toMap
  }

  private def mergeEntityConfig(conf1: Seq[EntityConfigGroup], conf2: Seq[EntityConfigGroup]): Seq[EntityConfigGroup] =
    (conf1.lastOption, conf2.headOption) match {
      case (_, None) => conf1 // conf2 is empty
      case (None, _) => conf2 // conf1 is empty
      // very common case - the adjacent configs are both PerProperty, so can be merged to make lookup more efficient
      case (Some(pp1: PerEntityConfigGroup), Some(pp2: PerEntityConfigGroup)) if pp1.scopePath == pp2.scopePath =>
        conf1.init ++ (pp1.merge(pp2) +: conf2.tail)
      case (_, _) =>
        conf1 ++ conf2 // regex configs and != scopePath can't be (easily) merged with other configs
    }

  /**
   * applies the configuration specified in optconfProviders
   *
   * Must be always called under lock: this method is only called in the [[optimus.platform.inputs.StateApplicators.StateApplicator]] apply for some of the
   * optconf node inputs and the apply method is only called in ProcessState.setState and ProcessState.reapplyApplicator
   * both of which synchronize on the ProcessState
   */
  @deprecating("Use GraphConfiguration.configureCachePolicies instead")
  private[optimus] def applyConfig(
      optconfProviders: util.List[_ <: OptconfProvider],
      startupProvider: OptconfProvider): Unit = {
    val parsingStart = System.currentTimeMillis()
    val confProviders = optconfProviders.asScala
    // merge the startup optconf on top of the merged originals
    settingsConfig = remapTwkIds(mergeOptconfs(confProviders).merge(NodeCacheConfigsParser.parse(startupProvider)))

    val parsingEnd = System.currentTimeMillis()
    log.info("Parsing optconf file(s) took " + (parsingEnd - parsingStart) + "ms")

    log.info(s"Applying ${optconfProviders.size} optconf(s)")
    log.info(s"Checking if any optconfs are scoped...")
    if (confProviders.exists(_.scopePath.nonEmpty)) {
      confProviders
        .find(_.scopePath.nonEmpty)
        .foreach(provider => log.info(s"Found scoped optconf: [${provider.scopePath}]${provider.optconfPath}"))
    } else log.info(s"No scoped optconfs found.")

    // reapply
    initGlobals()
    reapplyToConstructedEntities()

    val applyEnd = System.currentTimeMillis()
    val timeElapsedForApply = applyEnd - parsingStart
    log.info("Applying optconf file(s) took " + timeElapsedForApply + "ms")

    val optconfPathList = optconfPaths(confProviders)
    Breadcrumbs.send(
      PropertiesCrumb(
        ChainedID.root,
        Properties.appDir -> Option(System.getenv("APP_DIR"))
          .getOrElse(sys.props.getOrElse("user.dir", "unknown")),
        Properties.user -> sys.props.getOrElse("user.name", "unknown"),
        Properties.optconfAction -> "Apply",
        Properties.optconfPath -> optconfPathList,
        Properties.optconfApplyTimeElapsed -> timeElapsedForApply
      ))
    log.info(s"Optconf paths: $optconfPathList")
    val nodeCountByEntity = settingsConfig.entityConfigs.flatMap {
      case p: PerEntityConfigGroup => p.entityToConfig.values.map(_.propertyToConfig.size)
      case _                       => Nil
    }
    log.info(
      s"Typesafe property settings configuration reloaded. Configs: ${settingsConfig.entityConfigs.size} " +
        s"Caches: ${settingsConfig.cacheConfigs.size} Constructors: ${settingsConfig.constructorConfigs.size} " +
        s"Classes: ${nodeCountByEntity.size} Nodes: ${nodeCountByEntity.sum}")
  }

  def applyScopeDependent(optconfProvider: OptconfProvider): Unit = {
    val newConfig = NodeCacheConfigsParser.parse(optconfProvider)
    val scopePath = optconfProvider.scopePath
    initGlobals()
    val newEntityCfg = newConfig.entityConfigs
    val newEntityConfigs = settingsConfig.entityConfigs.filterNot(_.scopePath == scopePath) ++ newEntityCfg
    settingsConfig = settingsConfig.copy(entityConfigs = newEntityConfigs)
    reapplyToConstructedEntities()
  }

  private def optconfPaths(optconfProviders: Iterable[OptconfProvider]): String =
    optconfProviders.map(_.optconfPath).filter(_ ne null).mkString(",")

  private[config] def mergeOptconfs(optconfProviders: Iterable[OptconfProvider]): NodeCacheConfigs = {
    optconfProviders.filter(_.nonEmpty).foldLeft(NodeCacheConfigs.empty) { (acc, p) =>
      val second = NodeCacheConfigsParser.parse(p)
      if (Settings.ignorePgoGraphConfig && second.isPgoGen) {
        log.warn(s"PGO generated config file ${p.optconfPath} will be ignored since -Doptimus.config.pgo.ignore is set")
        acc
      } else acc.merge(second)
    }
  }

  /*
   * Undo a run-time optconf application (e.g. to restore the grid engine state at the end of distribution)
    Must be always called under lock: this method is only called in the [[StateApplicator]] reset  for some of the optconf
    node inputs and the apply method is only called in ProcessState.setState which synchronizes on the ProcessState
   */
  @deprecating("Use GraphInputConfiguration.resetCachePolicies")
  @nowarn("msg=10500 optimus.graph.CacheConfigUtils.undoExternalCacheConfig")
  private[optimus] def reset(): Unit = {
    Breadcrumbs.send(
      PropertiesCrumb(
        ChainedID.root,
        Properties.appDir -> Option(System.getenv("APP_DIR"))
          .getOrElse(sys.props.getOrElse("user.dir", "unknown")),
        Properties.user -> sys.props.getOrElse("user.name", "unknown"),
        Properties.optconfAction -> "Reset"
      ))
    log.info("Reset optconf...")
    settingsConfig = empty
    initGlobals()
    CacheConfigUtils.undoExternalCacheConfig()
  }

  val empty = new NodeCacheConfigs(Nil, Map.empty, Map.empty, false, "")

  @volatile private[this] var settingsConfig: NodeCacheConfigs = empty

  private def confProvider(path: String, scopePath: String): OptconfProvider = path match {
    case NodeCacheInfo.ClasspathRegex(classPath) =>
      log.info(s"Optconf exists on $classPath and path: $path")
      OptconfProvider.fromPath(path, scopePath)
    case localPath =>
      log.info(s"Optconf is at path $localPath")
      OptconfProvider
        .readOptconfFile(localPath)
        .map(content => OptconfProvider.fromContent(content, localPath, scopePath))
        .getOrElse(EmptyOptconfProvider)
  }

  private[optimus] def verifyPathAndCreateProvider(path: String): Option[OptconfProvider] = {
    val (scopePath, optconfPath) = path match {
      case NodeCacheInfo.ScopePathRegex(targetPath, optconfPath) => (targetPath, optconfPath)
      case _                                                     => (null, path)
    }
    NodeCacheInfo.verifyConfigPath(optconfPath).map(confProvider(_, scopePath))
  }

  initGlobals()
  private def initGlobals(): Unit = {
    def ensureCache(existing: NodeCCache, name: String, defaultSize: Int): NodeCCache = {
      val config = settingsConfig.cacheConfigs.get(name)
      val size = if (config.nonEmpty) config.get.cacheSizeLimit else defaultSize
      if (existing eq null) {
        val c = new UNodeCache(
          name = name,
          maxSize = size,
          requestedConcurrency = Settings.cacheConcurrency,
          cacheBatchSize = NodeCCache.defaultCacheBatchSize,
          cacheBatchSizePadding = NodeCCache.defaultCacheBatchSizePadding,
          reduceMaxSizeByScopedCachesSize = name == UNodeCache.globalCacheName
        )
        require(c.sharable, s"cache must be sharable '$name'")
        require(config.isEmpty, s"duplicate shared name '$name'")
        settingsConfig = settingsConfig.withCacheConfig(name, CacheConfig.fromCache(c))
        c
      } else {
        if (size != existing.getMaxSize) existing.setMaxSize(size)
        existing
      }
    }

    UNodeCache.global = ensureCache(UNodeCache.global, UNodeCache.globalCacheName, Settings.cacheSize)
    UNodeCache.siGlobal = ensureCache(UNodeCache.siGlobal, UNodeCache.globalSICacheName, Settings.cacheSISize)
  }
  def getCacheConfigs: Map[String, CacheConfig] = settingsConfig.cacheConfigs

  // [SEE_DOT_PARSING] TODO (OPTIMUS-46190): parser uses dots to reference nested config sections (arguably not needed)
  def cleanCacheName(name: String): String = name.replace(".", "_")
  def originalCacheName(name: String): String = name.replace("_", ".")

  private[optimus] def createNamedCache(name: String): NodeCCache = {
    val config = settingsConfig.cacheConfigs.getOrElse(
      name, {
        val newConfig = CacheConfig(name)
        settingsConfig = settingsConfig.withCacheConfig(name, newConfig)
        newConfig
      })
    config.getOrInstantiateCache
  }

  private[optimus] def getCacheByName(name: String): NodeCCache =
    settingsConfig.cacheConfigs
      .get(name)
      .map(_.getOrInstantiateCache)
      .getOrElse(throw new IllegalArgumentException("Requesting cache that does not exist!"))

  def config: NodeCacheConfigs = settingsConfig
  def isEmpty: Boolean = settingsConfig eq empty
  // helper for interop code that expects a String with all optconf contents [ {conf1},{conf2}]
  // NOTE this is not unused!!! See optimus.interop.gsf.ReflectiveGraphConfigInterface.getRawConfig
  def getRawConfigs: String = s"[${getOptconfProviders.map(_.jsonContent).mkString(",")}]"
  def getOptconfProviders: Seq[OptconfProvider] = {
    // technically this will be empty even if they pass in configOverride="" but we disallow that when we parse what
    // they pass in and tell them to use ignoreConfig now
    val configOverride = ProcessGraphInputs.ConfigOverride.currentValueOrThrow().asScala.toList
    if (configOverride.nonEmpty) configOverride
    else ProcessGraphInputs.ConfigGraph.currentValueOrThrow().asScala.toList
  }
  def stripCfgMargin(cfg: String): String = {
    cfg.replace("\n", "").replace(" ", "")
  }
  def entityConfigs: Seq[EntityConfigGroup] = settingsConfig.entityConfigs
  def constructorConfigs: Map[String, ConstructorConfig] = settingsConfig.constructorConfigs
}

private[optimus] sealed trait EntityConfigGroup {
  def configForEntity(entityName: String): Option[NodeConfigGroup]
  def size: Int
  def scopePath: Option[String] = None
}
// currently per entity configs always have per property configs (but this could be changed in future if needed)
private[optimus] final case class PerEntityConfigGroup(
    entityToConfig: Map[String, PerPropertyConfigGroup],
    override val scopePath: Option[String] = None)
    extends EntityConfigGroup {
  override def configForEntity(entityName: String): Option[NodeConfigGroup] = entityToConfig.get(entityName)
  override def size: Int = entityToConfig.size

  def merge(other: PerEntityConfigGroup): PerEntityConfigGroup = {
    val merged = (entityToConfig.keySet ++ other.entityToConfig.keySet).iterator.map { key =>
      val c1 = entityToConfig.getOrElse(key, PerPropertyConfigGroup.empty)
      val c2 = other.entityToConfig.getOrElse(key, PerPropertyConfigGroup.empty)
      key -> c1.merge(c2)
    }
    PerEntityConfigGroup(merged.toMap)
  }

}

// currently regex entity configs always have all-property configs (but this could be changed in future if needed)
private[optimus] final case class RegexEntityConfigGroup(entityRegex: Regex, config: AllPropertyConfigGroup)
    extends EntityConfigGroup {
  override def configForEntity(entityName: String): Option[NodeConfigGroup] =
    if (entityRegex.pattern.matcher(entityName).matches()) Some(config) else None
  override def size: Int = 1
}

private[optimus] sealed trait NodeConfigGroup {
  def configForNode(propertyName: String): Option[NodeConfig]
}
private[config] object PerPropertyConfigGroup {
  val empty: PerPropertyConfigGroup = PerPropertyConfigGroup(Map.empty)
}
private[optimus] final case class PerPropertyConfigGroup(
    propertyToConfig: Map[String, NodeConfig],
    scopePath: String = null)
    extends NodeConfigGroup {
  override def configForNode(propertyName: String): Option[NodeConfig] =
    propertyToConfig.get(propertyName)

  def merge(other: PerPropertyConfigGroup): PerPropertyConfigGroup = {
    if (propertyToConfig.isEmpty) other
    else if (other.propertyToConfig.isEmpty) this
    else {
      val merged = (propertyToConfig.keySet ++ other.propertyToConfig.keySet).iterator.map { key =>
        val c1 = propertyToConfig.getOrElse(key, EmptyNodeConfig.emptyNodeConfig)
        val c2 = other.propertyToConfig.getOrElse(key, EmptyNodeConfig.emptyNodeConfig)
        key -> c1.merge(c2)
      }
      PerPropertyConfigGroup(merged.toMap)
    }
  }
}
private[optimus] final case class AllPropertyConfigGroup(config: NodeConfig) extends NodeConfigGroup {
  def configForNode(propertyName: String): Option[NodeConfig] = Some(config)
}

private[optimus] object NodeCacheConfigsParser {
  private val log = getLogger(this.getClass)
  import CacheProtocols._
  import NodeCacheConfigs._

  def parse(optconfProvider: OptconfProvider): NodeCacheConfigs = parseWithPropertyConfigs(optconfProvider)._1

  def parseWithPropertyConfigs(optconfProvider: OptconfProvider): (NodeCacheConfigs, Map[String, PropertyConfig]) = {
    // ========= Parse Cache Config file =========
    val json: Map[String, JsValue] = {
      val content = optconfProvider.jsonContent
      // drop comments starting with // - note that this doesn't handle // inside " " correctly, but that's not valid
      // syntax in the config files anyway
      // only apply transformation if we need to.
      val jsonContentClean = OptimusConfigParser.cleanConfig(content)
      // see optimus.config.spray.json.ParserInput.apply for implicit conversion from String => ParserInput
      val res = OptimusConfigParser.parse(jsonContentClean).asInstanceOf[JsObject].fields
      val keys = res.keys.toSet
      if (!keys.forall(LegalOutermostFields))
        throw new OptimusConfigParsingException(
          s"Config file can only contain $VERSION, $PGO_GEN and $CACHE section - found: ${keys.filterNot(LegalOutermostFields).mkString(", ")}")
      if (!keys.contains(VERSION))
        throw new OptimusConfigParsingException(s"Missing $VERSION section")
      if (!keys.contains(CACHE))
        throw new OptimusConfigParsingException(s"Missing $CACHE section")
      res
    }

    def checkVersion(): Unit = {
      val ver = json(VERSION).convertTo[Double]
      if (ver != CURRENTVERSION)
        throw new OptimusConfigParsingException(s"$VERSION should be $CURRENTVERSION")
    }
    checkVersion()

    def isPgoGen: Boolean =
      if (json.contains(PGO_GEN)) json(PGO_GEN).convertTo[Boolean] else false

    val cache: Map[String, JsValue] = {
      val res = json(CACHE).asInstanceOf[JsObject].fields
      val invalidSections = res.keys.toSet -- SEC_ALL.toSet
      if (invalidSections.nonEmpty)
        throw new OptimusConfigParsingException(
          s"$CACHE section contains illegal sub-sections: ${invalidSections.mkString(",")}")
      res
    }

    def filterSection(sn: String): Map[String, JsValue] =
      cache.get(sn).map(_.asInstanceOf[JsObject].fields).getOrElse(Map.empty)
    // TODO (OPTIMUS-15654): improve syntax of type safe config to support
    //                                                        global value section
    // val values: Map[String, JsValue] = filterSection(SEC_VALUES)
    val caches: Map[String, JsValue] = filterSection(SEC_CACHES)
    val configs: Map[String, JsValue] = filterSection(SEC_CONFIGS)
    // we need to preserve order within the Classes section so that regex and regular rules are applied in the order
    // that they are written
    val classes: Seq[(String, JsValue)] =
      cache.get(SEC_CLASSES).map(_.asInstanceOf[JsOrderedObject].fields).getOrElse(Nil)
    val constructors: Map[String, JsValue] = filterSection(SEC_CONSTRUCTORS)

    // ========= Convert JsValue to Cache Config related Data Structure ==========
    // TODO (OPTIMUS-15654): improve syntax of type safe config to support
    //                                                        global value section
    // implicit def globalValueFormat[T: JsonFormat] = jsonFormat1(GlobalValueHolder.apply[T])

    // TODO (OPTIMUS-15654): improve syntax of type safe config to support
    //                                                        global value section
    // val globalValues: Map[String, GlobalValueHolder[_]] =
    // values.map{case (name, value) => (name, value.convertTo[GlobalValueHolder[Boolean]])}

    // Section Caches
    // this is mutable because Profiler would add entries into this map after we have already parsed config file
    val cacheConfigs: mutable.Map[String, CacheConfig] = mutable.Map(caches.toSeq: _*).map {
      case (cleanName, value: JsObject) =>
        (
          cleanName, // [SEE_DOT_PARSING]
          JsObject(value.fields + ("name" -> JsString(originalCacheName(cleanName)))).convertTo[CacheConfig]
        )
      case (_, _) => throw new IllegalStateException("CacheConfig should be only represented as a JsObject")
    }

    val usedCacheConfigs: mutable.Set[String] = mutable.Set.empty[String]
    implicit def reference2CacheConfig(ref: ConfigOrReference): CacheConfig = {
      ref match {
        case ref: ConfigurationReference =>
          if (ref.section != SEC_CACHES)
            throw new OptimusConfigParsingException(
              s"$SEC_CONFIGS section can only refer something in $SEC_CACHES section - was ${ref.section} at ${ref.sourceLine}")

          if (cacheConfigs.contains(ref.key)) {
            usedCacheConfigs.add(ref.key)
            cacheConfigs(ref.key)
          } else
            throw new OptimusConfigParsingException(
              s"$SEC_CACHES section doesn't contain cache: ${ref.key} at ${ref.sourceLine}")
        case inline: InlineConfiguration =>
          // it must be a per-property as it cant be a shared one
          CacheConfigProtocol
            .parseCacheConfig(requiresName = false, inline.content.fields)
            .addPerPropertyDefaults()
      }
    }

    def parsePropertyConfig(name: String, fields: Map[String, JsValue]): PropertyConfig = {
      val illegalFields = fields.keys.filterNot(LegalPropertyConfigFields)
      if (illegalFields.nonEmpty)
        throw new OptimusConfigParsingException(
          s"Config '$name' contained illegal fields: ${illegalFields.mkString(",")}")

      val cache: Option[CacheConfig] =
        fields.get("cache").map(_.convertTo[ConfigOrReference]).map(reference2CacheConfig)
      val cachePolicyField = fields.get("cachePolicy").map(_.convertTo[String])
      // Initialising cachePolicy as null to differentiate between config: {} and config:{cachePolicy: BasicPolicy}
      // where the first one keeps cachePolicy as it was and the second one changes cachePolicy to basicPolicy
      // config:{cachePolicy: BasicPolicy} is the same as {reuse: false}
      var cachePolicy: NCPolicy = null
      if (cachePolicyField.nonEmpty) cachePolicy = NCPolicy.forName(cachePolicyField.get)
      val gcnative = fields.get("gcnative").map(_.convertTo[Boolean])
      val syncId = fields.get("syncId").map(_.convertTo[Int])
      val localSlot = fields.get("localSlot").map(_.convertTo[Int])
      val twkID = fields.get("twkID").map(_.convertTo[Int])
      val dependsOnTweakBits = fields.get("depOn").map(_.convertTo[String])
      val trackForInvalidation = fields.get("trackForInvalidation").map(_.convertTo[Boolean])
      val scopePath = optconfProvider.scopePath

      PropertyConfig(
        name,
        cachePolicy,
        cache,
        gcnative,
        syncId,
        localSlot,
        twkID,
        dependsOnTweakBits,
        trackForInvalidation,
        scopePath)
    }

    // Section Configs
    val propertyConfigs: Map[String, PropertyConfig] = configs.map { case (name, value: JsValue) =>
      (name, parsePropertyConfig(name, value.asInstanceOf[JsObject].fields))
    }

    val usedPropertyConfigs: mutable.Set[String] = mutable.Set.empty[String]
    implicit def reference2PropertyConfig(ref: ConfigOrReference): PropertyConfig = {
      ref match {
        case ref: ConfigurationReference =>
          if (ref.section != SEC_CONFIGS)
            throw new OptimusConfigParsingException(
              s"$SEC_CLASSES section can only refer something in $SEC_CONFIGS section - was ${ref.section} at ${ref.sourceLine}")
          if (propertyConfigs.contains(ref.key)) {
            usedPropertyConfigs.add(ref.key)
            propertyConfigs(ref.key)
          } else throw new OptimusConfigParsingException(s"$SEC_CONFIGS section doesn't contain config: ${ref.key}")

        case inline: InlineConfiguration =>
          parsePropertyConfig("", inline.content.fields)
      }
    }

    // disable cache and setting custom cache at the same time is not allowed
    def isIllegalConfig(propConfOpt: Option[PropertyConfig], cacheConfOpt: Option[CacheConfig]): Boolean =
      propConfOpt.isDefined && (propConfOpt.get.cachePolicy eq NCPolicy.DontCache) && // ie. disable cache
        (cacheConfOpt.isDefined || propConfOpt.get.cache.isDefined) // ie. define custom cache

    val entityConfigs: List[EntityConfigGroup] = {
      // mutable data used for performance to build result
      // the result is immutable
      // there is no multi-threaded access in this method
      // and the mutable data does not escape this method
      val configSets = mutable.ListBuffer[EntityConfigGroup]()
      val currentPerPropMap = mutable.Map.empty[String, mutable.Map[String, NodeConfig]]

      def flushPerPropMap(): Unit = if (currentPerPropMap.nonEmpty) {
        val immutableMap = currentPerPropMap.iterator.map { case (k, v) => (k, PerPropertyConfigGroup(v.toMap)) }.toMap
        configSets += PerEntityConfigGroup(immutableMap, optconfProvider.scopePath)
        currentPerPropMap.clear()
      }

      classes.foreach { case (entityName, values: JsValue) =>
        val propList: Map[String, JsValue] = values.asInstanceOf[JsObject].fields
        propList.foreach { case (propertyName, props: JsValue) =>
          val propsMap: Map[String, JsValue] = props.asInstanceOf[JsObject].fields
          val illegalFields = propsMap.keys.filterNot(LegalNodeConfigFields)
          if (illegalFields.nonEmpty)
            throw new OptimusConfigParsingException(
              s"Entity '$entityName' node '$propertyName' contained illegal fields: ${illegalFields.mkString(",")}")

          val config: Option[PropertyConfig] = {
            propsMap.get("config") match {
              case None                  => None
              case Some(value: JsObject) => Some(value.convertTo[ConfigOrReference])
              case Some(_: JsArray) =>
                log.error("List of configs are not supported anymore")
                None
              case unknown =>
                throw new IllegalStateException(
                  s"Unexpected property config $unknown (${unknown.getClass} -- ${unknown.toJson}")
            }
          }
          val cacheConfig: Option[CacheConfig] = propsMap.get("cache").map(_.convertTo[ConfigOrReference])
          if (isIllegalConfig(config, cacheConfig)) {
            val msg =
              "Illegal action: Cannot disable cache and set a custom cache at the same time for the same node."
            throw new OptimusConfigParsingException(msg)
          }
          val tweakConfig: Option[PropertyConfig] = propsMap.get("tweakConfig").map(_.convertTo[ConfigOrReference])
          require(config.nonEmpty || cacheConfig.nonEmpty || tweakConfig.nonEmpty)
          val nc = NodeConfig(config, cacheConfig, tweakConfig, isPgoGen)

          if (entityName.contains("*")) {
            // entity regex; we always require property regex too
            if (propertyName != ".*")
              throw new OptimusConfigParsingException(
                s"""when using an entity regex you must use a property regex of exactly ".*", but found "$propertyName"""")
            flushPerPropMap()
            configSets += RegexEntityConfigGroup(entityName.r, AllPropertyConfigGroup(nc))
          } else if (propertyName.contains("*")) {
            // no entity regex; but property regex is still allowed
            if (propertyName != ".*")
              throw new OptimusConfigParsingException(
                s"""the only supported property regex is ".*", but found "$propertyName"""")
            // not currently optimized - just quote the pattern and store as a separate config set
            flushPerPropMap()
            configSets += RegexEntityConfigGroup(Pattern.quote(entityName).r, AllPropertyConfigGroup(nc))
          } else {
            val propToConfig = currentPerPropMap.getOrElseUpdate(entityName, mutable.Map.empty)
            propToConfig.update(propertyName, propToConfig.get(propertyName).map(_ merge nc).getOrElse(nc))
          }
        }
      }
      flushPerPropMap()
      configSets.toList
    }

    def parseConstructorConfig(name: String, fields: Map[String, JsValue]): ConstructorConfig = {
      val illegalFields = fields.keys.filterNot(LegalEmbeddableCtorConfigFields)
      if (fields.keySet.union(LegalEmbeddableCtorConfigFields).size > LegalEmbeddableCtorConfigFields.size) {
        throw new OptimusConfigParsingException(
          s"Embeddable '$name' in Constructors section contains illegal fields ${illegalFields.mkString(",")}")
      }

      val constructorCache: Option[CacheConfig] =
        fields.get("cache").map(_.convertTo[ConfigOrReference]).map(reference2CacheConfig)
      val cachePolicy = fields.get("cachePolicy").map(_.convertTo[String])
      ConstructorConfig(if (cachePolicy.isEmpty) null else NCPolicy.forName(cachePolicy.get))
    }

    val constructorConfigs: Map[String, ConstructorConfig] =
      constructors.map { case (className: String, value: JsValue) =>
        className -> parseConstructorConfig(className, value.asInstanceOf[JsObject].fields)
      }

    val unusedCcs: Set[String] = caches.keySet.diff(usedCacheConfigs).diff(UNodeCache.globalNames.toSet)
    val unusedPcs: Set[String] = propertyConfigs.keySet.diff(usedPropertyConfigs)
    if (unusedCcs.nonEmpty)
      throw new OptimusConfigParsingException(s"Cache(s) ${unusedCcs.mkString(", ")} defined but never referenced!")
    if (unusedPcs.nonEmpty)
      throw new OptimusConfigParsingException(
        s"Property Config(s) ${unusedPcs.mkString(", ")} defined but never referenced!")

    val nodeCacheConfigs =
      new NodeCacheConfigs(entityConfigs, constructorConfigs, cacheConfigs.toMap, isPgoGen, optconfProvider.toString)
    (nodeCacheConfigs, propertyConfigs)
  }
}

private[optimus] final case class NodeCacheConfigs(
    // Each EntityConfigGroup is either a Map from entity name to node configurations or a single Regex config.
    // Adjacent maps are merged for efficient lookup (see mergeEntityConfig), but regex configs can't easily be merged
    // so are kept separate
    entityConfigs: Seq[EntityConfigGroup],
    constructorConfigs: Map[String, ConstructorConfig],
    cacheConfigs: Map[String, CacheConfig],
    isPgoGen: Boolean,
    source: String) {
  import NodeCacheConfigs._

  private[config] def merge(that: NodeCacheConfigs): NodeCacheConfigs = {
    if (this.isEmpty) that
    else if (that.isEmpty) this
    else {
      val mergedEntityConfigs = mergeEntityConfig(entityConfigs, that.entityConfigs)
      val mergedConstructorConfigs = mergeConstructorConfig(constructorConfigs, that.constructorConfigs)
      val mergedCaches = mergeCacheSections(cacheConfigs, that.cacheConfigs)
      new NodeCacheConfigs(
        mergedEntityConfigs,
        mergedConstructorConfigs,
        mergedCaches.toMap,
        isPgoGen && that.isPgoGen,
        s"$source+${that.source}")
    }
  }

  /** finds all configs for specified entity and property and merges them in order */
  def mergedNodeConfig(entity: String, property: String): NodeConfig = {
    val configs = entityConfigs.flatMap(_.configForEntity(entity))
    configs.flatMap(_.configForNode(property)).foldLeft(EmptyNodeConfig.emptyNodeConfig)(_ merge _)
  }

  private[config] def withCacheConfig(name: String, config: CacheConfig): NodeCacheConfigs =
    copy(cacheConfigs = cacheConfigs.updated(name, config))

  private[config] def isEmpty: Boolean = entityConfigs.isEmpty && constructorConfigs.isEmpty && cacheConfigs.isEmpty

  override def toString: String = s"NodeCacheConfigs[$source]"
}
