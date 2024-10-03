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
package optimus.graph.diagnostics.pgo

import msjava.slf4jutils.scalalog.getLogger
import optimus.config.CacheConfig
import optimus.config.NodeCacheConfigs
import optimus.graph.ConstructorNode
import optimus.graph.NodeTaskInfo
import optimus.graph.cache.Caches
import optimus.graph.cache.NCPolicy
import optimus.graph.cache.NodeCCache
import optimus.graph.diagnostics.PNodeTaskInfo

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.Writer
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer

private[optimus] final case class CacheOffStatistics(countDisabled: Int, missesSaved: Long, cacheTimeSaved: Long) {
  def cacheTimeSavedMs: String = f"${cacheTimeSaved / 1e6}%1.0f"
}

object ConfigWriter {
  private[diagnostics] val log = getLogger("PROFILER")

  val pgoPolicyModes: Seq[PGOMode] = Seq(DisableCache, SuggestXS, SuggestXSFT).sortBy(_.priority)
  val pgoFlagModes: Seq[PGOMode] = Seq(NativeMemory, LocalCacheSlot, SyncID, Untracked).sortBy(_.priority)

  private val allModes: Seq[PGOMode] = pgoPolicyModes ++ pgoFlagModes :+ TweakDependencies

  val uiConfigurableModes: Seq[PGOMode] = allModes.filter(_.isUIConfigurable).sortBy(_.priority)
  val optconfConfigurableModes: Seq[PGOMode] = allModes.filter(_.isExternallyConfigurable).sortBy(_.priority)

  private def pad(count: Int): String = "  " * count

  private[diagnostics] def withWriter[T](configName: String)(f: Writer => T): T = {
    val file = new File(configName)
    file.getParentFile.mkdirs() // in case this contains subdirectories that don't yet exist
    val out = new BufferedOutputStream(new FileOutputStream(file))
    val writer = new OutputStreamWriter(out)
    try { f(writer) }
    finally { writer.close() }
  }

  def apply(out: Writer, pntis: collection.Seq[PNodeTaskInfo], cfg: ConfigWriterSettings): ConfigWriter =
    new ConfigWriter(out, pntis, NodeCacheConfigs.getCacheConfigs.values, cfg)
}

private final case class Config(key: String, value: String)

private[optimus] class ConfigWriter(
    out: Writer,
    allPNTIs: collection.Seq[PNodeTaskInfo],
    sharedCaches: Iterable[CacheConfig],
    cfg: ConfigWriterSettings) {

  import ConfigWriter._

  private val usedCaches: mutable.Set[String] = mutable.Set[String]()
  private val usedPropertConfigs: mutable.Set[String] = mutable.Set[String]()

  // TODO (OPTIMUS-46192): do less work on initialisation, and don't rely on ordering and mutation
  private val pntisToWrite = allPntisToWrite(allPNTIs)

  // sorted by name - so that config file will be smaller and easier to maintain/compare versions
  private val sortedPntisToWrite = withoutConstructorPntis(pntisToWrite).groupBy(_.entityName).toSeq.sortBy(_._1)

  private[diagnostics] def write(isPgoGen: Boolean): Unit = {
    writeVersion()
    writePgoFlag(isPgoGen)

    var cachesSection = generateCachesSection(sharedCaches)
    val sharedConfigs = uniqueSharedConfigs(withoutConstructorPntis(pntisToWrite))
    val constructorSection = generateConstructorSection(ctrPntisToWrite(pntisToWrite))
    var configSection = generateConfigSection(sharedConfigs)
    val classSection = generateClassesSection(sharedConfigs)

    val sharedCacheNames = sharedCaches.map(_.name).toSet
    if (usedCaches != sharedCacheNames)
      cachesSection = generateCachesSection(sharedCaches.filter(cc => usedCaches.contains(cc.name)))

    val propConfigNames = sharedConfigs.keys
    if (usedPropertConfigs != propConfigNames) {
      val usedSharedConfigs = sharedConfigs.filter({ case (k, _) => usedPropertConfigs.contains(k) })
      configSection = generateConfigSection(usedSharedConfigs)
      // don't need to rewrite classes section because if a config isn't used it won't affect classes section
    }

    out.write(cachesSection)
    out.write(configSection)
    out.write(constructorSection)
    out.write(classSection)

    endCachesSection()
    endAndCloseWriter()
  }

  private[diagnostics] def logSummary(): String = {
    val totalClasses = sortedPntisToWrite.size
    val totalNodes = sortedPntisToWrite.flatMap(_._2).size
    DisableCache.logDisableCacheSummary(totalClasses, totalNodes)
  }

  private def writePgoFlag(isPgoGen: Boolean): Unit =
    if (isPgoGen)
      out.write(s"""${pad(1)}"PGO": true,\n""")

  private def writeVersion(): Unit = {
    // The first non-whitespace character should be {
    // Settings depend on this to judge whether we are using new format of config file
    // After we totally deprecated the old syntax, we can no longer have this limitation
    out.write("{\n")
    out.write(s"""${pad(1)}"Version": 1.0,\n""")
  }

  private def autoTrim(cache: NodeCCache, autoTrimCaches: Boolean): Boolean = autoTrimCaches && {
    val equivalentSize = cache.profMaxSizeUsed
    equivalentSize > 0 && equivalentSize < cache.getMaxSize
  }

  private def reconfigureSettings(cc: CacheConfig): CacheConfig = {
    val cache = cc.getOrInstantiateCache
    if (autoTrim(cache, cfg.autoTrimCaches))
      cc.copy(cacheSizeLimit = cache.profMaxSizeUsed)
    else cc
  }

  private def generateCachesSection(sharedCaches: Iterable[CacheConfig]): String = {
    val res = new StringBuilder(s"""${pad(1)}"Cache": {\n${pad(2)}"Caches": {\n""")
    res.append(
      sharedCaches
        .flatMap { cc: CacheConfig =>
          if (cc.sharable) { // [SEE_EXTERNAL_SHARABLE_CACHE] if a cache was externally configured it will end up as a sharable one in this jvm
            val settings = reconfigureSettings(cc)
            Some(s"""${pad(3)}"${NodeCacheConfigs.cleanCacheName(cc.name)}": ${settings.toJsonString}""")
          } else None
        }
        .toList
        .mkString(",\n"))
    res.append(s"\n${pad(2)}},\n").mkString
  }

  private def generateConfigSection(sharedConfigs: Map[String, String]): String = {
    val res = new StringBuilder(s"""${pad(2)}"Configs": {\n""")
    res.append(
      sharedConfigs
        .map { case (k, conf) =>
          s"""${pad(3)}"$k": { $conf }"""
        }
        .toList
        .mkString(",\n"))
    res.append(s"\n${pad(2)}},\n").mkString
  }

  /** de-duplicate any shared config and produce a map for looking up config given a shared key */
  private def uniqueSharedConfigs(pnodesToWrite: collection.Seq[PNodeTaskInfo]): Map[String, String] = {
    // this may contain duplicates
    val configs = pnodesToWrite.flatMap(configForPnti(_))
    // so group by key
    val entriesByKey = configs.groupBy(_.key)
    // then filter on those with count > 1
    val entriesOccurringMoreThanOnce = entriesByKey.filter { case (_, configsForKey) => configsForKey.length > 1 }
    entriesOccurringMoreThanOnce.map { case (key, configsForKey) =>
      // only need the first one in the list, and only need the actual config (we already have the key)
      (key, configsForKey.head.value)
    }
  }

  private def shouldWriteCtrConfig(ctrNodes: collection.Seq[PNodeTaskInfo]): Boolean =
    ctrNodes.nonEmpty && ctrNodes.exists(ctr =>
      configForPnti(ctr, mutable.Set[PGOMode](), referToSharedCache = false, report = false).isDefined)

  private def generateConstructorSection(ctrNodes: collection.Seq[PNodeTaskInfo]): String = {
    val res = new StringBuilder()
    if (shouldWriteCtrConfig(ctrNodes)) {
      res.append(s"""${pad(2)}"Constructors": {\n""")
      val it = ctrNodes.iterator

      // embeddable name -> config (eg "someEmbeddable" -> "cachePolicy": "DontCachePolicy")
      val allCtrConfigs: mutable.Map[String, String] = mutable.Map.empty

      while (it.hasNext) {
        val ctrPnti = it.next()
        val conf = configForPnti(ctrPnti, mutable.Set[PGOMode](), referToSharedCache = false, report = false)
        val allConfigForPnti = ArrayBuffer[String]()
        conf.foreach { config =>
          val value = config.value
          if (value.nonEmpty) allConfigForPnti += value
        }
        if (allConfigForPnti.nonEmpty) {
          allCtrConfigs.put(ctrPnti.name.stripSuffix(ConstructorNode.suffix), allConfigForPnti.mkString(",\n"))
        }
      }
      val ctrConfigAsStr = allCtrConfigs
        .map { case (pntiName, conf) =>
          s"""${pad(4)}"$pntiName": {\n${pad(6)}$conf\n""" +
            s"""${pad(6)}}"""
        }
        .mkString(",\n")

      res.append(ctrConfigAsStr)
      res.append(s"\n${pad(2)}},\n")
    }
    res.mkString
  }

  private def generateClassesSection(sharedConfigs: Map[String, String]): String = {
    val res = new StringBuilder(s"""${pad(2)}"Classes": {\n""")
    val classConfigStrs = ArrayBuffer[String]()
    val it = sortedPntisToWrite.iterator

    while (it.hasNext) {
      val (entityName, properties) = it.next()
      val filtered = properties.filterNot(_.modifier == NodeTaskInfo.NAME_MODIFIER_TWEAK) // [PERF_TWEAK_RHS_CONFIG]
      val sortedProperties = filtered.sortBy(p => p.name + p.modifier)

      val perPropertyConfigStrs = ArrayBuffer[String]()
      val pntisIt = sortedProperties.iterator
      while (pntisIt.hasNext) {
        val pnti = pntisIt.next()
        val allConfigForProperty = ArrayBuffer[String]()
        val modesAlreadyApplied = mutable.Set[PGOMode]() // used to check cache disabled on this node
        var cacheAdded = false
        val conf = configForPnti(pnti, modesAlreadyApplied, referToSharedCache = true, report = true)
        val cacheDisabled = modesAlreadyApplied.contains(DisableCache)
        conf.foreach { config =>
          val key = config.key
          if (sharedConfigs.contains(key)) {
            allConfigForProperty += s""""config": $${ Configs.$key }""" // this config may refer to a shared cache, if any
            usedPropertConfigs.add(key)

            // [BRANCH_1] Shared cache was referenced in duplicated config and has been added
            cacheAdded = addReferenceToSharedCache(pnti, cacheDisabled = cacheDisabled, referToSharedCache = true)
          } else {
            val configForPropertyWithoutCache = configForPnti(pnti, referToSharedCache = false, report = false)
            configForPropertyWithoutCache.foreach { config =>
              val value = config.value
              if (value.nonEmpty) allConfigForProperty += s""""config": {$value}"""
            }

            // config with cache was not duplicated anywhere so wasn't in the shared configs, but we still might want
            // to add a cache reference
            allConfigForProperty += addCache(pnti, cacheDisabled) // [BRANCH_2]
            cacheAdded = true
          }
        }

        // catch the case where configForPnti returned empty because the only config is a custom non-shared cache, we
        // still want to add it. Possibilities are:
        // [BRANCH_1] Shared cache included in more than one config, therefore referred to from shared Configs section
        // [BRANCH_2] Shared cache but not duplicated by any other config
        // [BRANCH_3] Custom non-shared cache
        if (!cacheAdded) allConfigForProperty += addCache(pnti, cacheDisabled) // [BRANCH_3]

        perPropertyConfigStrs += formatConfigForProperty(
          pnti.propertyName /* [SEE_FREEZING_NODE_NAMES] */,
          allConfigForProperty)
      }

      classConfigStrs += formatConfigForClass(entityName, perPropertyConfigStrs)
    }
    res.append(classConfigStrs.filter(_.nonEmpty).mkString(",\n"))
    res.append(s"\n${pad(2)}}")
    res.mkString
  }

  private def formatConfigForProperty(propertyName: String, allConfigForProperty: ArrayBuffer[String]): String = {
    val nonEmptyPropertyConfigs = allConfigForProperty.filter(_.nonEmpty)
    if (nonEmptyPropertyConfigs.nonEmpty) {
      val sep = s",\n${pad(5)}"
      val (start, end) = if (nonEmptyPropertyConfigs.size > 1) (s"\n${pad(5)}", s"\n${pad(4)}") else ("", "")
      s"""${pad(4)}"$propertyName": { ${nonEmptyPropertyConfigs.mkString(start, sep, end)} }"""
    } else ""
  }

  private def formatConfigForClass(entityName: String, perPropertyConfigStrs: ArrayBuffer[String]): String = {
    val nonEmptyPntiStrings = perPropertyConfigStrs.filter(_.nonEmpty)
    if (nonEmptyPntiStrings.nonEmpty)
      s"""${pad(3)}"$entityName": {\n${nonEmptyPntiStrings.mkString(",\n")}\n${pad(3)}}"""
    else ""
  }

  private def endCachesSection(): Unit = out.write(s"\n${pad(1)}}")
  private def endAndCloseWriter(): Unit = {
    out.write("\n}")
    out.close()
  }

  private def cacheConfigForPnti(pnti: PNodeTaskInfo): CacheConfig = {
    def blank = CacheConfig(name = pnti.cacheName)

    if (pnti.nti ne null) {
      val cache = pnti.nti.customCache()
      if (cache ne null) CacheConfig.fromCache(cache)
      else blank
    } else blank
  }

  // [SEE_DOT_PARSING] TODO (OPTIMUS-46190): parser uses dots to reference nested config sections (arguably not needed)
  private def cleanCacheName(pnti: PNodeTaskInfo): String =
    NodeCacheConfigs.cleanCacheName(pnti.cacheName)

  /**
   * Possible outcomes 1: no cache because we're disabling cache on this pnti 2: a reference to a shared cache in the
   * caches section 3: an inlined custom cache config for a non-shared cache
   */
  private def addCache(pnti: PNodeTaskInfo, cacheDisabled: Boolean): String = {
    if (cacheDisabled) ""
    else {
      if ((pnti.cacheName ne null) && trulySharableCache(pnti))
        s""""cache": $${Caches.${cleanCacheName(pnti)}}"""
      else {
        // if no sharable cache, inline whatever custom cache is configured (it won't be in the Caches section)
        val cacheConfig = cacheConfigForPnti(pnti)
        if (cacheConfig.name ne null) // don't bother writing if empty
          s""""cache": ${cacheConfig.toJsonString}"""
        else ""
      }
    }
  }

  // skip non-entity nodes (they can sneak in via Include All Nodes)
  private[optimus] def withoutConstructorPntis(
      allPNodeTasks: collection.Seq[PNodeTaskInfo]): collection.Seq[PNodeTaskInfo] =
    allPNodeTasks.filterNot(_.name.endsWith(ConstructorNode.suffix))

  private[optimus] def allPntisToWrite(allPNodeTasks: collection.Seq[PNodeTaskInfo]): collection.Seq[PNodeTaskInfo] =
    allPNodeTasks
      .filter(configForPnti(_).nonEmpty)
      .filter(_.name.nonEmpty)

  // only constructor nodes
  private[optimus] def ctrPntisToWrite(allPNodeTasks: collection.Seq[PNodeTaskInfo]): collection.Seq[PNodeTaskInfo] =
    allPNodeTasks.filter(_.name.endsWith(ConstructorNode.suffix))

  private def addReferenceToSharedCache(
      pnti: PNodeTaskInfo,
      cacheDisabled: Boolean,
      referToSharedCache: Boolean): Boolean =
    !cacheDisabled && referToSharedCache && sharableCacheFor(pnti)

  private def trulySharableCache(pnti: PNodeTaskInfo): Boolean =
    (Caches.getPropertyCache(pnti.cacheName), Caches.getSharedCache(pnti.cacheName)) match {
      case (Some(_), Some(_)) => false
      case (None, Some(_))    => true
      case _                  => false
    }

  /** Check whether there is a sharable cache that we can refer to from the Caches section */
  private def sharableCacheFor(pnti: PNodeTaskInfo): Boolean =
    (pnti.cacheName ne null) && Caches.getSharedCache(pnti.cacheName).isDefined

  private def hasExternallyConfiguredCache(pnti: PNodeTaskInfo): Boolean = {
    val uiConfigured = pnti.profilerUIConfigured
    val optconfConfiguredCache = cfg.optconfConfiguredCache(pnti)
    val externallyConfigured = uiConfigured || optconfConfiguredCache
    val cache = sharableCacheFor(pnti) || (pnti.nti != null && pnti.nti.customCache != null)
    externallyConfigured && cache
  }

  def TEST_ONLY_cacheDisabledFor(pnti: PNodeTaskInfo): Boolean = {
    val cfg = configForPnti(pnti)
    if (cfg.isEmpty) false
    else cfg.get.value.contains(NCPolicy.DontCache.toString)
  }

  private def configForPnti(
      pnti: PNodeTaskInfo,
      referToSharedCache: Boolean = true,
      report: Boolean = false): Option[Config] = {
    val modesAlreadyApplied = mutable.Set[PGOMode]()
    configForPnti(pnti, modesAlreadyApplied, referToSharedCache, report)
  }

  private def configForPnti(
      pnti: PNodeTaskInfo,
      modesAlreadyApplied: mutable.Set[PGOMode],
      referToSharedCache: Boolean,
      report: Boolean): Option[Config] = {
    val keys = ListBuffer[String]()
    val values = ListBuffer[String]()

    /**
     * Don't want to disable cache if cache is configured through ui or optconf (these win over auto-modes and even over
     * externally configured disabled cache
     */
    val externallyConfiguredCache = hasExternallyConfiguredCache(pnti)

    def applyConfigFrom(modes: Seq[PGOMode], manualConfig: Boolean): Unit = {
      val it = modes.iterator
      while (it.hasNext) {
        val mode = it.next()
        if (!modesAlreadyApplied.contains(mode)) {
          val modeApplies =
            PGOMode.appliesDespitePreviouslyConfigured(mode, modesAlreadyApplied.toSet, externallyConfiguredCache)
          val configure = modeApplies && mode.configure(pnti, manualConfig, cfg.thresholds, report)
          if (configure) {
            modesAlreadyApplied += mode
            val entries = mode.optconfString(pnti)
            entries.foreach { case (key, value) =>
              keys += key
              values += value
            }
          }
        }
      }
    }

    // Firstly, apply any config from UI configurable modes (UI changes are the first to make sure they are respected)
    if (pnti.profilerUIConfigured)
      applyConfigFrom(uiConfigurableModes, manualConfig = true) // should be the policy user clicked + cfg.modes

    // If any auto-pgo modes are passed in to the ConfigWriterSettings, apply them (if caching isn't disabled)
    // it is important to run this step before adding any optconf settings to make sure we disable the cache where
    // it needs to (and disregard in the last step any conflicting settings)
    applyConfigFrom(cfg.modes, manualConfig = false)

    // Next apply config from optconf (if ConfigWriterSettings specify that we should include previous config)
    // this is applied in Debugger UI if we start the app with any optconfs passed
    if (cfg.optconfConfiguredPolicy(pnti))
      applyConfigFrom(optconfConfigurableModes, manualConfig = true)

    // note: referToSharedCache is true when collecting duplicated configs for pntis (see uniqueSharedConfigs) because
    // if the same cache appears in the config for more than one pnti, it is sharable.
    // The second time we call configForPni, we don't include the cache config here (ie, referToSharedCache is false)
    // because there is nothing to reference in the Caches section (the custom cache is presumably not sharable)
    val disabled = modesAlreadyApplied.contains(DisableCache)
    if (addReferenceToSharedCache(pnti, disabled, referToSharedCache)) {
      val cleanName = cleanCacheName(pnti)
      keys += cleanName
      // $$ escaping ${Caches.foo} is part of the output syntax
      values += s""""cache": $${Caches.$cleanName }"""
      usedCaches.add(cleanName)
    }

    if (keys.isEmpty) None else Some(Config(keys.mkString("_"), values.mkString(", ")))
  }
}
