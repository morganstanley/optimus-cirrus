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

import optimus.graph.DiagnosticSettings
import optimus.graph.NodeTaskInfo
import optimus.graph.cache.NCPolicy
import ConfigWriter.log
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.platform.inputs.registry.ProcessGraphInputs

import scala.collection.mutable

object PGOMode {
  def fromUIOptions(autoSuggestDisableCache: Boolean): Seq[PGOMode] =
    autoSuggestDisableCache match {
      case true  => Seq(DisableCache)
      case false => Seq.empty[PGOMode]
    }

  /** Currently only three modes are supported for auto-pgo. Names match command line argument values for --pgo-mode */
  def fromName(name: String): PGOMode = name.toLowerCase match {
    case "disablecache"                                     => DisableCache
    case "tweakusage" | "suggestxsft"                       => SuggestXSFT
    case "tweakusagewithdependencies" | "tweakdependencies" => TweakDependencies
    case _ => throw new IllegalArgumentException(s"Unrecognised PGOMode $name")
  }

  /**
   * return false if we should not apply this config for a node that has had cache configured by the already-applied
   * modes, either through UI or optconf or another auto-pgo mode, OR if we should not apply the config after an
   * externally configured cache (either through UI or optconf)
   */
  def appliesDespitePreviouslyConfigured(
      targetMode: PGOMode,
      alreadyApplied: Set[PGOMode],
      externallyConfigureCache: Boolean) = {
    // You should never apply another cache policy if the cache policy exists in the first place.
    val alreadyHasCachePolicy = alreadyApplied.exists(p => p == DisableCache || p == SuggestXSFT || p == SuggestXS)
    targetMode match {
      case DisableCache            => !externallyConfigureCache && !alreadyHasCachePolicy
      case SuggestXS | SuggestXSFT => !alreadyHasCachePolicy
      case _                       => true
    }
  }
}

sealed trait PGOMode {

  /**
   * Generate key-value entries for optconf file to represent this config. Most modes just generate one pair, see
   * TweakDependencies for the exception
   */
  def optconf(pnti: PNodeTaskInfo): Seq[(String, String)]

  /**
   * either manually configure (usually reading flags) or auto-configure (based on profiling data and AutoPGOThresholds
   */
  final def configure(pnti: PNodeTaskInfo, manual: Boolean, cfg: AutoPGOThresholds, doReport: Boolean): Boolean = {
    val configured = if (manual) manualConfigure(pnti) else autoConfigure(pnti, cfg)
    if (doReport && configured) report(pnti, manual, cfg)
    configured
  }

  final def configure(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean =
    configure(pnti, manual = false, cfg, doReport = false)

  final def configure(pnti: PNodeTaskInfo): Boolean = manualConfigure(pnti)

  /**
   * return true if PNTI was auto-configured by this mode based on profile data and AutoPGOThresholds - defaults to the
   * manualConfigure fallback for modes that don't do any special logic based on profiling data
   */
  protected def autoConfigure(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean = manualConfigure(pnti)

  /**
   * return true if PNTI is configured externally through UI or optconf (caller should check first) and matches this
   * mode (usually determined by checking flags). All modes must at least override this
   */
  protected def manualConfigure(pnti: PNodeTaskInfo): Boolean

  /**
   * Modes are sorted before being applied so that nodes with cache disabled do not have other policies overriding that
   * decision. Higher priority modes are applied first.
   */
  def priority: Int = 0

  /**
   * If a mode is externally configurable (through optconf) and a pnti is marked as externally configured, apply this
   * mode
   */
  def isExternallyConfigurable: Boolean = false

  /** If a mode is UI configurable and a pnti is marked as UI configured, apply this mode */
  def isUIConfigurable: Boolean = false

  /** custom stats or reporting */
  def report(pnti: PNodeTaskInfo, wasManual: Boolean, cfg: AutoPGOThresholds): Unit =
    if (DiagnosticSettings.explainPgoDecision) CacheFilterTweakUsageDecision.add(pnti, "Other")
}

case object DisableCache extends PGOMode {
  override def priority: Int = 3

  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val key = NCPolicy.DontCache.optconfName
    val value = s""""cachePolicy": "${NCPolicy.DontCache}""""
    Seq((key, value))
  }

  override def manualConfigure(pnti: PNodeTaskInfo): Boolean =
    (pnti.flags & NodeTaskInfo.DONT_CACHE) == NodeTaskInfo.DONT_CACHE

  /** turn off caching if there are no hits, or cache benefit is negative AND hit ratio is low */
  override def autoConfigure(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean = {
    if (pnti.isConstructor()) autoConfigureCtrNode(pnti, cfg)
    else noHits(pnti, cfg) || (noBenefit(pnti, cfg) && lowHitRatio(pnti, cfg))
  }

  // the logic to disable cache for constructor nodes is based on cache hits and misses only
  // anc time is hard to be trusted or reasoned about in this context
  private def autoConfigureCtrNode(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean =
    pnti.cacheHit == 0 || lowHitRatio(pnti, cfg)

  private[diagnostics] def noHits(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean =
    pnti.cacheHit == 0 && pnti.cacheMiss > cfg.neverHitThreshold

  private[diagnostics] def noBenefit(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean =
    pnti.cacheBenefit < cfg.benefitThresholdMs * 1e6

  private[diagnostics] def lowHitRatio(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean = {
    val requestsToCache = pnti.cacheHit + pnti.cacheMiss
    requestsToCache > 0 && (pnti.cacheHit / requestsToCache.toDouble) < cfg.hitRatio
  }

  override def isExternallyConfigurable: Boolean = true
  override def isUIConfigurable: Boolean = true

  override def report(pnti: PNodeTaskInfo, wasManual: Boolean, cfg: AutoPGOThresholds): Unit = {
    val nh = noHits(pnti, cfg)
    val nb = noBenefit(pnti, cfg)
    val lhr = lowHitRatio(pnti, cfg)
    updateStats(pnti, wasManual, nh, nb, lhr)

    if (DiagnosticSettings.explainPgoDecision)
      CacheFilterTweakUsageDecision.add(pnti, "DisableCache", nh, nb, lhr)
  }

  // collect statistics about how many nodes we switched off caching for, and what the benefit was
  private val totalStats = new MutableCacheOffStatistics
  private val preConfiguredStats = new MutableCacheOffStatistics
  private val noHitStats = new MutableCacheOffStatistics
  private val noBenefitStats = new MutableCacheOffStatistics
  private val highRatioStats = new MutableCacheOffStatistics

  private def updateStats(
      pnti: PNodeTaskInfo,
      wasManual: Boolean,
      noHits: Boolean,
      noBenefit: Boolean,
      lowHitRatio: Boolean): Unit = {
    totalStats.update(pnti.cacheMiss, pnti.cacheTime)
    if (wasManual) preConfiguredStats.update(pnti.cacheMiss, pnti.cacheTime)
    else if (noHits) noHitStats.update(pnti.cacheMiss, pnti.cacheTime)
    else if (noBenefit) {
      noBenefitStats.update(pnti.cacheMiss, pnti.cacheTime)
      if (!lowHitRatio)
        highRatioStats.update(pnti.cacheMiss, pnti.cacheTime) // these are the ones we exclude from disabled caching
    }
  }

  def logDisableCacheSummary(totalClasses: Int, totalNodes: Int): String = {
    val summaryHeader = f"Configuration for $totalNodes node(s) on $totalClasses class(es)."
    val summaryFooter =
      f"Caching off for ${totalStats.cnt}, saves ${totalStats.misses} misses and ${totalStats.cacheTime / 1e6}%1.3f ms"
    val cacheHits =
      f"   zero cache hits: ${noHitStats.cnt}, saves ${noHitStats.misses} misses and ${noHitStats.cacheTime / 1e6}%1.3f ms\n"
    val cacheBenefit =
      f"   no cache benefit: ${noBenefitStats.cnt}, saves ${noBenefitStats.misses} misses and ${noBenefitStats.cacheTime / 1e6}%1.3f ms\n"
    val hitRatio =
      f"   high cache hit ratio excludes: ${highRatioStats.cnt}, adds ${highRatioStats.misses} misses and ${highRatioStats.cacheTime / 1e6}%1.3f ms\n"
    val externalConfig = f"   externally configured as non-cacheable: ${preConfiguredStats.cnt}\n"

    val includeHitRatio = if (highRatioStats.cnt > 0) hitRatio else ""
    val includeExternalConfig = if (preConfiguredStats.cnt > 0) externalConfig else ""
    val msg = s"$summaryHeader\n$cacheHits$cacheBenefit$includeHitRatio$includeExternalConfig$summaryFooter"

    log.info(msg)
    s"$summaryHeader$summaryFooter" // to be shown in the profiler UI after saving this config
  }
}

case object SuggestXS extends PGOMode {
  override def priority: Int = 2

  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val key = NCPolicy.XS.optconfName
    val value = s""""cachePolicy": "${NCPolicy.XS}""""
    Seq((key, value))
  }

  override def manualConfigure(pnti: PNodeTaskInfo): Boolean = pnti.getFavorReuseIsXS

  override def isExternallyConfigurable: Boolean = true
  override def isUIConfigurable: Boolean = true
}

case object SuggestXSFT extends PGOMode {
  override def priority: Int = 1

  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val key = NCPolicy.XSFT.optconfName
    val value = s""""cachePolicy": "${NCPolicy.XSFT}""""
    Seq((key, value))
  }

  // corresponds to flag filter $!=!SI!g!R for UI, auto suggest xsft
  override def autoConfigure(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean = {
    pnti.hasCollectedDependsOnTweakMask && cacheConfigIsApplicableFor(pnti) &&
    pnti.cacheHit != pnti.cacheHitTrivial && // don't write the ones with only trivial hits on top-level proxies
    !DisableCache.autoConfigure(pnti, cfg) // don't write the ones with negative cache benefit
  }

  /** don't ever try to enable XSFT on SI, GivenRuntimeEnv, or internal nodes (see NCPolicy.forInfo) */
  private def cacheConfigIsApplicableFor(pnti: PNodeTaskInfo): Boolean =
    pnti.getCacheable && !pnti.isScenarioIndependent && !pnti.isInternal && !pnti.isGivenRuntimeEnv

  /** if default policy is XSFT, then this shouldn't do anything special */
  override def manualConfigure(pnti: PNodeTaskInfo): Boolean =
    !ProcessGraphInputs.EnableXSFT.currentValueOrThrow() && pnti.cachePolicy == NCPolicy.XSFT.favorReuseName

  override def isExternallyConfigurable: Boolean = true
  override def isUIConfigurable: Boolean = true

  override def report(pnti: PNodeTaskInfo, wasManual: Boolean, cfg: AutoPGOThresholds): Unit =
    if (DiagnosticSettings.explainPgoDecision) CacheFilterTweakUsageDecision.add(pnti, "XSFTEnabled")
}

/* show the actual dependencies on a node and corresponding tweak ids */
case object TweakDependencies extends PGOMode {
  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val entries = mutable.ArrayBuffer[(String, String)]()
    if (hasTweakDependenciesWorthWriting(pnti)) {
      val encodedMask = pnti.tweakDependencies.stringEncoded // [CONSIDER_REMAP_COMPRESS_ID_MASKS]
      val key = "depOn_" + encodedMask
      val value = s""""depOn": "$encodedMask""""
      entries += ((key, value))
    }
    if (hasTweakableID(pnti)) {
      val id = pnti.tweakID
      val key = "twkID_" + id
      val value = s""""twkID": $id""" // [CONSIDER_REMAP_COMPRESS_ID_MASKS]
      entries += ((key, value))
    }
    entries
  }

  private def hasTweakableID(pnti: PNodeTaskInfo): Boolean = pnti.isDirectlyTweakable && pnti.tweakID > 1

  private def hasTweakDependenciesWorthWriting(pnti: PNodeTaskInfo): Boolean =
    SuggestXSFT.autoConfigure(pnti, AutoPGOThresholds())

  override protected def manualConfigure(pnti: PNodeTaskInfo): Boolean =
    hasTweakableID(pnti) || hasTweakDependenciesWorthWriting(pnti)

  // deliberately grouped under 'XSFTEnabled'
  override def report(pnti: PNodeTaskInfo, wasManual: Boolean, cfg: AutoPGOThresholds): Unit =
    if (DiagnosticSettings.explainPgoDecision) CacheFilterTweakUsageDecision.add(pnti, "XSFTEnabled")
}

case object NativeMemory extends PGOMode {
  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val key = "gcnative"
    val value = """"gcnative": true"""
    Seq((key, value))
  }

  override def manualConfigure(pnti: PNodeTaskInfo): Boolean = pnti.hasNativeMarker
}

case object LocalCacheSlot extends PGOMode {
  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val localCacheSlot = if (pnti.nti ne null) pnti.nti.localCacheSlot.toString else "null"
    val key = s"local$localCacheSlot"
    val value = s""""localSlot": $localCacheSlot"""
    Seq((key, value))
  }

  /** currently unused so checking settings from optconf will not set it... */
  override def manualConfigure(pnti: PNodeTaskInfo): Boolean = false

  /** ...but if specified in auto-pgo modes, set it */
  override protected def autoConfigure(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds): Boolean = true
}

case object SyncID extends PGOMode {
  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val syncId = if (pnti.nti ne null) pnti.nti.syncID.toString else "null"
    val key = s"sync$syncId"
    val value = s""""syncId": $syncId"""
    Seq((key, value))
  }

  override def manualConfigure(pnti: PNodeTaskInfo): Boolean = pnti.hasSyncIdMarker
}

case object Untracked extends PGOMode {
  override def optconf(pnti: PNodeTaskInfo): Seq[(String, String)] = {
    val key = "untracked"
    val value = """"trackForInvalidation": false"""
    Seq((key, value))
  }

  // Special ScenarioReference and DependencyTrackerRoot tweaks have the dont_track flag, but don't include them in optconf
  override def manualConfigure(pnti: PNodeTaskInfo): Boolean = !pnti.isInternal && !pnti.trackForInvalidation
}

private class MutableCacheOffStatistics {
  private var _cnt = 0
  private var _misses = 0L
  private var _cacheTime = 0L

  def update(miss: Long, time: Long): Unit = {
    _cnt += 1
    _misses += miss
    _cacheTime += time
  }
  def cnt: Int = _cnt
  def misses: Long = _misses
  def cacheTime: Long = _cacheTime

  def toFixed: CacheOffStatistics = CacheOffStatistics(cnt, misses, cacheTime)
}
