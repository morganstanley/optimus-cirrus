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
import optimus.config.PerEntityConfigGroup
import optimus.core.TPDMask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.pgo
import optimus.graph.diagnostics.pgo.CacheFilterTweakUsageDecision.removeZero

import java.util.{List => JList}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

final case class PNTIStats(pnti: PNodeTaskInfo, cfg: AutoPGOThresholds) {
  def name: String = pnti.fullName
  def nodeStart: Long = pnti.start
  def cacheHit: Long = pnti.cacheHit
  def cacheMiss: Long = pnti.cacheMiss
  def cacheBenefit: Long = pnti.cacheBenefit
  def cacheTime: Long = pnti.cacheTime
  val noHits: Boolean = DisableCache.noHits(pnti, cfg)
  val noBenefit: Boolean = DisableCache.noBenefit(pnti, cfg)
  val lowHitRatio: Boolean = DisableCache.lowHitRatio(pnti, cfg)
}

final case class DecisionDifference(private val stats1: PNTIStats, private val stats2: PNTIStats) {
  def getRun(i: Int): PNTIStats = {
    if (i == 1) stats1
    else if (i == 2) stats2
    else throw new IllegalArgumentException("Can only pass in 1 or 2 for i")
  }

  private def getDiff(f: PNTIStats => Long): Long = f(stats1) - f(stats2)

  // this function is used to write this class into a CSV format
  def getRow: String = {
    s""""${stats1.name}","${removeZero(stats1.nodeStart)}","${removeZero(stats2.nodeStart)}","${removeZero(
        getDiff(_.nodeStart))}",""" +
      s""""${removeZero(stats1.cacheHit)}","${removeZero(stats2.cacheHit)}","${removeZero(getDiff(_.cacheHit))}",""" +
      s""""${removeZero(stats1.cacheMiss)}","${removeZero(stats2.cacheMiss)}","${removeZero(
          getDiff(_.cacheMiss))}",""" +
      s""""${removeZero(stats1.cacheBenefit)}","${removeZero(stats2.cacheBenefit)}","${removeZero(
          getDiff(_.cacheBenefit))}",""" +
      s""""${removeZero(stats1.cacheTime)}","${removeZero(stats1.cacheTime)}","${removeZero(
          getDiff(_.cacheTime))}","${stats1.noHits}",""" +
      s""""${stats2.noHits}","${stats1.noBenefit}","${stats2.noBenefit}","${stats1.lowHitRatio}","${stats2.lowHitRatio}""""
  }
}

final case class CacheFilterTweakUsageDecision(
    name: String,
    reasonType: String,
    cacheHit: Long,
    cacheHitTrivial: Int,
    cacheMiss: Long,
    cacheBenefit: Long,
    cacheTime: Long,
    getCacheable: Boolean,
    isScenarioIndependent: Boolean,
    profilerInternal: Boolean,
    isGivenRuntimeEnv: Boolean,
    tweakID: Int,
    tweakDependencies: TPDMask,
    noHits: Boolean = false,
    noBenefit: Boolean = false,
    lowHitRatio: Boolean = false
) {
  val pgoDecisionFields: Seq[Any] = collection.Seq(
    name,
    reasonType,
    cacheHit,
    cacheHitTrivial,
    cacheMiss,
    cacheBenefit,
    cacheTime,
    noHits,
    noBenefit,
    lowHitRatio,
    getCacheable,
    isScenarioIndependent,
    profilerInternal,
    isGivenRuntimeEnv,
    tweakID,
    tweakDependencies
  )

  override def toString: String =
    collection
      .Seq(
        name,
        reasonType,
        cacheHit,
        cacheHitTrivial,
        cacheMiss,
        cacheBenefit,
        cacheTime,
        noHits,
        noBenefit,
        lowHitRatio,
        getCacheable,
        isScenarioIndependent,
        profilerInternal,
        isGivenRuntimeEnv,
        tweakID,
        tweakDependencies
      )
      .map(x => "\"" + x + "\"")
      .mkString(",")
}

object CacheFilterTweakUsageDecision {
  private lazy val log = getLogger(this)
  val headerColumn: String = collection
    .Seq(
      "Name",
      "Type",
      "cacheHit",
      "cacheHitTrivial",
      "cacheMiss",
      "cacheBenefit",
      "cacheTime",
      "No Hits",
      "No Benefit",
      "Low Hit Ratio",
      "getCacheable",
      "isSI",
      "profilerInternal",
      "isGivenRuntimeEnv",
      "tweakableID",
      "tweakDependencies"
    )
    .map(x => "\"" + x + "\"")
    .mkString(",") + "\n"
  val differHeaderColumn: String = collection
    .Seq(
      "Name",
      "NodeStartsRun1",
      "NodeStartsRun2",
      "NodeStartsDiff",
      "CacheHitRun1",
      "CacheHitRun2",
      "CacheHitDiff",
      "CacheMissRun1",
      "CacheMissRun2",
      "CacheMissDiff",
      "CacheBenefitRun1",
      "CacheBenefitRun2",
      "CacheBenefitDiff",
      "CacheTimeRun1",
      "CacheTimeRun2",
      "CacheTimeDiff",
      "NoHitsRun1",
      "NoHitsRun2",
      "NoBenefitRun1",
      "NoBenefitRun2",
      "LowHitRatioRun1",
      "LowHitRatioRun2"
    )
    .map(x => "\"" + x + "\"")
    .mkString(",")
  // TODO (OPTIMUS-46192): do less work on init! This particular call makes debugging hard because breakpoints hit in two places
  val configWriter: ConfigWriter = ConfigWriter(null, collection.Seq.empty, ConfigWriterSettings.default)
  private val cacheFilterDecisiontweakUsageDecisionList: ListBuffer[CacheFilterTweakUsageDecision] = ListBuffer()
  def getList: ListBuffer[CacheFilterTweakUsageDecision] = cacheFilterDecisiontweakUsageDecisionList
  def add(
      pn: PNodeTaskInfo,
      reasonType: String,
      noHits: Boolean = false,
      noBenefit: Boolean = false,
      lowHitRatio: Boolean = false): Unit = {
    cacheFilterDecisiontweakUsageDecisionList +=
      CacheFilterTweakUsageDecision(
        pn.fullName,
        reasonType,
        pn.cacheHit,
        pn.cacheHitTrivial,
        pn.cacheMiss,
        pn.cacheBenefit,
        pn.cacheTime,
        pn.getCacheable,
        pn.isScenarioIndependent,
        pn.isInternal,
        pn.isGivenRuntimeEnv,
        pn.tweakID,
        pn.tweakDependencies,
        noHits,
        noBenefit,
        lowHitRatio
      )
  }
  def removeZero(value: Any): String = if (value == 0) "" else value.toString

  def csvFormat(): String = {
    val str = cacheFilterDecisiontweakUsageDecisionList.map(_.toString)
    headerColumn + str.flatMap(Option[String]).mkString("\n")
  }

  private def nameToPnti(pntis: JList[PNodeTaskInfo]): Map[String, PNodeTaskInfo] =
    pntis.asScala.map { pnti => pnti.fullName -> pnti }.toMap

  final case class PGODiff(
      extra: collection.Seq[PNodeTaskInfo],
      missing: collection.Seq[PNodeTaskInfo],
      extraDisabled: collection.Seq[DecisionDifference],
      missingDisabled: collection.Seq[DecisionDifference]) {

    /** how many of the PNTIs that appears in both runs resulted in different PGO decisions? */
    def numDifferent: Int = extraDisabled.length + missingDisabled.length
  }

  def flatMapOptconf(optconf: PerEntityConfigGroup): collection.Seq[String] = {
    optconf.entityToConfig.flatMap { case (prefix, map) =>
      map.propertyToConfig.map { case (name, _) => prefix + name }
    }.toSeq
  }

  def generateDiffStringForOptconf(
      optconf1: PerEntityConfigGroup,
      optconf2: PerEntityConfigGroup,
      threshold: Int): String = {
    val optconf1WithFullName = flatMapOptconf(optconf1)
    val optconf2WithFullName = flatMapOptconf(optconf2)
    val added = optconf2WithFullName.filterNot(optconf1WithFullName.contains(_))
    val removed = optconf1WithFullName.filterNot(optconf2WithFullName.contains(_))
    val header = """"Added:","Removed:""""
    val comparableCounts = optconf1WithFullName.count(optconf2WithFullName.contains(_))
    checkThreshold(threshold, comparableCounts, added.size + removed.size)
    header + "\n" + added
      .zipAll(removed, """""""", """""""")
      .map { case (added, removed) => s"$added,$removed" }
      .mkString("\n")
  }
  def checkThreshold(threshold: Int, comparableCount: Int, diffCount: Int): Unit = {
    if (threshold > 0 && comparableCount != 0) {
      val proportionChangedOutOfComparable = diffCount / comparableCount
      if (proportionChangedOutOfComparable > (threshold / 100))
        log.warn(
          s"Out of all PNTIs that appeared in both runs, $proportionChangedOutOfComparable% resulted in different PGO decisions")
    }
  }

  def compareRuns(
      first: JList[PNodeTaskInfo],
      second: JList[PNodeTaskInfo],
      threshold: Int,
      cfg: AutoPGOThresholds): PGODiff = {
    val firstRun = nameToPnti(first)
    val secondRun = nameToPnti(second)

    val extra = mutable.ArrayBuffer[PNodeTaskInfo]() // pnti appears in run2 not in run1
    val missing = mutable.ArrayBuffer[PNodeTaskInfo]() // pnti appears in run1 not in run2
    val extraDisabled = mutable.ArrayBuffer[DecisionDifference]() // disabled in run2 not in run1
    val missingDisabled = mutable.ArrayBuffer[DecisionDifference]() // disabled in run1 not in run2

    val allNames = firstRun.keySet ++ secondRun.keySet
    var comparableCount = 0 // keep count of how many appear in both
    allNames.foreach { name =>
      if (firstRun.contains(name) && secondRun.contains(name)) { // appears in both, check if the decision was the same
        comparableCount += 1
        val run1 = firstRun(name)
        val run2 = secondRun(name)
        val (disabledRun1, disabledRun2) = (DisableCache.configure(run1, cfg), DisableCache.configure(run2, cfg))
        if (disabledRun1 != disabledRun2) { // different decisions, get a diff
          val diff = DecisionDifference(pgo.PNTIStats(run1, cfg), pgo.PNTIStats(run2, cfg))
          if (disabledRun1 && !disabledRun2) missingDisabled += diff
          if (!disabledRun1 && disabledRun2) extraDisabled += diff
        }
      } else if (firstRun.contains(name)) missing += firstRun(name) // appears in run1 not in run2
      else extra += secondRun(name) // must appear in second run since that key didn't just come from nowhere
    }

    val pgoDiff = PGODiff(extra, missing, extraDisabled, missingDisabled)
    checkThreshold(threshold, comparableCount, pgoDiff.numDifferent)
    pgoDiff
  }
}
