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
package optimus.buildtool.builders.postbuilders.installer.component.fingerprintdiffing

import optimus.buildtool.artifactcomparator.BuildArtifactComparatorDeps
import optimus.buildtool.builders.postbuilders.installer.component.fingerprintdiffing.BuildArtifactComparator.fingerprintDiffs
import optimus.buildtool.builders.postbuilders.installer.component.testplans.GitChanges
import optimus.buildtool.config.FingerprintsDiffConfiguration
import optimus.buildtool.config.ScopeConfigurationSource
import optimus.buildtool.config.ScopeId
import optimus.platform._
import optimus.platform.util.Log
import optimus.scalacompat.collection._
import optimus.utils.TimingUtil
import optimus.workflow.filer.BuildArtifactUtils._
import optimus.workflow.filer._
import optimus.workflow.filer.model.ArtifactsSearchStrategy
import optimus.workflow.filer.model.ScopeFingerprintDiffs
import optimus.workflow.filer.model.SearchStrategy
import optimus.workflow.filer.model.StandardArtifactsSearchStrategy
import optimus.workflow.filer.model.StandardObtWorkspaceSearchStrategy
import optimus.workflow.filer.model.ZipObtWorkspaceSearchStrategy
import optimus.workflow.utils.BuildCommonUtils._
import org.apache.commons.io.FileUtils
import spray.json._

import java.io.BufferedWriter
import java.io.FileWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import scala.jdk.CollectionConverters._
import scala.util.Using

@entity
class BuildArtifactComparator(
    deps: BuildArtifactComparatorDeps,
    scopeConfigSource: ScopeConfigurationSource,
    fingerprintsConfig: FingerprintsDiffConfiguration,
    regCopyLocal: Boolean,
    version: String,
    gitChanges: GitChanges) {

  private val regArtifactsSearchStrategy: ArtifactsSearchStrategy =
    new StandardArtifactsSearchStrategy(None, deps.params.regArtifactsPathOpt)
  private val prBuildPathOpt = deps.params.regArtifactsPathOpt
  private val prObtLogsPathOpt = deps.params.regObtLogsPathOpt
  private val regBuildPath = deps.params.regBuildPath
  private val regSourcePath = deps.params.regSourcePath
  private val prFingerprintsPathOpt =
    Files
      .list(regBuildPath)
      .iterator()
      .asScala
      .map(_.getFileName.toString)
      .find(_.matches("""\d+\.\d+"""))
      .map(regBuildPath.resolve)

  private val regObtWorkspaceSearchStrategy = StandardObtWorkspaceSearchStrategy
  private val basObtWorkspaceSearchStrategy = ZipObtWorkspaceSearchStrategy
  private val jarRegexFilter = Some("\\.jar".r)

  @async def run(): Set[ScopeId] = {
    val prBuildPath = validateFileOpt(prBuildPathOpt)
    validateDirectory(regSourcePath)
    val prFingerprintsPath = validateDirectoryOpt(prFingerprintsPathOpt)
    val prObtLogsPath = validateDirectoryOpt(prObtLogsPathOpt)

    log.info(s"REG Path: $prBuildPath")
    log.info(s"REG Fingerprints Path: $prFingerprintsPath")
    log.info(s"REG OBT Logs Path: $prObtLogsPath")

    val actualPrBuildPath = checkCopyLocal(regCopyLocal, regArtifactsSearchStrategy, prBuildPath)
    val actualPrFingerprintsPath =
      checkCopyLocal(copyLocal = regCopyLocal, regObtWorkspaceSearchStrategy, prFingerprintsPath)
    val actualPrObtLogsPath =
      checkCopyLocal(copyLocal = regCopyLocal, regObtWorkspaceSearchStrategy, prObtLogsPath)

    runDiff(
      actualPrBuildPath,
      deps.params.actualBaselineFingerprintsPath,
      actualPrFingerprintsPath,
      deps.params.actualBaselineObtLogsPath,
      actualPrObtLogsPath
    )
  }

  @async private def runDiff(
      actualPrBuildPath: Path,
      actualBaselineFingerprintsPath: Path,
      actualPrFingerprintsPath: Path,
      actualBaselineObtLogsPath: Path,
      actualPrObtLogsPath: Path
  ): Set[ScopeId] = {
    log.info("Fetching LHS and RHS fingerprints...")
    val (compilationPaths, runconfPaths) = TimingUtil.timeThis("fetch LHS and RHS fingerprints") {
      val basObtLogs = basObtWorkspaceSearchStrategy.logsObtPath(actualBaselineObtLogsPath)
      val regObtLogs = regObtWorkspaceSearchStrategy.logsObtPath(actualPrObtLogsPath)
      val compilation = FingerprintPaths(
        basObtWorkspaceSearchStrategy.compilationFingerprintPath(actualBaselineFingerprintsPath),
        regObtWorkspaceSearchStrategy.compilationFingerprintPath(actualPrFingerprintsPath),
        basObtLogs,
        regObtLogs
      )
      val runconf = FingerprintPaths(
        basObtWorkspaceSearchStrategy.runconfFingerprintPath(actualBaselineFingerprintsPath),
        regObtWorkspaceSearchStrategy.runconfFingerprintPath(actualPrFingerprintsPath),
        basObtLogs,
        regObtLogs
      )
      (compilation, runconf)
    }

    log.info("Comparing fingerprints...")
    val fingerprints = TimingUtil.timeThis("compare fingerprints") {
      Seq(
        FingerprintDirComparison(compilationPaths),
        FingerprintDirComparison(paths = runconfPaths, throwOnMissing = false))
    }

    log.info("Calculating LHS and RHS jar hashes...")
    val rhsJarHashes = TimingUtil.timeThis("calculate RHS jar hashes") {
      hashBuildArtifacts(actualPrBuildPath, jarRegexFilter, version, regArtifactsSearchStrategy)(
        Set[JarHashingStrategy](ReadJarStoredEntryHashesStrategy))
    }

    log.info(s"Comparing hashes from strategy: $ReadJarStoredEntryHashesStrategy...")
    val (lhs, rhs) = TimingUtil.timeThis(s"compare jars of hash strat $ReadJarStoredEntryHashesStrategy ") {
      (deps.basJarHashes, rhsJarHashes(ReadJarStoredEntryHashesStrategy))
    }

    log.info("Computing diffs...")
    val diffs = TimingUtil.timeThis("compute diffs") {
      fingerprintDiffs(lhs, rhs, fingerprints, ignoredDiffs)
    }

    val runtimeDependencyChangedScopes: Set[ScopeId] = getRuntimeDependencyChangedScopes(diffs, scopeConfigSource)

    deps.params.outputJsonPathOpt match {
      case Some(filePath) => saveJsonReport(filePath, diffs)
      case None           => printFingerprints(diffs)
    }

    val gitScopes = gitChangedScopes()
    if (gitScopes.nonEmpty) {
      log.info(s"[Dynamic Scoping] Falling back to git for scopes: ${gitScopes.mkString(", ")}")
    }

    val changedScopes =
      diffs.map(_.scope).flatMap(FingerprintDependencyAnalyzer.toScopeId) ++ runtimeDependencyChangedScopes ++ gitScopes

    val changedScopesPaths = changedScopes.map(_.properPath)
    deps.params.outputChangedScopesPathOpt match {
      case Some(filePath) => saveChangedScopesReport(filePath, changedScopesPaths)
      case None           => printChangedScopes(changedScopesPaths)
    }

    changedScopes
  }

  private def printFingerprints(diffs: Set[ScopeFingerprintDiffs]): Unit = {

    def generateDiffLine[T](diffs: Set[T]): String = diffs.map(s => s"\t$s").mkString("\n")

    diffs.toSeq
      .sortBy(_.scope)
      .foreach(diff => {
        val jarDiffsReport = generateDiffLine(diff.jarDiffs)
        val fingerprintsReport = generateDiffLine(diff.fingerprintDiffs)
        log.info(
          s"""scope = ${diff.scope} with ${diff.jarDiffs.size} jar diffs and ${diff.fingerprintDiffs.size} fingerprint diffs
             |Jar Diffs:
             |$jarDiffsReport
             |Fingerprint Diffs:
             |$fingerprintsReport""".stripMargin)
      })
  }

  private def printChangedScopes(diffs: Set[String]): Unit = {
    val changedScopes = diffs.toSeq.sorted
    log.info(s"""Total changed scopes: ${changedScopes.size}
                |${changedScopes.mkString("\n")}""".stripMargin)
  }

  private def saveJsonReport(filePath: Path, diffs: Set[ScopeFingerprintDiffs]): Unit =
    writeReport("diffs", filePath, diffs.toSeq.sortBy(_.scope).toJson.prettyPrint)

  private def saveChangedScopesReport(filePath: Path, diffs: Set[String]): Unit =
    writeReport("changed scopes", filePath, diffs.toSeq.sorted.mkString(","))

  private def writeReport(desc: String, filePath: Path, content: String): Unit = {
    log.info(s"Writing fingerprint $desc to ${filePath.toAbsolutePath}...")
    Using(new BufferedWriter(new FileWriter(filePath.toFile))) { writer =>
      writer.write(content)
    }
  }

  // NOTE: Avoid local copy with non-archive on windows due to limitations with checking symbolic links across network
  private def checkCopyLocal(copyLocal: Boolean, strategy: SearchStrategy, path: Path): Path = if (copyLocal) {
    val actualPath = if (Files.isSymbolicLink(path)) Files.readSymbolicLink(path) else path
    val prefix =
      strategy.maybeExtension
        .map(actualPath.getFileName.toString.stripSuffix(_))
        .getOrElse(actualPath.getFileName.toString) + "-"
    if (Files.isDirectory(actualPath)) {
      val tempDir = Files.createTempDirectory(prefix)
      log.info(s"copying $actualPath to $tempDir...")
      TimingUtil.timeThis(s"copy ${actualPath.getFileName} to $tempDir") {
        FileUtils.copyDirectory(actualPath.toFile, tempDir.toFile)
      }
      deleteRecursivelyOnExit(tempDir.toFile)
      tempDir
    } else {
      val tempFile = Files.createTempFile(prefix, strategy.maybeExtension.getOrElse(""))
      log.info(s"copying $actualPath to $tempFile...")
      TimingUtil.timeThis(s"copy ${actualPath.getFileName} to $tempFile") {
        Files.copy(actualPath, tempFile, StandardCopyOption.REPLACE_EXISTING)
      }
      deleteRecursivelyOnExit(tempFile.toFile)
      tempFile
    }
  } else {
    path
  }

  @async private def getRuntimeDependencyChangedScopes(
      diffs: Set[ScopeFingerprintDiffs],
      scopeConfigSource: ScopeConfigurationSource): Set[ScopeId] = {
    log.info("Fetching runtime dependency scopes based on fingerprint diffs...")
    val depAnalyzer = FingerprintDependencyAnalyzer(scopeConfigSource, diffs)

    log.info(s"""Directly changed scopes:
                |${depAnalyzer.directlyChangedScopes.toSeq.sortBy(_.properPath).mkString("\n")}""".stripMargin)

    val changedScopes = depAnalyzer.runtimeScopesThatDependOnDirectlyChangedScopes

    log.info(s"Got total of ${changedScopes.size} runtime changed scopes: ${changedScopes.mkString(",")}")
    changedScopes
  }

  /**
   * Fingerprints do not support all the file types, so we need to fall back to Git
   */
  private def gitChangedScopes(): Set[ScopeId] = {
    def isFingerprintsUnsupportedFile(filePath: Path): Boolean =
      !fingerprintsConfig.supportedExtensions.exists(filePath.getFileName.toString.endsWith) ||
        fingerprintsConfig.unsupportedFiles.exists(filePath.toString.endsWith)

    gitChanges.changesByScope
      .getOrElse(Map.empty)
      .filter { case (_, changedFiles) => changedFiles.exists(isFingerprintsUnsupportedFile) }
      .map { case (scope, _) => scope }
      .toSet
  }

  private def ignoredDiffs = fingerprintsConfig.ignoredDiffs.map(e => FingerprintLineId(e.category, e.id))
}

object BuildArtifactComparator extends Log {

  def fingerprintDiffs(
      lhs: JarToHashes,
      rhs: JarToHashes,
      fingerprints: Seq[FingerprintDirComparison],
      ignoredDiffs: Set[FingerprintLineId] = Set.empty
  ): Set[ScopeFingerprintDiffs] = {
    val differingJars = rhs.keySet.toSeq.sorted
      .map(jarName => JarComparison(jarName, lhs.get(jarName), rhs.get(jarName)))
      .filter(_.diffMode != DiffMode.SAME)
      .flatMap { jarComparison =>
        {
          val diff = jarComparison.contentComparison.filter(_.diffMode != DiffMode.SAME).map(_.path).toSet
          if (diff.nonEmpty) Some(jarComparison.inferredScopeName -> diff) else None
        }
      }
      .toMap

    val differingFingerprints =
      fingerprints
        .flatMap(_.scopes)
        .filter(_.diffMode != DiffMode.SAME)
        .map(fingerprintScope => fingerprintScope.scope -> fingerprintScope.diff(ignoredDiffs))
        .filter { case (_, diff) => diff.nonEmpty }
        .groupBy { case (scope, _) => scope }
        .mapValuesNow(diff => diff.flatMap { case (_, lines) => lines }.toSet)
        .filter { case (_, diff) => diff.exists(_.rhs.isDefined) }

    (differingJars.keys ++ differingFingerprints.keys).toSet.map(scope =>
      ScopeFingerprintDiffs(
        scope,
        differingJars.getOrElse(scope, Set.empty),
        differingFingerprints.getOrElse(scope, Set.empty)))
  }

}
