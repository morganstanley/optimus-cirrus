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
package optimus.buildtool.tools

import java.nio.file.Files
import msjava.slf4jutils.scalalog.Logger
import msjava.slf4jutils.scalalog.getLogger
import optimus.buildtool.app._
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.format.ScopeDefinition
import optimus.platform._
import optimus.platform.dal.config.DalEnv
import org.kohsuke.{args4j => args}

import scala.collection.compat._
import scala.util.matching.Regex
import scala.collection.compat._
import scala.jdk.CollectionConverters._

class MigrationTrackerTool extends OptimusBuildToolCmdLine {
  @args.Option(
    name = "--rewriteFrontier",
    required = false,
    usage = ""
  )
  val rewriteFrontier: Boolean = false
}

private[buildtool] object MigrationTrackerTool extends MigrationTrackerToolT {
  def indent: Int = 8
}

private[buildtool] trait TrackerToolParams {
  val frontierScope: String = "optimus.onboarding.scala_2_13_frontier.main"
  val frontierObtFile: Option[String] = Option(
    "optimus/onboarding/projects/scala_2_13_frontier/scala_2_13_frontier.obt")
  val rulesYaml: Option[String] = Option("auto-build-rules/scala-213-rules.yaml")
  val allFrontierScope: Option[String] = None
  val obtFileOnly: Boolean = false
}

private[buildtool] trait MigrationTrackerToolT
    extends OptimusApp[MigrationTrackerTool]
    with OptimusBuildToolAppBase[MigrationTrackerTool]
    with TrackerToolParams {

  override def dalLocation = DalEnv("none")
  override protected val log: Logger = getLogger(this)
  private def tripleQuote(s: String) = {
    val tq = "\"\"\""
    tq + s + tq
  }
  def indent: Int
  def yamlIndent: Int = 12

  private def toFrontierRegex(scopeId: ScopeId) = tripleQuote(scopeId.elements.mkString("\\."))

  @entersGraph override def run(): Unit = {
    val impl: OptimusBuildToolImpl = OptimusBuildToolImpl(cmdLine, NoBuildInstrumentation)
    val helper = MigrationTrackerHelper(impl.obtConfig.scopeDefinitions)

    // Other scala scopes that are frontiers themselves
    // These might be getting picked up transitively by the current scala frontier, but I'm not actually sure
    val excludedModules: Seq[String] =
      Seq(
        "artifactory_frontier",
        "artifactory_sdlc_test",
        "scala_2_13_frontier_werr", // just to be safe..
        "stratosphere-all",
        // used by stratosphere-all, but explicitly claiming it will never have an effect on scala consumer scopes
        "py-scripts",
        // not actually an important scala scope; this pulls in all test scopes, both java and scala alike
        "unit_test_collector",
        "launcher",
        "launcher_utils",

        // TODO (OPTIMUS-65598): Tests tests fail in CI due to a known limitation
        // around python scripts in "minimal upload" mode used for the Scala 2.13 build.
        // Test infra can't run these on Scala 2.13 due to some issue with
        "hypo",
        "app_calcbuilder_test",
        "app_simple_smoke_test",
        "python_interop",
      )

    val frontierId = impl.obtConfig.scope(frontierScope)
    val frontierIds = (
      if (cmdLine.useMavenLibs) helper.transitiveInternalDeps(frontierId) - frontierId
      else helper.transitiveInternalDeps(frontierId)
    ).filterNot(id => excludedModules.contains(id.module) || id.module.endsWith("_bundle"))

    if (cmdLine.rewriteFrontier) {

      import Ordering.Implicits._

      val frontierEntries = {
        if (cmdLine.useMavenLibs) frontierIds
        else // only include scopes that actually contain .scala files in the frontier.
          frontierIds.apar.filter(helper.idHasScala)
      }.toVector.sortBy(_.elements)
      val frontierObt = frontierObtFile.map(f => cmdLine.workspaceSourceRoot.resolveFile(f))
      val stratoRules = rulesYaml.map(f => cmdLine.workspaceSourceRoot.resolveFile(f))
      val indentationStr = " " * indent
      val yamlIndentationStr = " " * yamlIndent
      val yamlEntryMarker = "- ^"

      def rewrite(file: FileAsset, snippet: String, indentStr: String): Unit = {
        val content = Files.readString(file.path)
        val startMarker = "##GENERATED_START##"
        val endMarker = "##GENERATED_END##"
        val regex = s"(?ms)${Regex.quote(startMarker)}.*${Regex.quote(endMarker)}"
        val updated =
          content.replaceAll(regex, Regex.quoteReplacement(startMarker + "\n" + snippet + "\n" + indentStr + endMarker))
        if (updated != content) {
          log.info("Updated " + file.path.toAbsolutePath.toString)
          Files.writeString(file.path, updated)
        } else {
          log.info("No change to " + file.path.toAbsolutePath.toString)
        }
      }

      frontierObt.foreach { f =>
        rewrite(f, frontierEntries.map(toFrontierRegex).map(indentationStr + _).mkString(",\n"), indentationStr)
      }
      stratoRules.foreach { f =>
        val frontierFiles =
          frontierObtFile.toSeq ++ frontierEntries.apar.flatMap(id => scopeRoot(id, impl, obtFileOnly))
        rewrite(
          f,
          frontierFiles.distinct.map(yamlIndentationStr + yamlEntryMarker + _).mkString("\n"),
          yamlIndentationStr)
      }
    } else {
      val separatorStr = "=" * 6
      val absolutelyAllIds = allFrontierScope.fold(impl.obtConfig.compilationScopeIds)(f =>
        helper.transitiveInternalDeps(impl.obtConfig.scope(f)))
      val allIds = absolutelyAllIds.filterNot(id => excludedModules.contains(id.module))
      if (absolutelyAllIds != allIds) {
        log.warn(s"Filtered full list of scopes down: ${allIds.size}/${absolutelyAllIds.size}")
      }
      log.info(separatorStr)
      logCounts("modules", frontierIds.size, allIds.size)
      val allLocMap = allIds.apar.map(id => (id, helper.idLoc(id))).toMap
      val frontierLocMap = allLocMap.filter(x => frontierIds.contains(x._1))
      val doneLangFileLines = helper.sumCounts(frontierLocMap.values.toVector)
      val allLangFileLines = helper.sumCounts(allLocMap.values.toVector)
      logCounts("scala files", doneLangFileLines.scala.files, allLangFileLines.scala.files)
      logCounts("java files", doneLangFileLines.java.files, allLangFileLines.java.files)
      logCounts("scala lines", doneLangFileLines.scala.lines, allLangFileLines.scala.lines)
      logCounts("java lines", doneLangFileLines.java.lines, allLangFileLines.java.lines)
      log.info(separatorStr)

      val doneMainIds = frontierIds.filter(_.isMain)
      val doneMainToTestIds = doneMainIds.apar
        .flatMap(id => impl.obtConfig.tryResolveScopes(id.properPath.stripSuffix(".main") + ".test"))
        .flatten

      def isTestish(id: ScopeId): Boolean = id.isTest || id.module.endsWith("_test")

      val nextModules = (allIds -- frontierIds).toSeq.apar
        .filter { id => helper.directInternalDeps(id).subsetOf(frontierIds) }
        .sortBy { x => (!isTestish(x), x.metaBundle.toString, allLocMap(x).scala.lines) }

      log.info(s"Next modules to migrate (${nextModules.size}):${nextModules.apar
          .map(id => s"\n${toFrontierRegex(id)}, # ${helper.idLoc(id).scala.lines} scala loc")
          .mkString}")
      log.info(separatorStr)
      val csv = allIds.toVector
        .sortBy(_.tuple)
        .iterator
        .map { x =>
          s"${x.meta},${x.bundle},${x.module},${x.tpe},${allLocMap(x).scala.lines},${frontierIds.contains(x)}"
        }
        .mkString("\n")
      log.info("meta,bundle,module,tpe,scala_loc,migrated\n" + csv)
    }
  }

  @node def scopeRoot(scopeId: ScopeId, impl: OptimusBuildToolImpl, configFileOnly: Boolean): Seq[String] = {
    val idPaths = impl.obtConfig.scopeConfiguration(scopeId).paths
    val configFile = idPaths.configurationFile.toString

    if (configFileOnly) {
      Seq(configFile)
    } else {
      val root = idPaths.scopeRoot
      val matchesAFile = Files.isDirectory(root.path) && IO.using(Files.walk(root.path)) { rootPath =>
        rootPath.iterator().asScala.exists { path =>
          Files.isRegularFile(path) && !path.toString.contains("generated-obt")
        }
      }
      if (matchesAFile) // source code changes and scopeId .obt file changes
        Seq(root.toString + "/.*", configFile)
      else Seq.empty
    }

  }

  def logCounts(label: String, done: Long, all: Long): Unit =
    log.info(f"$label: $done%,d / $all%,d (${100.0 * done / all}%.1f%%)")
}

@entity class MigrationTrackerHelper(val definitions: Map[ScopeId, ScopeDefinition]) {
  @node def directInternalDeps(id: ScopeId): Set[ScopeId] = {
    val conf = definitions(id).configuration
    (conf.internalCompileDependencies ++ conf.internalRuntimeDependencies).toSet
  }

  @node def transitiveInternalDeps(id: ScopeId): Set[ScopeId] = {
    Set(id) ++ directInternalDeps(id).apar.flatMap(transitiveInternalDeps)
  }

  @node def idLoc(id: ScopeId): LangFilesLines = {
    LangFilesLines(idLocForExtension(id, "scala"), idLocForExtension(id, "java"))
  }
  @node def idLocForExtension(id: ScopeId, extension: String): FilesLines = {
    definitions(id).configuration.paths.absSourceRoots.apar
      .map(dir => dirLoc(dir, extension))
      .foldLeft(FilesLines.empty)(_ + _)
  }

  @node def dirLoc(src: Directory, extension: String): FilesLines =
    findSourceFiles(src, extension).apar.map(f => FilesLines(1, fileLoc(f))).foldLeft(FilesLines.empty)(_ + _)

  @node def fileLoc(file: FileAsset): Long = IO.using(Files.lines(file.path))(_.count())

  @node def idHasScala(id: ScopeId): Boolean =
    definitions(id).configuration.paths.absSourceRoots.apar.exists(dirHasScala)

  @node def dirHasScala(src: Directory): Boolean = findSourceFiles(src, "scala").nonEmpty
  @node def dirHasJava(src: Directory): Boolean = findSourceFiles(src, "java").nonEmpty

  @node def findSourceFiles(src: Directory, extension: String): Seq[FileAsset] = {
    if (Files.isDirectory(src.path)) { // e.g. scala_compat has no src/main/scala only scala-2.12/2.13
      Directory.findFilesUnsafe(src, Directory.fileExtensionPredicate(extension))
    } else Nil
  }

  def sumCounts(xs: IterableOnce[LangFilesLines]): LangFilesLines =
    xs.iterator.foldLeft(LangFilesLines.empty)(_ + _)
}

final case class FilesLines(files: Int, lines: Long) {
  def +(other: FilesLines): FilesLines = FilesLines(files + other.files, lines + other.lines)
}
object FilesLines {
  val empty = FilesLines(0, 0)
}
final case class LangFilesLines(scala: FilesLines, java: FilesLines) {
  def +(other: LangFilesLines): LangFilesLines = LangFilesLines(scala + other.scala, java + other.java)
}
object LangFilesLines {
  val empty = LangFilesLines(FilesLines.empty, FilesLines.empty)
}
