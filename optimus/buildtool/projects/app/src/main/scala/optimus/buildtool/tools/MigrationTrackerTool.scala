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
  val frontierScope = "optimus.onboarding.scala_2_13_frontier.main"
  val frontierObtFile = "optimus/onboarding/projects/scala_2_13_frontier/scala_2_13_frontier.obt"
  val rulesYaml = Option("auto-build-rules/strato-rules.yaml")
  val allFrontierScope: Option[String] = None
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
      )

    val frontierId = impl.obtConfig.scope(frontierScope)
    val frontierIds = (
      if (cmdLine.useMavenLibs) helper.transitiveInternalDeps(frontierId) - frontierId
      else helper.transitiveInternalDeps(frontierId)
    ).filterNot(_.module == "scala_2_13_frontier").filterNot(id => excludedModules.contains(id.module))

    if (cmdLine.rewriteFrontier) {

      import Ordering.Implicits._
      val frontierEntries = frontierIds.toVector.sortBy(_.elements)
      val frontierObt = cmdLine.workspaceSourceRoot.resolveFile(frontierObtFile)
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

      rewrite(frontierObt, frontierEntries.map(toFrontierRegex).map(indentationStr + _).mkString(",\n"), indentationStr)
      stratoRules.foreach { f =>
        val frontierFiles = frontierObtFile +: frontierEntries.apar.flatMap(id => scopeRoot(id, impl))
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
      val (doneFileCount, doneLineCount) = helper.sumCounts(frontierLocMap.values.toVector)
      val (allFileCount, allLineCount) = helper.sumCounts(allLocMap.values.toVector)
      logCounts("files", doneFileCount, allFileCount)
      logCounts("lines", doneLineCount, allLineCount)
      log.info(separatorStr)

      val doneMainIds = frontierIds.filter(_.isMain)
      val doneMainToTestIds = doneMainIds.apar
        .flatMap(id => impl.obtConfig.tryResolveScopes(id.properPath.stripSuffix(".main") + ".test"))
        .flatten

      def isTestish(id: ScopeId): Boolean = id.isTest || id.module.endsWith("_test")

      val nextModules = (allIds -- frontierIds).toSeq.apar
        .filter { id => helper.directInternalDeps(id).subsetOf(frontierIds) }
        .sortBy { x => (!isTestish(x), x.metaBundle.toString, allLocMap(x)) }

      log.info(s"Next modules to migrate (${nextModules.size}):${nextModules.apar
          .map(id => s"\n${toFrontierRegex(id)}, # ${helper.idLoc(id)._2} loc")
          .mkString}")
      log.info(separatorStr)
      val csv = allIds.toVector
        .sortBy(_.tuple)
        .iterator
        .map { x =>
          s"${x.meta},${x.bundle},${x.module},${x.tpe},${allLocMap(x)._2},${frontierIds.contains(x)}"
        }
        .mkString("\n")
      log.info("meta,bundle,module,tpe,loc,migrated\n" + csv)
    }
  }

  @node def scopeRoot(scopeId: ScopeId, impl: OptimusBuildToolImpl): Seq[String] = {
    val idPaths = impl.obtConfig.scopeConfiguration(scopeId).paths
    val root = idPaths.scopeRoot
    val matchesAFile = Files.isDirectory(root.path) && Files.walk(root.path).iterator().asScala.exists { path =>
      Files.isRegularFile(path) && !path.toString.contains("generated-obt")
    }
    if (matchesAFile) // source code changes and scopeId .obt file changes
      Seq(root.toString + "/.*", idPaths.configurationFile.toString)
    else Seq.empty
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

  @node def idLoc(id: ScopeId): (Int, Long) = {
    sumCounts(definitions(id).configuration.paths.absSourceRoots.apar.map(dirLoc))
  }

  @node def dirLoc(src: Directory): (Int, Long) = sumCounts(
    findSourceFiles(src, "scala").apar.map(f => (1, fileLoc(f))))

  @node def fileLoc(file: FileAsset): Long = IO.using(Files.lines(file.path))(_.count())

  @node def idHasScala(id: ScopeId): Boolean =
    definitions(id).configuration.paths.absSourceRoots.apar.exists(dirHasScala)

  @node def dirHasScala(src: Directory): Boolean = findSourceFiles(src, "scala").nonEmpty
  @node def dirHasJasn(src: Directory): Boolean = findSourceFiles(src, "java").nonEmpty

  @node def findSourceFiles(src: Directory, extension: String): Seq[FileAsset] = {
    if (Files.isDirectory(src.path)) { // e.g. scala_compat has no src/main/scala only scala-2.12/2.13
      Directory.findFilesUnsafe(src, Directory.fileExtensionPredicate(extension))
    } else Nil
  }

  def sumCounts(xs: IterableOnce[(Int, Long)]): (Int, Long) =
    xs.iterator.foldLeft((0, 0L)) { case ((a1, a2), (b1, b2)) => (a1 + b1, a2 + b2) }
}
