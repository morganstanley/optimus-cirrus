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
package optimus.buildtool.bsp

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CompletableFuture
import ch.epfl.scala.bsp4j.BuildTargetIdentifier
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.artifacts.PathingArtifact
import optimus.buildtool.builders.BuildResult
import optimus.buildtool.compilers.CompilationException
import optimus.buildtool.compilers.zinc.ZincIncrementalMode
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.RelaxedIdString
import optimus.buildtool.config.ScopeId.RootScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.format.OutputMappings
import optimus.buildtool.files.Directory
import optimus.buildtool.files.JsonAsset
import optimus.buildtool.trace.ObtTraceListener
import optimus.buildtool.trace.StoreMappings
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.FileDiff
import optimus.graph.CancellationScope
import optimus.platform.annotations.closuresEnterGraph
import optimus.platform._
import optimus.scalacompat.collection._
import spray.json._

import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.util.control.NonFatal

object BspBuilder {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
}

class BspBuilder(
    service: BuildServerProtocolService,
    cancellationScope: CancellationScope,
    listener: ObtTraceListener,
    bspListener: BSPTraceListener
) {
  import BspBuilder.log
  import service._

  object Ops {
    // The extra '/?' is a workaround for dodgy targets of the form
    // "obt://codetree1//optimus/platform/dal_core/main"
    private val TargetFormat = """obt://[^/]*//?([\w\-/]*)""".r

    implicit class BuildTargetOps(id: BuildTargetIdentifier) {
      def asPartialScope: Option[String] = id.getUri match {
        case TargetFormat(s) =>
          Some(s.replace('/', '.'))
        case s =>
          val msg = s"Unexpected target format: $s"
          log.warn(msg)
          listener.warn(msg)
          None
      }

      // This may be unsafe, in that `id` may represent a partial scope rather than a complete one
      def asScopeId: Option[ScopeId] = asPartialScope.map(ScopeId.parse)
    }
  }
  import Ops._

  def compile(
      id: String,
      targets: Seq[BuildTargetIdentifier],
      incremental: Boolean,
      installDir: Option[Directory],
      modifiedFiles: Option[FileDiff]
  ): CompletableFuture[BuildResult] = {

    workspace
      .addTweaks(cancellationScope, listener)(incrementalTweaks(incremental))
      .thenCompose { _ =>
        run {
          listener.startBuild()
          asyncResult {
            val scopeConfigSource = builder().factory.scopeConfigSource
            val scopeIds = scopeConfigSource.compilationScopeIds

            val targetMappings = matchingScopeIds(targets, scopeIds)

            // We set tracer.targetMappings here, so that we can get back the requested target for build notifications.
            bspListener.setTargetMappings(targetMappings)
            // We set modifiedFiles here, so the listener can filter out low-priority messages for unchanged files.
            bspListener.setModifiedFiles(modifiedFiles)

            val result = builder().build(targetMappings.keySet, installerFactory(installDir), modifiedFiles)

            listener.endBuild(success = result.successful)
            listener.traceTask(RootScopeId, StoreMappings) {
              if (result.successful) {
                val compiledArtifacts = result.artifacts
                val compiledScopeIds = Artifact.scopeIds(compiledArtifacts).toSet
                val bundleScopeIds = scopeConfigSource.pathingBundles(compiledScopeIds)
                val bundleArtifacts = compiledArtifacts.collect {
                  case Artifact.InternalArtifact(id, a: PathingArtifact) if bundleScopeIds.contains(id.scopeId) =>
                    id.scopeId.metaBundle -> a
                }.toGroupedMap
                // TODO (OPTIMUS-25798): replace with run configs from bsp
                storeCpMapping(compiledArtifacts, bundleArtifacts)
              }
              storeMessageMapping(result.messageArtifacts)
            }

            bspListener.ensureDiagnosticsReported(result.messageArtifacts)
            result
          }.recover { t =>
            listener.endBuild(false)
            throw t
          }.value
        }
      }
      .exceptionally(handleException)
  }

  def configHash: CompletableFuture[String] = run {
    structureHasher.hash()
  }

  @closuresEnterGraph
  private def run[A](task: => A): CompletableFuture[A] = workspace.run(cancellationScope, listener)(task)

  @async private def incrementalTweaks(incremental: Boolean): Seq[Tweak] = {
    val (newZincMode, newUseIncremental) =
      if (incremental) (incrementalMode.defaultZincIncrementalMode, incrementalMode.defaultUseIncrementalArtifacts)
      else {
        log.debug("Non-incremental mode - will build without incremental artifacts")
        (ZincIncrementalMode.None, false)
      }

    val zincModeTweaks =
      if (incrementalMode.zincIncrementalMode != newZincMode)
        Tweaks.byValue(incrementalMode.zincIncrementalMode := newZincMode)
      else Nil

    val useIncrementalTweaks =
      if (incrementalMode.useIncrementalArtifacts != newUseIncremental)
        Tweaks.byValue(incrementalMode.useIncrementalArtifacts := newUseIncremental)
      else Nil

    zincModeTweaks.toIndexedSeq ++ useIncrementalTweaks
  }

  // Implement our own scope lookup rather than calling scopeConfigSource.resolveScopes so that we can
  // be stricter on substring matches and warn on non-matching scopes rather than throwing.
  // We detect if given scope is a main scope and in such cases we want to build all scopes within the parent module.
  private def matchingScopeIds(
      targets: Seq[BuildTargetIdentifier],
      allScopes: Set[ScopeId]
  ): Map[ScopeId, BuildTargetIdentifier] = {
    val scopes = targets.flatMap { t =>
      // filter out root scope
      t.asPartialScope.filter(_.nonEmpty).toSeq.flatMap { ps =>
        val Seq(meta, bundle, module, tpe) = RelaxedIdString.parts(ps)
        val allScopesMatchingModule = allScopes.filter { s =>
          meta.forall(_ == s.meta) && bundle.forall(_ == s.bundle) && module.forall(_ == s.module)
        }.toIndexedSeq

        val matchingScopes = tpe.fold(allScopesMatchingModule) { tpe =>
          val scopesMatchingTpe = allScopesMatchingModule.filter(_.tpe == tpe)
          scopesMatchingTpe.flatMap { id =>
            findMainScopeId(allScopesMatchingModule, id) match {
              case Some(mainId) if mainId == id => allScopesMatchingModule
              case _                            => Seq(id)
            }
          }.distinct
        }

        if (matchingScopes.isEmpty) log.warn(s"No matching scopes for target: ${t.getUri}")
        matchingScopes.map(s => (s, t))
      }
    }.toMap

    if (targets.nonEmpty && scopes.isEmpty) {
      val msg =
        s"Building entire workspace (no scopes found for build target(s): ${targets.map(_.getUri).mkString(", ")})"
      log.warn(msg)
      listener.warn(msg)
      // if there's more than one invalid target specified, just map the scopes to the first one
      allScopes.map(s => s -> targets.head).toMap
    } else scopes
  }

  private def findMainScopeId(ids: Seq[ScopeId], p: ScopeId): Option[ScopeId] =
    ids.find(_.isMain).orElse {
      if (p.module.endsWith("test")) ids.find(_.tpe == "test")
      else None
    }

  /**
   * Updates the classpath mapping used by IntelliJ to run apps and tests. Note that we update rather than overwrite so
   * that modules built in previous builds but not in this one are not removed from the mapping.
   */
  private def storeCpMapping(artifacts: Seq[Artifact], bundleArtifacts: Map[MetaBundle, Seq[PathingArtifact]]): Unit = {
    val updatedScopes = artifacts.collect { case p: PathingArtifact =>
      val scopeId = p.id.scopeId
      val pathingArtifacts = p +: bundleArtifacts.getOrElse(scopeId.metaBundle, Nil)
      p.id.scopeId.properPath -> pathingArtifacts.map(_.pathingFile.pathString)
    }.toSingleMap

    val plainDest = buildDir.resolveFile(NamingConventions.ClassPathMapping)
    OutputMappings.updateClasspathPlain(plainDest, updatedScopes)
  }

  private val SRC = "/src/"
  private def pathPrefix(filePath: String): Option[String] = {
    val i = filePath.indexOf(SRC)
    if (i > 0)
      Some(filePath.substring(0, i))
    else None
  }
  private object LocatedCompilerMessages {
    def unapply(a: Artifact): Option[(String, CompilerMessagesArtifact)] = a match {
      case ma: CompilerMessagesArtifact =>
        for {
          m <- ma.messages.find(m => m.pos.exists(_.filepath.contains(SRC)))
          p <- m.pos
          prefix <- pathPrefix(p.filepath)
        } yield (prefix, ma)
      case _ => None
    }
  }

  private def storeMessageMapping(artifacts: Seq[MessagesArtifact]): Unit = {
    val dest = buildDir.resolveFile("message-mapping.json").asJson
    val updatedMessagesByScope: Map[String, JsArray] = artifacts
      .collect { case LocatedCompilerMessages(prefix, ms) =>
        prefix -> JsString(ms.messageFile.pathString)
      }
      .toGroupedMap
      .map { case (prefix, artifactPaths) =>
        prefix -> JsArray(artifactPaths: _*)
      }

    writeJsonMappings(dest, outputDir, updatedMessagesByScope)
  }

  private def readJsonMappings(file: JsonAsset): Map[String, JsValue] =
    if (file.exists) {
      try {
        val data = new String(Files.readAllBytes(file.path)).parseJson.asInstanceOf[JsObject]
        data.fields
      } catch {
        case NonFatal(t) =>
          log.warn(s"Failed to read $file", t)
          Map.empty
      }
    } else Map.empty

  private def writeJsonMappings(
      file: JsonAsset,
      versionedDir: Directory,
      updatedMappings: Map[String, JsValue]): Unit = {
    val previousMappings: Map[String, JsValue] = readJsonMappings(file)
    val prefix = versionedDir.toString
    // Remove references to files in the wrong directory (presumably old obt versions)
    val filtered = previousMappings
      .mapValuesNow {
        case files: JsArray =>
          JsArray(files.elements.filter {
            case js: JsString => js.value.startsWith(prefix)
            case _            => false
          })
        // Should never happen...
        case _ =>
          JsArray(Vector.empty)
      }
      .filter { case (_, files) =>
        files.elements.nonEmpty
      }
    val json = JsObject(filtered ++ updatedMappings)
    val content = PrettyPrinter(json).getBytes(StandardCharsets.UTF_8)
    Files.createDirectories(file.parent.path)
    AssetUtils.atomicallyWrite(file, replaceIfExists = true) { p =>
      Files.write(p, content)
    }
  }

  private def handleException(t: Throwable): BuildResult = {
    @tailrec def exceptionMessages(e: Throwable, root: Throwable): Seq[MessagesArtifact] = e match {
      case e: CompilationException =>
        Seq(e.artifact)
      case e if e.getCause != null =>
        exceptionMessages(e.getCause, root)
      case _ =>
        Nil
    }

    val messages = exceptionMessages(t, t)
    bspListener.ensureDiagnosticsReported(messages)
    service.cancelAll(bspListener, t)

    val (translated, errorMessage) = BuildServerProtocolService.translateException("Build failed")(t)
    if (messages.isEmpty) errorMessage match {
      case Some(s) => bspListener.error(s)
      case None    => bspListener.error("Compilation failed", translated)
    }

    BuildResult(translated)
  }
}
