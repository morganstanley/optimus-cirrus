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
package optimus.buildtool.compilers.zinc

import java.net.URL
import java.nio.file.Path
import optimus.buildtool.artifacts.Artifact.InternalArtifact
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.JarAsset
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.utils.Reflectors
import optimus.buildtool.utils.StackUtils
import optimus.buildtool.utils.Utils
import optimus.tools.metadata.updater.JsonUpdaterReflectiveStub
import sbt.internal.inc.javac.JavaNoPosition
import sbt.internal.inc.javac.JavaProblem

import scala.collection.immutable.Seq
import scala.util.control.NonFatal
import xsbti.Reporter
import xsbti.Severity

object IsolatedJsonUpdaterClient {
  private val log = msjava.slf4jutils.scalalog.getLogger(this)
  private val PlatformEntityPluginScopeId = ScopeId("optimus", "platform", "entityplugin", "main")

  def convertToClassNames(classes: Seq[ClassInJarVirtualFile]): Seq[String] =
    classes.map(_.clazz.stripSuffix(".class").replace('/', '.'))

  def prepareCompilationErrorMessage(t: Throwable, projectNameOpt: Option[String] = None): String = {
    val rawMsg = StackUtils.oneLineStacktrace(t)
    val projectName = projectNameOpt.map(n => s" [$n]").getOrElse("")
    s"""|An error occurred when updating entity plugin metadata files.
        |As a workaround you can temporarily add a space character (or do another minor change) to another scala file in the same project$projectName and retry compilation.
        |Please contact stratosphere support if the problem persists.
        |--------------------
        |$rawMsg""".stripMargin
  }
}

/**
 * A classloader-isolated client for optimus.tools.metadata.updater.JsonUpdater so that we can call the version in the
 * codebase we are compiling, rather than the version we were compiled with, in case the json format is different.
 *
 * This is used to remove entries from the json files when source files have been deleted but not modified or added (in
 * which case the compiler is not invoked and so the normal logic in OptimusExportInfoComponent doesn't run)
 */
class IsolatedJsonUpdaterClient(reporter: Reporter, scopeId: ScopeId, traceType: MessageTrace) {
  import IsolatedJsonUpdaterClient._

  private val prefix = Utils.logPrefix(scopeId, traceType)

  def deleteEntries(
      deletedClasses: Seq[ClassInJarVirtualFile],
      outputJar: JarAsset,
      pluginArtifacts: Seq[Seq[ClassFileArtifact]],
      scalaJars: Seq[JarAsset]
  ): Unit = {
    resolveEntityPluginClasspath(scalaJars, pluginArtifacts).foreach { classpath =>
      val deletedClassNames = convertToClassNames(deletedClasses)
      try {
        Reflectors
          .biConsumer(classOf[JsonUpdaterReflectiveStub], classpath)
          .accept(outputJar.path, deletedClassNames.toArray)
      } catch {
        case NonFatal(exception) =>
          val msg = prepareCompilationErrorMessage(exception)
          reporter.log(JavaProblem(JavaNoPosition, Severity.Error, msg))
      }
    }
  }

  private def resolveEntityPluginClasspath(
      scalaJars: Seq[JarAsset],
      pluginArtifacts: Seq[Seq[ClassFileArtifact]]
  ): Option[Seq[URL]] =
    findEntityPluginClasspath(pluginArtifacts).map { pluginClasspath =>
      (pluginClasspath ++ scalaJars.map(_.path)).map(_.toUri.toURL)
    }

  private def findEntityPluginClasspath(pluginArtifacts: Seq[Seq[ClassFileArtifact]]): Option[Seq[Path]] = {
    val pluginClasspath = pluginArtifacts.collectFirst {
      case cp if cp.collect { case InternalArtifact(InternalArtifactId(PlatformEntityPluginScopeId, _, _), a) =>
            a
          }.nonEmpty =>
        cp.map(_.path)
    }

    if (pluginClasspath.isEmpty) log.debug(s"${prefix}Entity plugin not found")
    pluginClasspath
  }
}
