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
package optimus.buildtool.generators

import optimus.buildtool.artifacts.ExternalBinaryArtifact
import optimus.buildtool.config.AfsNamingConventions
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyDefinitions
import optimus.buildtool.files.FileAsset
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.ExternalDependencyResolver
import optimus.buildtool.resolvers.MavenUtils
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.Utils
import optimus.platform._

import java.nio.file.Paths

object Platforms {
  final case class Platform(classifier: String)
  val Windows: Platform = Platform("windows-x86_64")
  val Linux: Platform = Platform("linux-x86_64")
  def current: Platform = if (Utils.isWindows) Windows else Linux
}

trait DependencyReference {
  val organisation: String
  val project: String
  val variant: Option[String]

  @node def dependencyDefinition(scope: CompilationScope): DependencyDefinition = {
    scope.externalDependencyResolver.dependencyDefinitions
      .find(d => d.group == organisation && d.name == project && d.variant.map(_.name) == variant)
      .getOrElse {
        val suffix = variant.map(v => s".$v").getOrElse("")
        throw new IllegalArgumentException(s"No central dependency found for $organisation.$project$suffix")
      }
  }
}

trait ResolvableResource {
  @node def resolve(configuration: Map[String, String], scope: CompilationScope): FileAsset
  @node def fingerprint(configuration: Map[String, String], scope: CompilationScope): String
}

final case class MavenExecutable(
    organisation: String,
    project: String,
    classifier: Option[String],
    variant: Option[String] = None,
) extends DependencyReference

trait PortableExecutable extends ResolvableResource {
  val windows: DependencyReference
  val linux: DependencyReference

  @node def configured(config: Map[String, String]): DependencyReference
  @node override def resolve(configuration: Map[String, String], scope: CompilationScope): FileAsset
  @node def fingerprint(configuration: Map[String, String], scope: CompilationScope): String = {
    configured(configuration).dependencyDefinition(scope).withClassifier(None).fingerprint("portable-executable")
  }
}

final case class AfsExecutable(
    organisation: String,
    project: String,
    executablePath: String,
    variant: Option[String]
) extends DependencyReference {

  def file(version: String): FileAsset =
    FileAsset(Paths.get(s"${AfsNamingConventions.AfsDistStr}$organisation/PROJ/$project/$version/$executablePath"))
}

final case class PortableMavenExecutable(
    dependencyResolver: ExternalDependencyResolver,
    dependencyCopier: DependencyCopier,
    windows: MavenExecutable,
    linux: MavenExecutable
) extends PortableExecutable {
  @node override def configured(config: Map[String, String]): MavenExecutable = {
    val (default, platform) = if (Utils.isWindows) (windows, "windows") else (linux, "linux")

    default.copy(
      classifier = config.get(s"$platform.classifier").orElse(default.classifier),
      variant = config.get("variant")
    )
  }

  @node override def resolve(configuration: Map[String, String], scope: CompilationScope): FileAsset = {
    val configuredDependency = configured(configuration)
    val dependencyDefinition = configuredDependency
      .dependencyDefinition(scope)
      .withClassifier(
        configuredDependency.classifier
      ) // We want to force update the classifier anyway, because they may be platform specific

    val res = dependencyResolver
      .resolveDependencies(DependencyDefinitions(Seq(dependencyDefinition), Nil))

    res.resolvedArtifacts.toList match {
      case (exec: ExternalBinaryArtifact) :: Nil =>
        val depCopyExec = dependencyCopier.atomicallyDepCopyFileIfMissing(exec.file)
        MavenUtils.setExecutablePermissions(depCopyExec)
        depCopyExec
      case rest => throw new IllegalStateException(s"There should be only a single artifact downloaded but was $rest")
    }
  }
}

final case class PortableAfsExecutable(windows: AfsExecutable, linux: AfsExecutable) extends PortableExecutable {

  @node def resolve(configuration: Map[String, String], scope: CompilationScope): FileAsset = {
    val thisDependency = configured(configuration)
    val dependencyDefinition = thisDependency.dependencyDefinition(scope)
    thisDependency.file(dependencyDefinition.version)
  }

  /**
   * Returns the AFS executable corresponding to the current platform, modified by whatever is in the config file.
   */
  @node def configured(config: Map[String, String]): AfsExecutable = {
    val (default, platform) = if (Utils.isWindows) (windows, "windows") else (linux, "linux")
    def get(key: String, default: String): String = config.getOrElse(s"$key.$platform", default)

    default.copy(
      organisation = get("execMetaDir", default.organisation),
      project = get("execProjectDir", default.project),
      executablePath = get("execPath", default.executablePath),
      variant = config.get("variant")
    )
  }
}
