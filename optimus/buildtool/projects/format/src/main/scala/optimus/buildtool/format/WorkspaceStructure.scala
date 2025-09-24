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
package optimus.buildtool.format

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ModuleSetId
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.OrderedElement
import optimus.buildtool.config.ParentId
import optimus.buildtool.config.WorkspaceId
import optimus.buildtool.dependencies.DependencySetId
import optimus.buildtool.dependencies.VariantSetId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.OrderingUtils._
import optimus.platform._

import java.nio.file.Path
import scala.collection.compat._

sealed trait ObtFile {
  def path: RelativePath // path is relative to workspace source root
  def id: ParentId

  def tryWith[A](op: => Result[A]): Result[A] = Result.tryWith(this, 0)(op)
  def tryWith[A](v: ConfigValue)(op: => Result[A]): Result[A] = Result.tryWith(this, v)(op)

  def errorAt(v: ConfigValue, msg: String): Error =
    Error(msg, this, v.origin().lineNumber())

  def warningAt(v: ConfigValue, msg: String): Warning =
    Warning(msg, this, v.origin().lineNumber())

  def failure(v: ConfigValue, msg: String): Failure = Failure(Seq(errorAt(v, msg)))
}

object ObtFile {

  object Loader {
    private final class ApplyLoader(loader: Loader, f: Result[Config] => Result[Config]) extends Loader {
      override def apply(file: ObtFile): Result[Config] = f(loader(file))
      override def exists(file: ObtFile): Boolean = loader.exists(file)
      override def absolutePath(file: ObtFile): Path = loader.absolutePath(file)
    }
    def apply(loader: Loader)(f: Result[Config] => Result[Config]): Loader = new ApplyLoader(loader, f)
  }

  trait Loader {
    @entersGraph def exists(file: ObtFile): Boolean
    @entersGraph def apply(file: ObtFile): Result[Config]
    @entersGraph def absolutePath(file: ObtFile): Path
  }

  object ScanningLoader {
    private final class ApplyScanningLoader(loader: ScanningLoader, f: Result[Config] => Result[Config])
        extends ScanningLoader {
      override def apply(file: ObtFile): Result[Config] = f(loader(file))
      override def exists(file: ObtFile): Boolean = loader.exists(file)
      override def absolutePath(file: ObtFile): Path = loader.absolutePath(file)
      override def files(dir: RelativePath): Seq[RelativePath] = loader.files(dir)
    }
    def apply(loader: ScanningLoader)(f: Result[Config] => Result[Config]): ScanningLoader =
      new ApplyScanningLoader(loader, f)
  }

  trait ScanningLoader extends Loader {
    @entersGraph def files(dir: RelativePath): Seq[RelativePath]
  }

  final case class FilesystemLoader(workspaceSrcRoot: Directory) extends ScanningLoader {
    override def exists(file: ObtFile): Boolean = workspaceSrcRoot.resolveFile(file.path).exists

    override def apply(file: ObtFile): Result[Config] =
      Result.tryWith(file, line = 0)(Success(ConfigFactory.parseFile(absolutePath(file).toFile)))

    override def absolutePath(file: ObtFile): Path = workspaceSrcRoot.resolveFile(file.path).path

    override def files(dir: RelativePath): Seq[RelativePath] =
      Directory.findFiles(workspaceSrcRoot.resolveDir(dir)).map(workspaceSrcRoot.relativize)
  }
}

sealed abstract class TopLevelConfig(pathText: String) extends ObtFile {
  val path: RelativePath = RelativePath(pathText)
  val id: WorkspaceId.type = WorkspaceId
}
object TopLevelConfig {
  val allFiles: Seq[TopLevelConfig] = List(
    WorkspaceDefaults,
    WorkspaceConfig,
    ResolverConfig,
    DependenciesConfig,
    FingerprintsDiffConfig,
    JvmDependenciesConfig,
    MavenDependenciesConfig,
    BuildDependenciesConfig,
    JdkDependenciesConfig,
    BundlesConfig,
    ModuleSetsConfig,
    ApplicationValidation,
    RunConfSubstitutions,
    DockerConfig,
    RulesConfig,
    CppToolchainConfig,
    MischiefConfig,
    PythonConfig,
    RuleFiltersConfig,
  ).sortBy(_.path)

  object empty extends TopLevelConfig("")
}

trait DependencyConfig extends ObtFile

/* WARNING: Addition of new TopLevelConfig require adjusting of [[ TopLevelConfig.allFiles ]] */

object WorkspaceDefaults extends TopLevelConfig("workspace.obt")
object WorkspaceConfig extends TopLevelConfig("conf.obt")
object ResolverConfig extends TopLevelConfig("resolvers.obt")
object DependenciesConfig extends TopLevelConfig("dependencies.obt") with DependencyConfig
object FingerprintsDiffConfig extends TopLevelConfig("config/fingerprints-diff.obt")
object JvmDependenciesConfig extends TopLevelConfig("dependencies/jvm-dependencies.obt") with DependencyConfig
object MavenDependenciesConfig extends TopLevelConfig("dependencies/maven-dependencies.obt") with DependencyConfig
object BuildDependenciesConfig extends TopLevelConfig("dependencies/build-dependencies.obt") with DependencyConfig
object JdkDependenciesConfig extends TopLevelConfig("dependencies/jdk.obt")
object BundlesConfig extends TopLevelConfig("bundles.obt")
object ModuleSetsConfig extends TopLevelConfig("config/module-sets.obt")
object ApplicationValidation extends TopLevelConfig("app-validation.obt")
object RunConfSubstitutions extends TopLevelConfig("runconf-substitutions.obt")
object DockerConfig extends TopLevelConfig("docker.obt")
object RulesConfig extends TopLevelConfig("config/rules/rules.obt")
object CppToolchainConfig extends TopLevelConfig("cpp-toolchains.obt")
object MischiefConfig extends TopLevelConfig(NamingConventions.MischiefConfig)
object PythonConfig extends TopLevelConfig("dependencies/python-dependencies.obt")
object RuleFiltersConfig extends TopLevelConfig("config/rules/filters.obt")

final case class Module(
    id: ModuleId,
    setId: ModuleSetId,
    public: Boolean,
    path: RelativePath,
    owner: Option[String],
    owningGroup: Option[String],
    line: Int
) extends ObtFile
    with OrderedElement[ModuleId]

final case class Bundle(id: MetaBundle, eonId: Option[String], modulesRoot: String, root: Boolean, line: Int)
    extends ObtFile
    with OrderedElement[MetaBundle] {
  def path: RelativePath = RelativePath(s"${id.meta}/${id.bundle}/bundle.obt")
}

final case class ModuleSetConfig(
    path: RelativePath
) extends ObtFile {
  val id: WorkspaceId.type = WorkspaceId
}
object ModuleSetConfig {
  val Root: RelativePath = RelativePath("config/module-sets")
}

trait SetConfig extends DependencyConfig

final case class DependencySetConfig(
    path: RelativePath
) extends ObtFile
    with SetConfig {
  val id: WorkspaceId.type = WorkspaceId
}
object DependencySetConfig {
  val Root: RelativePath = RelativePath("dependencies/dependency-sets")
}

final case class VariantSetConfig(
    path: RelativePath
) extends ObtFile
    with SetConfig {
  val id: WorkspaceId.type = WorkspaceId
}

final case class WorkspaceStructure(
    name: String,
    bundles: Set[Bundle],
    moduleSets: Map[ModuleSetId, ModuleSetDefinition],
    dependencySets: Map[DependencySetId, DependencySetConfig],
    variantSets: Map[VariantSetId, VariantSetConfig]
) {
  val modules: Map[ModuleId, Module] = moduleSets.values.flatMap(ms => ms.modules).map(m => m.id -> m).toMap

  def allFiles: Seq[ObtFile] = TopLevelConfig.allFiles ++ bundles ++ moduleSets.values.map(_.file) ++ modules.values ++
    dependencySets.values ++ variantSets.values

  def lineInBundlesFileFor(file: ObtFile): Option[Int] = file match {
    case m: Module =>
      modules.get(m.id).map(_.line)
    case b: Bundle =>
      bundles.find(_.id == b.id).map(_.line)
    case _ =>
      None
  }

}

object WorkspaceStructure {
  val empty: WorkspaceStructure = WorkspaceStructure("", Set.empty, Map.empty, Map.empty, Map.empty)

  private def loadBundleDef(meta: String, name: String, config: Config): Result[Bundle] = {
    val modulesRoot = if (config.hasPath(Names.ModulesRoot)) config.getString(Names.ModulesRoot) else s"$meta/$name"
    val eonId: Option[String] = config.optionalString(Names.EonId)
    val root = config.optionalBoolean(Names.Root).contains(true)
    val id = MetaBundle(meta, name)
    Success(Bundle(id, eonId, modulesRoot, root, config.origin().lineNumber())).withProblems {
      config.checkExtraProperties(ModuleSetsConfig, Keys.bundleFileDefinition)
    }
  }

  def loadModuleStructure(name: String, loader: ObtFile.ScanningLoader): Result[WorkspaceStructure] =
    BundlesConfig.tryWith {
      val bundlesFileConfig = loader(BundlesConfig)
      val bundles: ResultSeq[Bundle] = for {
        bundlesConfig <- ResultSeq.single(bundlesFileConfig)
        (meta, metaConf) <- ResultSeq(bundlesConfig.nestedWithFilter(BundlesConfig) { case (name, _) =>
          name != Names.ForbiddenDependencies
        })
        (bundleName, bundleConf) <- ResultSeq(metaConf.nested(BundlesConfig))
        bundle <- ResultSeq.single(loadBundleDef(meta, bundleName, bundleConf))
      } yield bundle

      val validatedBundles = bundles
        .withProblems { bundle =>
          val roots = bundle.filter(_.root)
          if (roots.size > 1)
            roots.map { r =>
              Error(
                "Marking multiple bundles as root is forbidden (should only be assigned to workspace repository bundle)",
                BundlesConfig,
                r.line
              )
            }
          else Nil
        }
        .withProblems(bundles => OrderingUtils.checkOrderingIn(BundlesConfig, bundles))
        .value

      def setName(p: RelativePath): String = p.name.stripSuffix(s".${NamingConventions.ObtExt}")

      val dependencyFiles = loader.files(DependencySetConfig.Root)

      val dependencySets =
        Success(dependencyFiles.flatMap { path =>
          val name = setName(path)
          if (name == path.parent.name) Some(DependencySetId(name) -> DependencySetConfig(path))
          else None
        }.toMap)

      val variantSets = Success(dependencyFiles.flatMap { path =>
        val name = setName(path)
        val parentName = path.parent.name
        if (name != parentName && name.startsWith(parentName))
          Some(VariantSetId(name, DependencySetId(parentName)) -> VariantSetConfig(path))
        else None
      }.toMap).withProblems {
        dependencyFiles
          .flatMap { path =>
            val name = setName(path)
            val parentName = path.parent.name
            if (!name.startsWith(parentName))
              Some(
                Error(
                  s"Invalid dependency/variant set name '$name' for dependency set '$parentName'",
                  VariantSetConfig(path),
                  0))
            else if (!dependencyFiles.contains(path.parent.resolvePath(s"$parentName.${NamingConventions.ObtExt}")))
              Some(Error(s"No dependency set '$parentName' found for variant set '$name'", VariantSetConfig(path), 0))
            else None
          }
      }

      for {
        bundles <- validatedBundles
        depSets <- dependencySets
        varSets <- variantSets
        moduleSets <- ModuleSetDefinition.load(loader, bundles, depSets.keySet, varSets.keySet)
      } yield WorkspaceStructure(
        name,
        bundles.toSet,
        moduleSets.map { moduleSet => moduleSet.id -> moduleSet }.toMap,
        depSets,
        varSets
      )
    }

  def loadAllModulesOrFail(workspaceRoot: Directory): Array[Module] = {
    val workspace = loadModuleStructure("<unused>", ObtFile.FilesystemLoader(workspaceRoot))

    workspace
      .getOrElse(
        throw new RuntimeException(s"Unable to load bundle definitions!:\n ${workspace.errors.mkString("\n")}")
      )
      .modules
      .values
      .toArray
  }
}
