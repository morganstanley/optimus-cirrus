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
import optimus.buildtool.files.Directory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.OrderingUtils._
import optimus.platform._

import java.nio.file.Path
import scala.collection.immutable.Seq
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
    private final class NoCheckingLoader(f: ObtFile => Result[Config]) extends Loader {
      override def apply(file: ObtFile): Result[Config] = f(file)
      override def exists(file: ObtFile): Boolean = true
      override def absolutePath(file: ObtFile): Path = file.path.path.toAbsolutePath
    }
    def skipFileExistChecking(f: ObtFile => Result[Config]): Loader = new NoCheckingLoader(f)

    private final class OverrideApplyLoader(loader: Loader, f: ObtFile => Result[Config]) extends Loader {
      override def apply(file: ObtFile): Result[Config] = f(file)
      override def exists(file: ObtFile): Boolean = loader.exists(file)
      override def absolutePath(file: ObtFile): Path = loader.absolutePath(file)
    }
    def overrideApply(loader: Loader)(f: ObtFile => Result[Config]): Loader = new OverrideApplyLoader(loader, f)

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

  final case class FilesystemLoader(workspaceSrcRoot: Directory) extends Loader {
    override def exists(file: ObtFile): Boolean = workspaceSrcRoot.resolveFile(file.path).exists

    override def apply(file: ObtFile): Result[Config] =
      Result.tryWith(file, line = 0)(Success(ConfigFactory.parseFile(absolutePath(file).toFile)))

    override def absolutePath(file: ObtFile): Path = workspaceSrcRoot.resolveFile(file.path).path
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
    JvmDependenciesConfig,
    MavenDependenciesConfig,
    BuildDependenciesConfig,
    JdkDependenciesConfig,
    BundlesConfig,
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

/* WARNING: Addition of new TopLevelConfig require adjusting of [[ TopLevelConfig.allFiles ]] */

object WorkspaceDefaults extends TopLevelConfig("workspace.obt")
object WorkspaceConfig extends TopLevelConfig("conf.obt")
object ResolverConfig extends TopLevelConfig("resolvers.obt")
object DependenciesConfig extends TopLevelConfig("dependencies.obt")
object JvmDependenciesConfig extends TopLevelConfig("dependencies/jvm-dependencies.obt")
object MavenDependenciesConfig extends TopLevelConfig("dependencies/maven-dependencies.obt")
object BuildDependenciesConfig extends TopLevelConfig("dependencies/build-dependencies.obt")
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
    path: RelativePath,
    owner: String,
    owningGroup: String,
    line: Int
) extends ObtFile
    with OrderedElement[ModuleId]

final case class Bundle(id: MetaBundle, eonId: Option[String], modulesRoot: String, root: Boolean, line: Int)
    extends ObtFile
    with OrderedElement[MetaBundle] {
  def path: RelativePath = RelativePath(s"${id.meta}/${id.bundle}/bundle.obt")
}

final case class WorkspaceStructure(
    name: String,
    bundles: Set[Bundle],
    modules: Map[ModuleId, Module],
    moduleSets: Map[ModuleSetId, ModuleSetDefinition]
) {
  def allFiles: Seq[ObtFile] = TopLevelConfig.allFiles ++ bundles ++ modules.values

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
  val Empty: WorkspaceStructure = WorkspaceStructure("", Set.empty, Map.empty, Map.empty)

  private def loadBundleDef(meta: String, name: String, config: Config): Result[Bundle] = {
    val modulesRoot = if (config.hasPath(Names.ModulesRoot)) config.getString(Names.ModulesRoot) else s"$meta/$name"
    val eonId: Option[String] = config.optionalString(Names.EonId)
    val root = config.optionalBoolean(Names.Root).contains(true)
    val id = MetaBundle(meta, name)
    Success(Bundle(id, eonId, modulesRoot, root, config.origin().lineNumber()))
  }

  private def genModule(name: String, bundle: Bundle, conf: Config) =
    Result.tryWith(BundlesConfig, conf)(
      Success {
        Module(
          id = bundle.id.module(name),
          path = RelativePath(s"${bundle.modulesRoot}/$name/$name.obt"),
          owner = conf.getString("owner"),
          owningGroup = conf.getString("group"),
          line = conf.root().origin().lineNumber()
        )
      }
    )

  private def checkModulesMavenName(modules: Seq[Module], defaultModules: Seq[Module]): Seq[Error] = {
    val incompatibleModules = modules.groupBy(_.id.scope("main").forMavenRelease.fullModule).filter(_._2.size > 1)

    for {
      (id, modules) <- incompatibleModules.to(Seq)
      m <- modules
    } yield {
      val file = if (defaultModules.contains(m)) BundlesConfig else ModuleSetsConfig
      Error(s"Duplicate (lower-case) module name: '$id'", file, m.line)
    }
  }

  def loadModuleStructure(name: String, loader: ObtFile.Loader): Result[WorkspaceStructure] =
    BundlesConfig.tryWith {
      val bundlesFileConfig = loader(BundlesConfig)
      val bundleAndModulesConf: ResultSeq[(Bundle, Config)] = for {
        bundlesConfig <- ResultSeq.single(bundlesFileConfig)
        (meta, moduleConf) <- ResultSeq(bundlesConfig.nestedWithFilter(BundlesConfig) { case (name, _) =>
          name != Names.ForbiddenDependencies
        })
        (bundleName, bundleConf) <- ResultSeq(moduleConf.nested(BundlesConfig))
        bundle <- ResultSeq.single(loadBundleDef(meta, bundleName, bundleConf))
      } yield bundle -> bundleConf
        .withoutPath(Names.ModulesRoot)
        .withoutPath(Names.ForbiddenDependencies)
        .withoutPath(Names.EonId)
        .withoutPath(Names.Root)

      val validatedBundleAndModulesConf = bundleAndModulesConf.withProblems { bundleAndModules =>
        val roots = bundleAndModules.collect { case (bundle, _) if bundle.root => bundle }
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

      val bundles = validatedBundleAndModulesConf
        .map { case (bundle, _) => bundle }
        .value
        .withProblems(bundles => OrderingUtils.checkOrderingIn(BundlesConfig, bundles))

      val defaultModules = (for {
        (bundle, bundleConfig) <- validatedBundleAndModulesConf
        (name, moduleConfig) <- ResultSeq(bundleConfig.nested(BundlesConfig))
        module <- ResultSeq.single(
          genModule(name, bundle, moduleConfig).withProblems(
            moduleConfig.checkExtraProperties(BundlesConfig, Keys.moduleOwnership)))
      } yield module).value.withProblems(mods => OrderingUtils.checkOrderingIn(BundlesConfig, mods))

      val moduleSets = bundles.flatMap(ModuleSetDefinition.load(loader, _))

      for {
        bundles <- bundles
        defaultMods <- defaultModules
        moduleSets <- moduleSets
        allModules <- Success(
          defaultMods ++ moduleSets.flatMap(_.publicModules) ++ moduleSets.flatMap(_.privateModules)
        ).withProblems(mods => checkModulesMavenName(mods, defaultMods))
      } yield {
        val allModuleSets = if (defaultMods.nonEmpty) {
          moduleSets.find(_.moduleSet.id == ModuleSetId.Default) match {
            case Some(default) =>
              val newDefault = default.copy(publicModules = default.publicModules ++ defaultMods.toSet)
              moduleSets.filter(_.moduleSet.id != ModuleSetId.Default) :+ newDefault
            case None =>
              val newDefault = ModuleSetDefinition(
                id = ModuleSetId.Default,
                publicModules = defaultMods.toSet,
                privateModules = Set.empty,
                canDependOn = Set.empty,
                line = 0
              )
              moduleSets :+ newDefault
          }
        } else moduleSets
        WorkspaceStructure(
          name,
          bundles.toSet,
          allModules.map { module => module.id -> module }.toMap,
          allModuleSets.map { moduleSet => moduleSet.moduleSet.id -> moduleSet }.toMap
        )
      }
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
