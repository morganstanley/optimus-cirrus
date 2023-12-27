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
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.OrderedElement
import optimus.buildtool.config.ParentId
import optimus.buildtool.config.WorkspaceId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ConfigUtils._
import optimus.platform._

import java.nio.file.Path
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.util.matching.Regex
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
    BundlesConfig,
    DockerConfig,
    RulesConfig,
    CppToolchainConfig,
    MavenDependenciesConfig,
    BuildDependenciesConfig,
    JdkDependenciesConfig,
    MischiefConfig,
    PythonConfig
  ).sortBy(_.path)

  object empty extends TopLevelConfig("")
}

object WorkspaceDefaults extends TopLevelConfig("workspace.obt")
object WorkspaceConfig extends TopLevelConfig("conf.obt")
object ResolverConfig extends TopLevelConfig("resolvers.obt")
object DependenciesConfig extends TopLevelConfig("dependencies.obt")
object JvmDependenciesConfig extends TopLevelConfig("dependencies/jvm-dependencies.obt")
object MavenDependenciesConfig extends TopLevelConfig("dependencies/maven-dependencies.obt")
object BuildDependenciesConfig extends TopLevelConfig("dependencies/build-dependencies.obt")
object JdkDependenciesConfig extends TopLevelConfig("dependencies/jdk.obt")
object BundlesConfig extends TopLevelConfig("bundles.obt")
object ApplicationValidation extends TopLevelConfig("app-validation.obt")
object RunConfSubstitutions extends TopLevelConfig("runconf-substitutions.obt")
object DockerConfig extends TopLevelConfig("docker.obt")
object RulesConfig extends TopLevelConfig("rules.obt")
object CppToolchainConfig extends TopLevelConfig("cpp-toolchains.obt")
object MischiefConfig extends TopLevelConfig(NamingConventions.MischiefConfig)
object PythonConfig extends TopLevelConfig("dependencies/python-dependencies.obt")

final case class Module(
    id: ModuleId,
    path: RelativePath,
    owner: String,
    owningGroup: String,
    forbiddenDependencies: Seq[ForbiddenDependency],
    line: Int
) extends ObtFile
    with OrderedElement

final case class Bundle(id: MetaBundle, modulesRoot: String, forbiddenDependencies: Seq[ForbiddenDependency], line: Int)
    extends ObtFile
    with OrderedElement {
  def path: RelativePath = RelativePath(s"${id.meta}/${id.bundle}/bundle.obt")
}

final case class MavenMappingFile(pathStr: String) extends ObtFile {
  val path: RelativePath = RelativePath(pathStr)
  val id: WorkspaceId.type = WorkspaceId
}

final case class ForbiddenDependency(
    name: String,
    configurations: Set[String],
    private val allowedInModules: Set[String],
    private val allowedPatterns: Set[Regex],
    internalOnly: Boolean,
    externalOnly: Boolean) {

  private val regex = name.r

  def matchesExternalDep(dep: DependencyDefinition): Boolean = {
    def configMatch = configurations.isEmpty || configurations.contains(dep.configuration)
    !internalOnly && configMatch && regex.findFirstMatchIn(dep.key).isDefined
  }

  def matchesInternalDep(key: String, module: String): Boolean = {
    def keyMatch = key.startsWith(name) || module.endsWith(name)
    !externalOnly && keyMatch
  }

  def isAllowedIn(moduleName: String): Boolean =
    allowedInModules.contains(moduleName) || allowedPatterns.exists(_.pattern.matcher(moduleName).find)

  /** For use in global-level forbidden dependencies. */
  def isAllowedIn(module: ModuleId): Boolean = isAllowedIn(module.properPath)
}

final case class WorkspaceStructure(name: String, bundles: Seq[Bundle], modules: Map[ModuleId, Module]) {
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
  val Empty: WorkspaceStructure = WorkspaceStructure("", Seq.empty, Map.empty)

  private def loadBundleDef(meta: String, name: String, config: Config): Result[Bundle] = {
    val modulesRoot = if (config.hasPath(Names.modulesRoot)) config.getString(Names.modulesRoot) else s"$meta/$name"
    val id = MetaBundle(meta, name)
    val forbiddenDeps: Result[Seq[ForbiddenDependency]] =
      if (config.hasPath(Names.forbiddenDependencies))
        Result.traverse(config.configs(Names.forbiddenDependencies))(genForbiddenDependency)
      else Success(Seq.empty)
    forbiddenDeps.map { fds =>
      Bundle(id, modulesRoot, fds, config.origin().lineNumber())
    }
  }

  private def genForbiddenDependency(conf: Config): Result[ForbiddenDependency] = {
    val name = conf.getString(Names.name)
    val configurations =
      if (conf.hasPath(Names.configurations)) conf.getStringList(Names.configurations).asScala.toSet
      else Set.empty[String]
    val allowedInModules =
      if (conf.hasPath(Names.allowedIn)) conf.getStringList(Names.allowedIn).asScala.toSet
      else Set.empty[String]
    val allowedPatterns: Set[Regex] =
      if (conf.hasPath(Names.allowedPatterns)) conf.getStringList(Names.allowedPatterns).asScala.map(_.r).toSet
      else Set.empty[Regex]
    val internalOnly = if (conf.hasPath(Names.internalOnly)) conf.getBoolean(Names.internalOnly) else false
    val externalOnly = if (conf.hasPath(Names.externalOnly)) conf.getBoolean(Names.externalOnly) else false
    Success(ForbiddenDependency(name, configurations, allowedInModules, allowedPatterns, internalOnly, externalOnly))
      .withProblems(conf.checkExtraProperties(BundlesConfig, Keys.forbiddenDependencyKeys))
  }

  private def genModule(name: String, bundle: Bundle, conf: Config) =
    Result.tryWith(BundlesConfig, conf)(
      Success {
        Module(
          id = bundle.id.module(name),
          path = RelativePath(s"${bundle.modulesRoot}/$name/$name.obt"),
          owner = conf.getString("owner"),
          owningGroup = conf.getString("group"),
          forbiddenDependencies = bundle.forbiddenDependencies.filterNot(fd => fd.isAllowedIn(moduleName = name)),
          line = conf.root().origin().lineNumber()
        )
      }
    )

  def loadModuleStructure(name: String, loader: ObtFile.Loader): Result[WorkspaceStructure] =
    BundlesConfig.tryWith {
      val bundlesFileConfig = loader(BundlesConfig)
      val bundleAndModulesConf: ResultSeq[(Bundle, Config)] = for {
        bundlesConfig <- ResultSeq.single(bundlesFileConfig)
        (meta, moduleConf) <- ResultSeq(bundlesConfig.nestedWithFilter(BundlesConfig) { case (name, _) =>
          name != Names.forbiddenDependencies
        })
        (bundleName, bundleConf) <- ResultSeq(moduleConf.nested(BundlesConfig))
        bundle <- ResultSeq.single(loadBundleDef(meta, bundleName, bundleConf))
      } yield bundle -> bundleConf.withoutPath(Names.modulesRoot).withoutPath(Names.forbiddenDependencies)

      val globalForbiddenDependencies: Result[Seq[ForbiddenDependency]] = for {
        bundle <- bundlesFileConfig
        dep <-
          if (bundle.hasPath(Names.forbiddenDependencies))
            Result.traverse(bundle.configs(Names.forbiddenDependencies))(genForbiddenDependency)
          else Success(Nil)
      } yield dep

      val modules: ResultSeq[Module] = for {
        (bundle, bundleConfig) <- bundleAndModulesConf
        (name, moduleConfig) <- ResultSeq(bundleConfig.nested(BundlesConfig))
        module <- ResultSeq.single(
          genModule(name, bundle, moduleConfig).withProblems(
            moduleConfig.checkExtraProperties(BundlesConfig, Keys.moduleOwnership)))
      } yield module

      val workspaceStructure: Result[WorkspaceStructure] = for {
        bmc <- bundleAndModulesConf.value
        mods <- modules.value
        globalForbiddenDeps <- globalForbiddenDependencies
      } yield WorkspaceStructure(
        name,
        bmc.map { case (bundle, _) => bundle },
        mods.map { module =>
          module.id -> module.copy(forbiddenDependencies =
            globalForbiddenDeps.filterNot(fd => fd.isAllowedIn(module.id)) ++ module.forbiddenDependencies)
        }.toMap
      )

      workspaceStructure.withProblems { ws =>
        val bundleOrderingsErrors = OrderingUtils.checkOrderingIn(BundlesConfig, ws.bundles)
        val moduleOrderingsErrors =
          OrderingUtils.checkOrderingIn(BundlesConfig, ws.modules.values.to(Seq))
        val forbiddenDependencyErrors =
          OrderingUtils.checkForbiddenDependencies(
            BundlesConfig,
            ws.bundles,
            globalForbiddenDependencies.getOrElse(Seq.empty))
        bundleOrderingsErrors ++ moduleOrderingsErrors ++ forbiddenDependencyErrors
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
