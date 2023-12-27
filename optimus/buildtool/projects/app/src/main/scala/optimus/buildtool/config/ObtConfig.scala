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
package optimus.buildtool.config

import java.io.InputStreamReader
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import optimus.buildtool.artifacts._
import optimus.buildtool.compilers.RegexScanner
import optimus.buildtool.dependencies.MultiSourceDependencies
import optimus.buildtool.files.Directory
import optimus.buildtool.files.Directory.ExclusionFilter
import optimus.buildtool.files.Directory.NotHiddenFilter
import optimus.buildtool.files.DirectoryFactory
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.files.WorkspaceSourceRoot
import optimus.buildtool.format.AppValidator
import optimus.buildtool.format.Failure
import optimus.buildtool.format.Message
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.PostInstallApp
import optimus.buildtool.format.ProjectProperties
import optimus.buildtool.format.Result
import optimus.buildtool.format.RunConfSubstitutionsValidator
import optimus.buildtool.format.ScopeDefinition
import optimus.buildtool.format.Success
import optimus.buildtool.format.WarningOverride
import optimus.buildtool.format.WorkspaceDefinition
import optimus.buildtool.format.WorkspaceStructure
import optimus.buildtool.format.docker.DockerStructure
import optimus.buildtool.resolvers.IvyResolver
import optimus.buildtool.trace.FindObtFiles
import optimus.buildtool.trace.LoadConfig
import optimus.buildtool.trace.MessageTrace
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.ReadObtFiles
import optimus.buildtool.trace.RegexCodeFlagging
import optimus.buildtool.utils.GitUtils
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import java.nio.file.Path
import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.immutable.Set
import scala.collection.immutable.SortedMap

/**
 * Represents complete configuration for OBT to build a workspace, as loaded from .obt files
 */
@entity class ObtConfig(
    workspaceSrcRoot: ReactiveDirectory,
    directoryFactory: DirectoryFactory,
    val properties: Option[ProjectProperties],
    val externalDependencies: ExternalDependencies,
    val nativeDependencies: Map[String, NativeDependencyDefinition],
    val globalExcludes: Seq[Exclude],
    val ivyResolvers: Seq[IvyResolver],
    val scopeDefinitions: Map[ScopeId, ScopeDefinition],
    val messages: Map[(MessageArtifactType, MessageTrace), Seq[CompilationMessage]],
    val appValidator: AppValidator,
    val runConfSubstitutionsValidator: RunConfSubstitutionsValidator,
    val dockerStructure: DockerStructure,
    val workspaceStructure: WorkspaceStructure
) extends ScopeConfigurationSourceBase
    with DockerConfigurationSupport {

  @node def compilationScopeIds: Set[ScopeId] = scopeDefinitions.keySet

  @node def local(id: ScopeId): Boolean = {
    val s = scopeDefinitions(id)
    directoryFactory.fileExists(workspaceSrcRoot.resolveFile(s.module.path))
  }

  @node def scopeConfiguration(id: ScopeId): ScopeConfiguration =
    scopeDefinitions(id).configuration

  @node def jarConfiguration(id: ScopeId, versionConfig: VersionConfiguration): JarConfiguration = {
    val scopeDef = scopeDefinitions(id)
    ScopeDefinition.jarConfigurationFor(scopeDef, versionConfig)
  }

  // Note: We can only include a scope in the class bundle if:
  // - it's a main scope (otherwise test discovery on the jar will pick up the test for all the scopes in the bundle)
  // - it's not an agent
  // - it's not going to be copied to other bundles
  @node override def includeInClassBundle(id: ScopeId): Boolean = {
    val cfg = scopeConfiguration(id)
    id.isMain && scopeDefinitions(id).includeInClassBundle && cfg.agentConfig.isEmpty && cfg.targetBundles.isEmpty
  }

  @node def copyFilesConfiguration(id: ScopeId): Option[CopyFilesConfiguration] =
    if (id == ScopeId.RootScopeId) None else scopeDefinitions(id).copyFiles

  @node def extensionConfiguration(id: ScopeId): Option[ExtensionConfiguration] =
    if (id == ScopeId.RootScopeId) None else scopeDefinitions(id).extensions

  override def archiveConfiguration(id: ScopeId): Option[ArchiveConfiguration] =
    if (id == ScopeId.RootScopeId) None else scopeDefinitions(id).archive

  @node def ignoredOptimusAlerts(id: ScopeId): Seq[String] =
    scopeDefinitions(id).configuration.scalacConfig.warnings.overrides.collect {
      case WarningOverride(_, _, WarningOverride.OptimusMessage(id)) => id.toString
    }

  @node def owner(id: ScopeId): String = scopeDefinitions(id).module.owner

  @node def postInstallApps(id: ScopeId): Seq[Set[PostInstallApp]] =
    if (id == ScopeId.RootScopeId) Nil else scopeDefinitions(id).postInstallApps

  @node override def root(id: ParentId): Directory = id match {
    case m: ModuleId =>
      scopeDefinitions
        .collect {
          case (id, sd) if id.fullModule == m =>
            sd.configuration.absScopeConfigDir
        }
        .toSet
        .single
    case p =>
      workspaceSrcRoot.resolveDir(p.elements.mkString("/"))
  }
}

@entity object ObtConfig {
  @node def load(
      workspaceName: String,
      directoryFactory: DirectoryFactory,
      git: Option[GitUtils],
      regexScanner: RegexScanner,
      workspaceSrcRoot: WorkspaceSourceRoot,
      configParams: Map[String, String],
      cppOsVersions: Seq[String],
      useMavenLibs: Boolean = false
  ): ObtConfig =
    ObtTrace.traceTask(ScopeId.RootScopeId, LoadConfig) {
      val configSrcRoot =
        ObtTrace.traceTask(ScopeId.RootScopeId, FindObtFiles) {
          directoryFactory // this is the directory we're going to watch for OBT config changes
            .lookupDirectory(
              workspaceSrcRoot.path,
              // avoid visiting hidden dirs, most notably .git
              dirFilter = NotHiddenFilter && ExclusionFilter(WorkspaceLayout.Strato.profiles(workspaceSrcRoot)),
              // since this causes invalidation of all of the config nodes, filter to only respond to changes in .obt and .conf
              // we also specifically exclude mischief.obt because that file isn't read through normal channels
              fileFilter = Directory.fileExtensionPredicate("obt") &&
                ExclusionFilter(workspaceSrcRoot.resolveFile("mischief.obt")),
              // maxDepth 5 is sufficient to find all .obt files
              maxDepth = 5
            )
        }

      val stratoConfig = StratoConfig.load(directoryFactory, workspaceSrcRoot)

      configSrcRoot.declareVersionDependence()
      val result = ObtTrace.traceTask(ScopeId.RootScopeId, ReadObtFiles) {
        val loader: ObtFile.Loader = loadFile(configSrcRoot, git)
        WorkspaceDefinition.load(
          workspaceName,
          workspaceSrcRoot,
          stratoConfig.config,
          configParams.asJava,
          loader,
          cppOsVersions,
          useMavenLibs
        )
      }

      result match {
        case Success(ws, problems) =>
          val sourceFiles: SortedMap[SourceUnitId, HashedContent] =
            SortedMap((configSrcRoot +: stratoConfig.stratoDirs).apar.collect {
              // filter out locations outside workspace src (eg. <workspace>/config)
              case d if workspaceSrcRoot.contains(d) =>
                directoryFactory.lookupSourceFolder(workspaceSrcRoot, d).sourceFiles
            }.flatten: _*)

          val scannerInputs =
            asNode(() => RegexScanner.ScanInputs(sourceFiles, ws.globalRules.rules.map(_.rules).getOrElse(Nil)))
          val rulesMessages = Map(
            (ArtifactType.RegexMessages, RegexCodeFlagging) -> regexScanner.scan(ScopeId.RootScopeId, scannerInputs)
          )

          val allExternalDependencies: ExternalDependencies = {
            val loadedMultiSourceDeps =
              ws.dependencies.jvmDependencies.multiSourceDependencies.getOrElse(MultiSourceDependencies(Seq.empty))
            val (disabledAfsMappedDeps, enabledAfsMappedDeps) =
              loadedMultiSourceDeps.multiSourceDeps.map(_.asExternalDependency).partition(_.definition.isDisabled)
            val unmappedAfsDeps =
              ws.dependencies.jvmDependencies.dependencies
                .map(ExternalDependency(_, Nil)) ++ loadedMultiSourceDeps.afsOnlyDeps.map(_.asExternalDependency)
            val unmappedMavenDeps = ws.dependencies.jvmDependencies.mavenDependencies.map(ExternalDependency(_, Nil))
            val mappedMavenDeps = (disabledAfsMappedDeps ++ enabledAfsMappedDeps)
              .flatMap(_.equivalents)
              .distinct
              .map(ExternalDependency(_, Nil))
            val mixModeMavenDeps = loadedMultiSourceDeps.mavenOnlyDeps.map(_.asExternalDependency)

            val allAfs = AfsDependencies(unmappedAfsDeps, disabledAfsMappedDeps, enabledAfsMappedDeps)
            val allMaven = MavenDependencies(unmappedMavenDeps, mappedMavenDeps, mixModeMavenDeps)
            ExternalDependencies(allAfs, allMaven)
          }

          ObtConfig(
            workspaceSrcRoot = workspaceSrcRoot,
            directoryFactory = directoryFactory,
            properties = Some(ws.config),
            externalDependencies = allExternalDependencies,
            nativeDependencies = ws.dependencies.jvmDependencies.nativeDependencies,
            globalExcludes = ws.dependencies.jvmDependencies.globalExcludes.toIndexedSeq,
            ivyResolvers = IvyResolver.loadIvyConfig(directoryFactory, workspaceSrcRoot, ws.resolvers),
            scopeDefinitions = ws.scopes,
            messages = Converter.toObt(problems) ++ rulesMessages,
            appValidator = ws.appValidator,
            runConfSubstitutionsValidator = ws.runConfSubstitutionsValidator,
            dockerStructure = ws.dockerStructure,
            workspaceStructure = ws.structure
          )
        case Failure(problems) =>
          throw new IllegalArgumentException(s"Failed to load obt configuration:\n\t${problems.mkString("\n\t")}")
      }
    }

  private def loadFile(workspaceSrcRoot: ReactiveDirectory, git: Option[GitUtils]): ObtFile.Loader =
    new FileLoader(workspaceSrcRoot, git)

  private class FileLoader(workspaceSrcRoot: ReactiveDirectory, git: Option[GitUtils]) extends ObtFile.Loader {
    @entersGraph override def exists(file: ObtFile): Boolean = existsNode(workspaceSrcRoot, git, file)
    @entersGraph override def apply(file: ObtFile): Result[Config] = loadFileNode(workspaceSrcRoot, git, file)
    @entersGraph override def absolutePath(file: ObtFile): Path = loadFilePath(workspaceSrcRoot, file)
  }

  @node private def existsNode(
      workspaceSrcRoot: ReactiveDirectory,
      git: Option[GitUtils],
      file: ObtFile
  ): Boolean = {
    workspaceSrcRoot.declareVersionDependence()
    val fileAsset = workspaceSrcRoot.resolveFile(file.path)
    // Note: We're safe to use existsUnsafe within a node here since we're already watching fileAsset
    // via to the call to `workspaceSrcRoot.declareVersionDependence()` above
    fileAsset.existsUnsafe || git.exists(_.file(file.path.pathString).exists)
  }

  @node private def loadFileNode(
      workspaceSrcRoot: ReactiveDirectory,
      git: Option[GitUtils],
      file: ObtFile
  ): Result[Config] = {
    workspaceSrcRoot.declareVersionDependence()
    val fileAsset = workspaceSrcRoot.resolveFile(file.path)
    git match {
      // Note: We're safe to use existsUnsafe within a node here since we're already watching fileAsset
      // via to the call to `workspaceSrcRoot.declareVersionDependence()` above
      case Some(g) if !fileAsset.existsUnsafe =>
        val f = g.file(file.path.pathString)
        if (f.exists) {
          Result.tryWith(file, line = 0) {
            Success(f.withStream(s => ConfigFactory.parseReader(new InputStreamReader(s))))
          }
        } else Success(ConfigFactory.empty())
      case _ =>
        Result.tryWith(file, line = 0)(Success(ConfigFactory.parseFile(fileAsset.path.toFile)))
    }
  }

  @node private def loadFilePath(
      workspaceSrcRoot: ReactiveDirectory,
      file: ObtFile
  ): Path = {
    workspaceSrcRoot.declareVersionDependence()
    workspaceSrcRoot.resolveFile(file.path).path
  }

  private object Converter {
    def toObt(msgs: Seq[Message]): Map[(MessageArtifactType, MessageTrace), Seq[CompilationMessage]] =
      Map((ArtifactType.ConfigMessages, LoadConfig) -> msgs.map(toObt))

    def toObt(m: Message): CompilationMessage = {
      val pos = MessagePosition(m.file.path.toString, m.line, -1, m.line, -1, -1, -1)
      val level = if (m.isError) CompilationMessage.Error else CompilationMessage.Warning
      CompilationMessage(Some(pos), m.msg, level)
    }

  }
}
