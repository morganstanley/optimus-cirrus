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

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.ExternalClassFileArtifact
import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.artifacts.MessagesArtifact
import optimus.buildtool.builders.StandardBuilder
import optimus.buildtool.compilers.cpp.CppFileCompiler.PrecompiledHeader
import optimus.buildtool.config.CppConfiguration.CompilerFlag
import optimus.buildtool.config.CppConfiguration.LinkerFlag
import optimus.buildtool.config.CppConfiguration.OutputType
import optimus.buildtool.config._
import optimus.buildtool.dependencies.PythonAfsDependencyDefinition
import optimus.buildtool.dependencies.PythonDependency
import optimus.buildtool.dependencies.PythonDependencyDefinition
import optimus.buildtool.files.Directory
import optimus.buildtool.files.LocalDirectoryFactory
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceFolder
import optimus.buildtool.format.CppToolchainStructure
import optimus.buildtool.format.WarningsConfiguration
import optimus.buildtool.generators.GeneratorType
import optimus.buildtool.processors.ProcessorType
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.resolvers.DependencyMetadataResolver
import optimus.buildtool.rubbish.ArtifactRecency
import optimus.buildtool.scope.FingerprintHasher
import optimus.buildtool.trace.BuildWorkspaceStructure
import optimus.buildtool.trace.HashWorkspaceStructure
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.ResolveScopeStructure
import optimus.buildtool.utils.Hashing
import optimus.platform._
import optimus.scalacompat.collection._

import java.nio.file.Path
import scala.collection.compat._
import scala.collection.immutable.Seq

@entity class StructureHasher(
    hasher: FingerprintHasher,
    underlyingBuilder: NodeFunction0[StandardBuilder],
    scalaVersionConfig: NodeFunction0[ScalaVersionConfig],
    pythonEnabled: NodeFunction0[Boolean],
    depMetadataResolvers: NodeFunction0[Seq[DependencyMetadataResolver]]) {
  import JsonImplicits._
  import spray.json._

  @node private def scopeConfigSource = underlyingBuilder().factory.scopeConfigSource

  @node private def scalaFp: String = {
    val scalaConfig = scalaVersionConfig()
    "scala fingerprint:" + Hashing.hashString(scalaConfig.toJson(ScalaVersionConfigFormat).compactPrint)
  }

  @node private def pyEnabledFp: String = {
    val pythonEnabledAsBoolean = pythonEnabled()
    "python enabled: " + Hashing.hashString(pythonEnabledAsBoolean.toString)
  }

  @node private def singleScopeFp(id: ScopeId): String = {
    val config = scopeConfigSource.scopeConfiguration(id)
    id.properPath + ":" + Hashing.hashString(config.toJson(ScopeConfigurationFormat).compactPrint)
  }

  @node private def scopesFp: Seq[String] =
    scopeConfigSource.compilationScopeIds.toIndexedSeq
      .sortBy(_.properPath)
      .apar
      .map(singleScopeFp)

  @node private def depMetadataResolversFp: String =
    "resolvers fingerprint:" + Hashing.hashStrings(depMetadataResolvers().flatMap(_.fingerprint).sorted)

  /**
   * Compute a hash for the workspace from the scope configurations.
   */
  @node def hash: FingerprintArtifact = ObtTrace.traceTask(ScopeId.RootScopeId, HashWorkspaceStructure) {
    val (scala, py, resolvers, scopes) = apar(scalaFp, pyEnabledFp, depMetadataResolversFp, scopesFp)
    val configFingerprint =
      hasher.hashFingerprint(Seq(scala, py, resolvers) ++ scopes, ArtifactType.StructureFingerprint)
    log.debug(s"Hashed config fingerprint: ${configFingerprint.hash}")
    configFingerprint
  }
}

@entity class StructureBuilder(
    underlyingBuilder: NodeFunction0[StandardBuilder],
    scalaVersionConfig: NodeFunction0[ScalaVersionConfig],
    rawPythonEnabled: NodeFunction0[Boolean],
    rawExtractVenvs: NodeFunction0[Boolean],
    directoryFactory: LocalDirectoryFactory,
    dependencyCopier: DependencyCopier,
    structureHasher: StructureHasher,
    recency: Option[ArtifactRecency]
) {

  @node def structure: WorkspaceStructure = ObtTrace.traceTask(ScopeId.RootScopeId, BuildWorkspaceStructure) {
    val builder = underlyingBuilder()
    val rawScalaConfig = scalaVersionConfig()

    checkForErrors(builder.factory.globalMessages)

    // Depcopy all scala lib jars (class, source and javadoc) so they're available for use in the bundle structure.
    // This will generally be a superset of `rawScalaConfig.scalaJars`, which we also depcopy below.
    val scalaLibPath = dependencyCopier.depCopyDirectoryIfMissing(rawScalaConfig.scalaLibPath)
    val scalaJars = rawScalaConfig.scalaJars.apar.map(dependencyCopier.atomicallyDepCopyJarIfMissing)
    val scalaConfig = rawScalaConfig.copy(scalaLibPath = directoryFactory.reactive(scalaLibPath), scalaJars = scalaJars)
    val pythonEnabled = rawPythonEnabled()
    val extractVenvs = rawExtractVenvs()
    val scopeConfigSource = builder.factory.scopeConfigSource

    val scopes: Map[ScopeId, (Seq[Artifact], ResolvedScopeInformation)] =
      ObtTrace.traceTask(ScopeId.RootScopeId, ResolveScopeStructure) {
        scopeConfigSource.compilationScopeIds.apar.flatMap { id =>
          val config = scopeConfigSource.scopeConfiguration(id)
          val local = scopeConfigSource.local(id)
          if (local) {
            val localScopeDeps =
              config.internalCompileDependencies.apar.flatMap(localScopeDependencies(_, scopeConfigSource)).distinct
            val sparseScopeDeps =
              config.internalCompileDependencies.apar.flatMap(sparseScopeDependencies(_, scopeConfigSource)).distinct

            val resolutions = builder.factory
              .lookupScope(id)
              .to(Seq)
              .apar
              .flatMap(_.allCompileDependencies)
              .apar
              .flatMap(_.resolution)

            checkForErrors(resolutions)

            val externalDeps = resolutions.apar.flatMap { resolution =>
              val deps = resolution.result.resolvedArtifacts
              deps.apar.map(dependencyCopier.atomicallyDepCopyExternalClassFileArtifactsIfMissing)
            }
            val scopeInfo = ResolvedScopeInformation(config, local, localScopeDeps, sparseScopeDeps, externalDeps)
            Some(id -> (resolutions, scopeInfo))
          } else None
        }(Map.breakOut)
      }

    val artifacts = scopes.flatMap { case (_, (as, _)) => as }.to(Seq)

    val scopeInfos = scopes.map { case (id, (_, info)) => id -> info }
    val hashedStructureFingerprint = structureHasher.hash

    // mark the artifacts so that we don't garbage collect them on the next bsp run
    recency.foreach(_.markRecent(artifacts :+ hashedStructureFingerprint))

    WorkspaceStructure(hashedStructureFingerprint.hash, scalaConfig, pythonEnabled, extractVenvs, scopeInfos)
  }

  @node private def localScopeDependencies(id: ScopeId, scopeConfigSource: ScopeConfigurationSource): Seq[ScopeId] = {
    if (scopeConfigSource.compilationScopeIds(id)) {
      val config = scopeConfigSource.scopeConfiguration(id)
      if (scopeConfigSource.local(id)) Seq(id)
      else
        config.internalCompileDependencies.apar.flatMap(localScopeDependencies(_, scopeConfigSource)).distinct
    } else Nil
  }

  @node private def sparseScopeDependencies(
      id: ScopeId,
      scopeConfigSource: ScopeConfigurationSource
  ): Seq[ScopeId] = {
    if (scopeConfigSource.compilationScopeIds(id)) {
      val config = scopeConfigSource.scopeConfiguration(id)
      val scopeJar =
        if (scopeConfigSource.local(id)) None
        else Some(id)

      scopeJar.toIndexedSeq ++ config.internalCompileDependencies.apar.flatMap { d =>
        sparseScopeDependencies(d, scopeConfigSource)
      }.distinct
    } else Nil
  }

  private def checkForErrors(artifacts: Seq[Artifact]): Unit = {
    val errorArtifacts = artifacts.collectInstancesOf[MessagesArtifact].filter(_.hasErrors)
    if (errorArtifacts.nonEmpty) {
      val messages = errorArtifacts.flatMap(_.messages.filter(_.isError))
      val messageStrings = messages.map { m =>
        m.pos match {
          case Some(p) => s"${m.msg} [${p.filepath}:${p.startLine}]"
          case None    => m.msg
        }
      }
      throw new IllegalArgumentException(s"Configuration error(s):\n  ${messageStrings.mkString("\n  ")}")
    }
  }
}

final case class WorkspaceStructure(
    structureHash: String,
    scalaConfig: ScalaVersionConfig,
    pythonEnabled: Boolean,
    extractVenvs: Boolean,
    scopes: Map[ScopeId, ResolvedScopeInformation]
) {
  override def toString: String = s"WorkspaceStructure($structureHash)"
  // again assuming that sha256 is a decent hash
  override def hashCode: Int = structureHash.##
  override def equals(that: Any): Boolean = that match {
    case other: WorkspaceStructure => this.structureHash == other.structureHash
    case _                         => false
  }
}

final case class ResolvedScopeInformation(
    config: ScopeConfiguration,
    local: Boolean,
    localInternalCompileDependencies: Seq[ScopeId],
    sparseInternalCompileDependencies: Seq[ScopeId],
    externalCompileDependencies: Seq[ExternalClassFileArtifact]
)

//noinspection TypeAnnotation
object JsonImplicits {
  import optimus.buildtool.artifacts.JsonImplicits._
  import spray.json.DefaultJsonProtocol._
  import spray.json._

  implicit val DirectoryFormat: JsonFormat[Directory] = new JsonFormat[Directory] {
    override def write(obj: Directory): JsValue = obj.path.toJson
    override def read(json: JsValue): Directory = Directory(json.convertTo[Path])
  }
  implicit val ReactiveDirectoryFormat: JsonFormat[ReactiveDirectory] = new JsonFormat[ReactiveDirectory] {
    override def write(obj: ReactiveDirectory): JsValue = obj.path.toJson
    override def read(json: JsValue): ReactiveDirectory = throw new UnsupportedOperationException
  }
  implicit val SourceFolderFormat: JsonFormat[SourceFolder] = new JsonFormat[SourceFolder] {
    override def write(obj: SourceFolder): JsValue = obj.workspaceSrcRootToSourceFolderPath.path.toJson
    override def read(json: JsValue): SourceFolder = throw new UnsupportedOperationException
  }

  implicit val ScalaVersionConfigFormat: RootJsonFormat[ScalaVersionConfig] = jsonFormat3(ScalaVersionConfig.apply)

  implicit val GeneratorTypeFormat: JsonFormat[GeneratorType] = new JsonFormat[GeneratorType] {
    override def write(obj: GeneratorType): JsValue = obj.name.toJson
    override def read(json: JsValue): GeneratorType = GeneratorType(json.convertTo[String])
  }
  implicit val KindFormat: JsonFormat[Kind] = new JsonFormat[Kind] {
    override def write(obj: Kind): JsValue = obj.toString.toJson
    override def read(json: JsValue): Kind = Kind.values.map(k => k.toString -> k).toMap.apply(json.convertTo[String])
  }

  implicit val ProcessorTypeFormat: JsonFormat[ProcessorType] = new JsonFormat[ProcessorType] {
    override def write(obj: ProcessorType): JsValue = obj.name.toJson
    override def read(json: JsValue): ProcessorType = ProcessorType(json.convertTo[String])
  }

  implicit val ExcludeFormat: RootJsonFormat[Exclude] = jsonFormat3(Exclude.apply)
  implicit val VariantFormat: RootJsonFormat[Variant] = jsonFormat3(Variant.apply)
  implicit val IvyArtifactFormat: RootJsonFormat[IvyArtifact] = jsonFormat3(IvyArtifact.apply)
  implicit val ModuleIdFormat: RootJsonFormat[ModuleId] = jsonFormat3(ModuleId.apply)

  implicit val DependencyDefinitionFormat: RootJsonFormat[DependencyDefinition] = jsonFormat18(
    DependencyDefinition.apply)
  implicit val NativeDependencyDefinitionFormat: RootJsonFormat[NativeDependencyDefinition] = jsonFormat4(
    NativeDependencyDefinition.apply)

  implicit val GeneratorConfigurationFormat: RootJsonFormat[GeneratorConfiguration] = jsonFormat7(
    GeneratorConfiguration.apply)
  implicit val RunConfConfigurationFormat: RootJsonFormat[RunConfConfiguration[RelativePath]] = jsonFormat2(
    RunConfConfiguration.apply[RelativePath])
  implicit val AgentConfigurationFormat: RootJsonFormat[AgentConfiguration] = jsonFormat2(AgentConfiguration.apply)
  implicit val MetaProjFormat: RootJsonFormat[MetaBundle] = jsonFormat2(MetaBundle.apply)

  implicit val PatternFormat: JsonFormat[Pattern] = new JsonFormat[Pattern] {
    override def write(obj: Pattern): JsValue = (obj.regex.regex, obj.exclude, obj.message).toJson
    override def read(json: JsValue): Pattern = {
      val (regex, exclude, message) = json.convertTo[(String, Boolean, Option[String])]
      Pattern(regex, exclude, message)
    }
  }

  implicit val GroupFormat: RootJsonFormat[Group] = jsonFormat3(Group.apply)
  implicit val FilterFormat: RootJsonFormat[Filter] = jsonFormat4(Filter.apply)
  implicit val CodeFlaggingRuleFormat: RootJsonFormat[CodeFlaggingRule] = jsonFormat8(CodeFlaggingRule.apply)
  implicit val RegexConfigurationFormat: RootJsonFormat[RegexConfiguration] = jsonFormat1(RegexConfiguration.apply)

  implicit val OutputTypeFormat: JsonFormat[OutputType] = new JsonFormat[OutputType] {
    override def write(obj: OutputType): JsValue = obj match {
      case OutputType.Library    => "library".toJson
      case OutputType.Executable => "executable".toJson
    }
    override def read(json: JsValue): OutputType = json.convertTo[String] match {
      case "library"    => OutputType.Library
      case "executable" => OutputType.Executable
    }
  }

  implicit val CompilerFlagFormat: JsonFormat[CompilerFlag] = new JsonFormat[CompilerFlag] {
    override def write(obj: CompilerFlag): JsValue = CppToolchainStructure.printCompilerFlag(obj).toJson
    override def read(json: JsValue): CompilerFlag = CppToolchainStructure.parseCompilerFlag(json.convertTo[String])
  }

  implicit val LinkerFlagFormat: JsonFormat[LinkerFlag] = new JsonFormat[LinkerFlag] {
    override def write(obj: LinkerFlag): JsValue = CppToolchainStructure.printLinkerFlag(obj).toJson
    override def read(json: JsValue): LinkerFlag = CppToolchainStructure.parseLinkerFlag(json.convertTo[String])
  }

  implicit val PrecompiledHeaderFormat: RootJsonFormat[PrecompiledHeader] = jsonFormat3(PrecompiledHeader.apply)

  // Dependencies.apply is customized with private args, which would confuse write.toJson at runtime then
  // get bsp failures. Therefore we have to define a new JsonFormat with read & write
  implicit val DependenciesFormat: JsonFormat[Dependencies] =
    new JsonFormat[Dependencies] {
      override def read(json: JsValue): Dependencies = {
        val (internal, externalAfs, externalMaven) =
          json.convertTo[(Seq[ScopeId], Seq[DependencyDefinition], Seq[DependencyDefinition])]
        Dependencies(internal, externalAfs, externalMaven)
      }
      override def write(obj: Dependencies): JsValue = obj.toJsonInput.toJson
    }
  implicit val GroupNameConfigFormat: RootJsonFormat[GroupNameConfig] = jsonFormat3(GroupNameConfig.apply)
  implicit val SubstitutionFormat: RootJsonFormat[Substitution] = jsonFormat2(Substitution.apply)
  implicit val PartialScopeIdFormat: RootJsonFormat[PartialScopeId] = jsonFormat4(PartialScopeId.apply)
  implicit val ForbiddenDependencyConfigurationFormat: RootJsonFormat[ForbiddenDependencyConfiguration] = jsonFormat7(
    ForbiddenDependencyConfiguration.apply)
  implicit val AllDependenciesFormat: RootJsonFormat[AllDependencies] = jsonFormat7(AllDependencies.apply)
  implicit val InheritableWarningsConfigFormat: JsonFormat[WarningsConfiguration] =
    new JsonFormat[WarningsConfiguration] {
      override def write(obj: WarningsConfiguration): JsValue = obj.asJson
      override def read(json: JsValue): WarningsConfiguration = throw new UnsupportedOperationException
    }
  implicit val ScalacConfigurationFormat: RootJsonFormat[ScalacConfiguration] = jsonFormat4(ScalacConfiguration.apply)
  implicit val JavacConfigurationFormat: RootJsonFormat[JavacConfiguration] = jsonFormat4(JavacConfiguration.apply)
  implicit val CppToolchainFormat: RootJsonFormat[CppToolchain] = jsonFormat14(CppToolchain.apply)
  implicit val CppBuildConfigurationFormat: RootJsonFormat[CppBuildConfiguration] = jsonFormat16(
    CppBuildConfiguration.apply)
  implicit val CppConfigurationFormat: RootJsonFormat[CppConfiguration] = jsonFormat3(CppConfiguration.apply)

  implicit val WebConfigurationFormat: RootJsonFormat[WebConfiguration] = jsonFormat4(WebConfiguration.apply)
  implicit val ElectronConfigurationFormat: RootJsonFormat[ElectronConfiguration] = jsonFormat5(
    ElectronConfiguration.apply)

  implicit val ProcessorConfigurationFormat: RootJsonFormat[ProcessorConfiguration] = jsonFormat7(
    ProcessorConfiguration.apply)

  implicit val ScopePathsFormat: RootJsonFormat[ScopePaths] = jsonFormat10(ScopePaths.apply)
  implicit val ScopeFlagsFormat: RootJsonFormat[ScopeFlags] = jsonFormat14(ScopeFlags.apply)

  implicit val interopConfigurationFormat: RootJsonFormat[InteropConfiguration] = jsonFormat2(
    InteropConfiguration.apply)

  implicit val pythonPyPiDependencyFormat: RootJsonFormat[PythonDependencyDefinition] = jsonFormat5(
    PythonDependencyDefinition.apply)
  implicit val pythonAfsDependencyFormat: RootJsonFormat[PythonAfsDependencyDefinition] = jsonFormat6(
    PythonAfsDependencyDefinition.apply)

  implicit val pythonDependencyFormat: JsonFormat[PythonDependency] = new JsonFormat[PythonDependency] {
    override def write(obj: PythonDependency): JsValue = obj match {
      case dep @ PythonDependencyDefinition(_, _, _, _, _)       => dep.toJson
      case dep @ PythonAfsDependencyDefinition(_, _, _, _, _, _) => dep.toJson
    }
    override def read(value: JsValue): PythonDependency =
      value.asJsObject.fields("sourceType") match {
        case JsString("afs")  => value.convertTo[PythonAfsDependencyDefinition]
        case JsString("pypi") => value.convertTo[PythonDependencyDefinition]
        case _                => throw DeserializationException(s"Couldn't deserialize ${value.compactPrint}")
      }
  }

  implicit val afsModuleFormat: JsonFormat[ModuleType] = new JsonFormat[ModuleType] {
    override def write(obj: ModuleType): JsValue = obj match {
      case ModuleType.Afs  => ModuleType.Afs.label.toJson
      case ModuleType.PyPi => ModuleType.PyPi.label.toJson
    }
    override def read(json: JsValue): ModuleType = ModuleType.resolve(json.convertTo[String]).get
  }

  implicit val pythonOverriddenCommandsFormat: RootJsonFormat[PythonConfiguration.OverriddenCommands] = jsonFormat2(
    PythonConfiguration.OverriddenCommands.apply)

  implicit val pythonConfigurationFormat: RootJsonFormat[PythonConfiguration] = jsonFormat4(PythonConfiguration.apply)

  implicit val ScopeConfigurationFormat: RootJsonFormat[ScopeConfiguration] = jsonFormat18(ScopeConfiguration.apply)

  implicit val ExternalClassFileArtifactFormat: JsonFormat[ExternalClassFileArtifact] =
    new JsonFormat[ExternalClassFileArtifact] {
      override def write(obj: ExternalClassFileArtifact): JsValue = obj.cached.toJson
      override def read(json: JsValue): ExternalClassFileArtifact =
        json.convertTo[ExternalClassFileArtifact.Cached].asEntity
    }

  implicit val ResolveScopeInformationFormat: RootJsonFormat[ResolvedScopeInformation] = jsonFormat5(
    ResolvedScopeInformation.apply)
}
