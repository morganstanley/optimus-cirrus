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
package optimus.buildtool.compilers

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueFactory
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompiledRunconfArtifact
import optimus.buildtool.artifacts.CompilerMessagesArtifact
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.MessagePosition
import optimus.buildtool.artifacts.{ArtifactType => AT}
import optimus.buildtool.cache.NodeCaching
import optimus.buildtool.compilers.AsyncCppCompiler.BuildType
import optimus.buildtool.compilers.cpp.CppUtils
import optimus.buildtool.compilers.runconfc.RunConfInventory
import optimus.buildtool.compilers.runconfc.RunConfInventoryEntry
import optimus.buildtool.compilers.runconfc.TemplateDescription
import optimus.buildtool.compilers.runconfc.Templates
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ObtConfig
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StratoConfig
import optimus.buildtool.files.Asset
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.buildtool.files.NativeUpstreamFallbackPaths
import optimus.buildtool.files.NativeUpstreamScopes
import optimus.buildtool.files.NativeUpstreams
import optimus.buildtool.files.RelativeInstallPathBuilder
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.format.Names
import optimus.buildtool.format.RunConfSubstitutions
import optimus.buildtool.format.RunConfSubstitutionsValidator
import optimus.buildtool.resolvers.DependencyCopier
import optimus.buildtool.runconf.AppRunConf
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.runconf.RunConfFile
import optimus.buildtool.runconf.TestRunConf
import optimus.buildtool.runconf.compile.AtLocation
import optimus.buildtool.runconf.compile.CompileResult
import optimus.buildtool.runconf.compile.InputFile
import optimus.buildtool.runconf.compile.Level
import optimus.buildtool.runconf.compile.Location
import optimus.buildtool.runconf.compile.Problem
import optimus.buildtool.runconf.compile.PropertyExtractor.unquote
import optimus.buildtool.runconf.compile.RunEnv
import optimus.buildtool.runconf.compile.plugins.native.NativeLibrariesResolver
import optimus.buildtool.runconf.compile.{Compiler => RunconfCompiler}
import optimus.buildtool.runconf.plugins.JavaModule
import optimus.buildtool.runconf.plugins.NativeLibraries
import optimus.buildtool.scope.sources.RunConfSourceSubstitution
import optimus.buildtool.scope.sources.RunconfCompilationSources.isCommonRunConf
import optimus.buildtool.scope.sources.UpstreamRunconfInputs
import optimus.buildtool.trace
import optimus.buildtool.trace.ObtTrace
import optimus.buildtool.trace.Runconf
import optimus.buildtool.utils.AsyncUtils
import optimus.buildtool.utils.ConsistentlyHashedJarOutputStream
import optimus.buildtool.utils.HashedContent
import optimus.buildtool.utils.OS
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.ParameterExpansionParser
import optimus.buildtool.utils.PathUtils
import optimus.buildtool.utils.Utils
import optimus.buildtool.utils.Utils.distinctLast
import optimus.platform._

import java.util.Properties
import scala.collection.compat._
import scala.collection.immutable.SortedMap
import scala.collection.immutable.IndexedSeq
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.control.NonFatal

final case class RunConfCompilerOutput(
    runConfArtifact: Option[CompiledRunconfArtifact],
    messages: CompilerMessagesArtifact,
    configs: Seq[RunConf])

@entity trait AsyncRunConfCompiler {
  def obtWorkspaceProperties: Config
  def runConfSubstitutionsValidator: RunConfSubstitutionsValidator

  @node final def runConfArtifact(
      scopeId: ScopeId,
      inputs: NodeFunction0[AsyncRunConfCompiler.Inputs]): Option[CompiledRunconfArtifact] =
    output(scopeId, inputs).runConfArtifact
  @node final def messages(
      scopeId: ScopeId,
      inputs: NodeFunction0[AsyncRunConfCompiler.Inputs]): CompilerMessagesArtifact =
    output(scopeId, inputs).messages
  @node final def runConfigurations(
      scopeId: ScopeId,
      inputs: NodeFunction0[AsyncRunConfCompiler.Inputs]): IndexedSeq[RunConf] =
    output(scopeId, inputs).configs.toVector

  @node protected def output(
      scopeId: ScopeId,
      inputs: NodeFunction0[AsyncRunConfCompiler.Inputs]): RunConfCompilerOutput
}

object AsyncRunConfCompiler {
  final case class Inputs(
      workspaceSourceRoot: Directory,
      absScopeConfigDir: Directory,
      localArtifacts: SortedMap[SourceUnitId, HashedContent],
      upstreamInputs: UpstreamRunconfInputs,
      sourceSubstitutions: Seq[RunConfSourceSubstitution],
      blockedSubstitutions: Seq[RunConfSourceSubstitution],
      installVersion: String,
      outputJar: JarAsset
  )
}

final case class AnnotatedRunconf(sourceLocation: String, rc: RunConf)
final case class RunConfCompilationResult(
    rootRunConfs: Seq[AnnotatedRunconf],
    localRunConfs: Seq[AnnotatedRunconf],
    problems: Seq[Problem],
    extraMessages: Seq[CompilationMessage])
final case class ApplicationScriptResult(fileName: String, content: Either[String, Seq[CompilationMessage]])

final case class RunConfCompileGroup(sourceFile: InputFile, dependencyFiles: Seq[InputFile], isLocal: Boolean)

final class StaticRunscriptCompilationBindingSources(
    obtWorkspaceProperties: Config
) {
  // ==== WARNING ====================================================
  //  Anything exposed here must be referentially transparent (RT)
  // =================================================================
  def hashableInputs: Seq[String] =
    (obtRootPropertiesOfInterestAsProperties ++ obtWorkspacePropertiesOfInterestAsProperties)
      .map { case (k, v) =>
        s"[Property]$k=$v"
      }
      .to(Seq)
      .sorted

  val obtRootPropertiesOfInterestAsProperties: Map[String, String] = {
    def captureConfig(name: String): Option[(String, String)] =
      if (obtWorkspaceProperties.hasPath(name))
        Some(name -> obtWorkspaceProperties.getAnyRef(name).toString)
      else
        None

    (Nil :+
      captureConfig(Names.ScalaHomePath) :+ // RT
      captureConfig(Names.JavaHomePath) // RT
    ).flatten.toMap
  }

  val obtWorkspacePropertiesOfInterestAsProperties: Map[String, String] = {
    def captureConfig(name: String): Option[(String, String)] = {
      val path = s"${Names.Workspace}.$name"
      if (obtWorkspaceProperties.hasPath(path))
        Some(name -> obtWorkspaceProperties.getAnyRef(path).toString)
      else
        None
    }

    (Nil :+
      captureConfig(Names.JavaProject) :+ // RT
      captureConfig(Names.JavaVersion) :+ // RT
      captureConfig(Names.StratosphereVersion) :+ // RT
      captureConfig(Names.ObtVersion) :+ // RT
      captureConfig(Names.ScalaSourceCompatibility) :+ // RT
      captureConfig(Names.ScalaVersion) :+ // RT
      captureConfig(Names.JavaOptionFiltering) // RT
    ).flatten.toMap
  }
}

@entity final class AsyncRunConfCompilerImpl(
    obtConfig: ObtConfig,
    stratoConfig: Config,
    dependencyCopier: DependencyCopier
) extends AsyncRunConfCompiler {
  val obtWorkspaceProperties: Config =
    obtConfig.properties.fold(ConfigFactory.empty)(_.config)

  private val staticBindingSources = new StaticRunscriptCompilationBindingSources(obtWorkspaceProperties)

  val runConfSubstitutionsValidator: RunConfSubstitutionsValidator =
    obtConfig.runConfSubstitutionsValidator

  /**
   * JNI paths here can come from one of three places:
   *   - `internalPaths` is the collection of internal dependencies that potentially have native artifacts
   *   - `externalDeps` is the collection of external dependencies defined for the scope in .obt files
   *   - `nativeLibraries` is the collection of external dependencies defined for the app in .runconf files
   */
  @node private def jniPaths(
      scopeId: ScopeId,
      nativeUpstreams: NativeUpstreams[Directory],
      transitiveJniPaths: Seq[String],
      nativeLibraries: NativeLibraries,
      pathBuilder: RelativeInstallPathBuilder,
      dirVariable: String,
      exec: String
  ): Seq[String] = {
    val internalPaths =
      nativeScopePaths(
        scopeId = scopeId,
        nativeUpstreams = nativeUpstreams,
        pathBuilder,
        dirVariable,
        exec,
        fileName = None
      )
    val externalDeps = distinctLast(transitiveJniPaths)
    val externalPaths = NativeLibrariesResolver.resolve(nativeLibraries, externalDeps)
    internalPaths ++ externalPaths
  }

  // noinspection SameParameterValue
  @node private def preloadPaths(
      scopeId: ScopeId,
      nativeUpstreamPreloads: NativeUpstreams[FileAsset],
      pathBuilder: RelativeInstallPathBuilder,
      dirVariable: String,
      exec: String,
      fileName: ScopeId => String
  ): Seq[String] = nativeScopePaths(
    scopeId = scopeId,
    nativeUpstreams = nativeUpstreamPreloads,
    pathBuilder,
    dirVariable = dirVariable,
    exec = exec,
    fileName = Some(fileName)
  )

  @node private def nativeScopePaths(
      scopeId: ScopeId,
      nativeUpstreams: NativeUpstreams[Asset],
      pathBuilder: RelativeInstallPathBuilder,
      dirVariable: String,
      exec: String,
      fileName: Option[ScopeId => String]
  ): Seq[String] = nativeUpstreams match {
    case NativeUpstreamScopes(nativeScopes) =>
      pathBuilder.locationIndependentNativePath(
        scopeId,
        nativeScopes,
        dirVariable,
        Some(exec),
        fileName
      )
    case NativeUpstreamFallbackPaths(paths) =>
      paths.map(_.pathString).distinct
  }

  @node private def moduleLoads(transitiveModuleLoads: Seq[String], runconfModuleLoads: Seq[String]): Seq[String] = {
    def addKerberosUnlessMitKerberosForWindows(modules: Seq[String]): Seq[String] =
      if (!modules.exists(_.startsWith("kerberos/mitkfw")))
        Seq("kerberos")
      else Seq.empty
    val modules = runconfModuleLoads ++ transitiveModuleLoads
    distinctLast(modules ++ addKerberosUnlessMitKerberosForWindows(modules))
  }

  @node private def computePathingJarName(scopeId: ScopeId, arc: AppRunConf): String = {
    import NamingConventions._
    val pathingScope = arc.additionalScope
      .filter { bundleScope =>
        // Make sure the scope is included in the bundle jar otherwise we will face ClassNotFoundException
        // If so, we use the bundle jar to minimize grid engine restart
        obtConfig.scopeConfiguration(bundleScope).internalRuntimeDependencies.contains(scopeId)
      }
      .getOrElse(scopeId)

    pathingJarName(pathingScope)
  }

  @node private def contextBindings(
      scopeId: ScopeId,
      defaultJavaModule: Option[JavaModule],
      arc: AppRunConf,
      upstreamInputs: UpstreamRunconfInputs,
      installVersion: String
  ): Map[String, Any] = {
    import AsyncRunConfCompilerImpl.dirNameEnvVar
    import Templates._
    import optimus.buildtool.utils.CrossPlatformSupport.convertToLinuxVariables

    val javaModule = (Some(arc.javaModule) ++ defaultJavaModule)
      .collectFirst {
        case javaModule if javaModule.isDefined => javaModule
      }
    val javaHomePath = javaModule.flatMap(_.pathOption)
    // this endsWith is the one on Path not the one on String, so it matches the last path segment always
    assert(javaHomePath.forall(_.endsWith("exec")), javaHomePath.get)
    val earlySetEnvVars: Seq[(String, String)] = Seq(
      javaHomePath.map(path => "JAVA_HOME" -> PathUtils.platformIndependentString(path))
    ).flatten

    val linuxDirVar = linuxShellVariableNameWrapper(dirNameEnvVar)
    val windowsDirVar = windowsBatchVariableNameWrapper(dirNameEnvVar)

    val pathingJar = computePathingJarName(scopeId, arc)
    require(pathingJar.endsWith(".jar"))
    val linuxClassPath = Seq(s"$linuxDirVar/../lib/$pathingJar")
    // `windowsDirVar` resolves including a trailing `\\`
    val windowsClassPath = Seq(s"$windowsDirVar..\\lib\\$pathingJar")

    val pathBuilder = new RelativeInstallPathBuilder(installVersion)

    // Note: `dirVariable` is expected to resolve with a trailing separator
    val linuxJniPaths =
      jniPaths(
        scopeId = scopeId,
        nativeUpstreams = upstreamInputs.nativeUpstreams,
        transitiveJniPaths = upstreamInputs.transitiveJniPaths,
        nativeLibraries = arc.nativeLibraries,
        pathBuilder = pathBuilder,
        dirVariable = s"$linuxDirVar/",
        exec = "$ID_EXEC"
      )
    val windowsJniPaths =
      jniPaths(
        scopeId = scopeId,
        nativeUpstreams = upstreamInputs.nativeUpstreams,
        transitiveJniPaths = upstreamInputs.transitiveJniPaths,
        nativeLibraries = arc.nativeLibraries,
        pathBuilder = pathBuilder,
        dirVariable = windowsDirVar,
        exec = OsUtils.WindowsSysName
      ).map(_.replace('/', '\\'))

    val upstreamPreloads =
      if (arc.debugPreload) upstreamInputs.nativeUpstreamDebugPreloads else upstreamInputs.nativeUpstreamReleasePreloads
    val preloadBuildType = if (arc.debugPreload) BuildType.Debug else BuildType.Release

    val linuxPreloadPaths = preloadPaths(
      scopeId = scopeId,
      nativeUpstreamPreloads = upstreamPreloads,
      pathBuilder = pathBuilder,
      dirVariable = s"$linuxDirVar/",
      exec = "$ID_EXEC",
      fileName = CppUtils.linuxNativeLibrary(_, preloadBuildType)
    )

    val internalAgents = upstreamInputs.upstreamAgentScopes.flatMap { s =>
      pathBuilder.locationIndependentJar(scopeId, s, NamingConventions.pathingJarName(s))
    }
    val externalAgents = upstreamInputs.externalAgents.apar.flatMap { jar =>
      val externalAgentJar = dependencyCopier.atomicallyDepCopyJarIfMissing(jar)
      pathBuilder.locationIndependentJar(scopeId, externalAgentJar)
    }

    val allAgents = (internalAgents ++ externalAgents).distinct

    val linuxAgentsJarPaths =
      allAgents.map(agentJarPath => if (agentJarPath.startsWith("..")) s"$linuxDirVar/$agentJarPath" else agentJarPath)
    val windowsAgentsJarPaths = allAgents.map(_.replace('/', '\\'))

    val linuxJavaOpts = arc.javaOptsForLinux
    val windowsJavaOpts = arc.javaOptsForWindows

    def packageEnvVars(env: Map[String, String]): Seq[(String, String)] =
      env.toIndexedSeq.filterNot(AsyncRunConfCompilerImpl.isEmptyAllocOpts).sortBy(_._1)
    val environmentVariables = arc.env.toSeq.filterNot(AsyncRunConfCompilerImpl.isEmptyAllocOpts).sortBy(_._1)
    val linuxEnvVars = packageEnvVars(arc.envForLinux)
    val windowsEnvVars = packageEnvVars(arc.envForWindows)

    val mainClassArgs = arc.mainClassArgs
    val linuxMainClassArgs = arc.mainClassArgsForLinux
    val windowsMainClassArgs = arc.mainClassArgsForWindows

    val nonJreModuleLoads = moduleLoads(upstreamInputs.transitiveModuleLoads, arc.moduleLoads)
    val javaModuleToLoad = javaModule
      .flatMap {
        case JavaModule(Some(meta), Some(project), Some(version), None) =>
          Some(s"$meta/$project/$version")
        case _ =>
          None
      }

    import AsyncRunConfCompilerImpl._
    Map(
      "linuxClassPath" -> linuxClassPath.asJava,
      "windowsClassPath" -> windowsClassPath.asJava,
      "linuxJniPaths" -> linuxJniPaths.asJava,
      "linuxPreloadPaths" -> linuxPreloadPaths.asJava,
      "windowsJniPaths" -> windowsJniPaths.asJava,
      "moduleLoads" -> nonJreModuleLoads.asJava,
      "scopedName" -> scopedName(arc),
      "appName" -> getCustomVariableOption(arc, appNameOverride).getOrElse(
        unquote(scopedName(arc).split('.').drop(3).mkString("."))),
      "earlySetEnvVarsBlock" -> earlySetEnvVars.asJava,
      "linuxJavaOpts" -> linuxJavaOpts.asJava,
      "linuxAgentsJarPaths" -> linuxAgentsJarPaths.asJava,
      "windowsAgentsJarPaths" -> windowsAgentsJarPaths.asJava,
      "windowsJavaOpts" -> windowsJavaOpts.asJava,
      "mainClass" -> arc.mainClass,
      "mainClassArgs" -> mainClassArgs.asJava,
      "linuxMainClassArgs" -> linuxMainClassArgs.asJava,
      "windowsMainClassArgs" -> windowsMainClassArgs.asJava,
      "env" -> environmentVariables.asJava, // Backward compatible
      "linuxEnv" -> linuxEnvVars.asJava,
      "windowsEnv" -> windowsEnvVars.asJava,
      "oldTrain" -> getCustomVariable(arc, oldTrainOverride, "true").toBoolean,
      "kerberosKeytabSet" -> hasCustomVariable(arc, kerberosKeytab), // Linux only
      "kerberosKeytab" -> convertToLinuxVariables(getCustomVariable(arc, kerberosKeytab, "")), // Linux only
      "javaModuleSet" -> arc.javaModule.version.isDefined,
      "javaModule" -> javaModuleToLoad.mkString,
      "credentialGuardCompatibility" -> arc.credentialGuardCompatibility,
      "interopPython" -> arc.interopPython
    ) ++ arc.scriptTemplates.customVariables.filterNot(isSpecialPurposeCustomVariable)
  }

  @node private def generateApplicationScripts(
      scopeId: ScopeId,
      templates: Seq[InputFile],
      javaModule: Option[JavaModule],
      runConfs: Seq[RunConf],
      upstreamInputs: UpstreamRunconfInputs,
      installVersion: String
  ): Seq[ApplicationScriptResult] = {
    runConfs.apar.collect {
      case arc: AppRunConf
          if unquote(arc.id.tpe) == scopeId.tpe && !arc.strictRuntime.enabled.getOrElse(
            false) && !arc.python && !arc.interopPython =>
        //
        // Note about the behavior of Windows, Linux and Docker templates:
        //
        //   - They all are generated here, and stored in the runconf build output jar
        //   - The `ApplicationScriptsInstaller` will only copy Windows and Linux runscripts and ignore Docker,
        //     with one exception: `app-runner`, which must be callable in Docker container for the OBT distribution.
        //   - The Docker `ImageBuilder` will copy the Docker runscripts and ignore the others
        //
        Templates.getTemplateDescriptions(templates, arc.name, arc.scriptTemplates).apar.map {
          case Left(templateDescription) =>
            val bindings =
              contextBindings(scopeId, javaModule, arc, upstreamInputs, installVersion)
            val applicationScriptName =
              AsyncRunConfCompilerImpl
                .getTemplateFilenameOverride(arc, templateDescription)
                .getOrElse(s"${arc.name}${templateDescription.outputFileExtension}")
            ApplicationScriptResult(
              s"bin/$applicationScriptName",
              try {
                Left(templateDescription.template.execute(bindings.asJava))
              } catch {
                case NonFatal(t) =>
                  Right(Seq(CompilationMessage.error(t)))
              }
            )
          case Right(compilationMessages) =>
            ApplicationScriptResult(arc.name, Right(compilationMessages))
        }
      case arc: AppRunConf if unquote(arc.id.tpe) == scopeId.tpe && (arc.python || arc.interopPython) =>
        val msg = s"Skipping runscript generation for python enabled runconf '${arc.name}'"
        log.debug(msg)
        Seq(
          ApplicationScriptResult(
            arc.name,
            Right(
              Seq(
                CompilationMessage.info(msg)
              )
            )
          ))
    }.flatten
  }

  @async private def createJar(
      scopeId: ScopeId,
      commonFiles: Seq[InputFile],
      rootFiles: Seq[InputFile],
      sourceSubstitutions: Seq[RunConfSourceSubstitution],
      applicationScripts: Seq[ApplicationScriptResult],
      runConfs: Seq[AnnotatedRunconf],
      jar: FileAsset
  ): CompiledRunconfArtifact = {
    Utils.atomicallyWrite(jar) { tempJar =>
      // we don't incrementally rewrite these jars, so might as well compress them and save the disk space
      val tempJarStream =
        new ConsistentlyHashedJarOutputStream(JarAsset(tempJar), None, compressed = true)
      AsyncUtils.asyncTry {
        // Preserve original source files, with the folder structure as runconf package infer scope from structure
        def storeSourceFile(file: InputFile, isCommon: Boolean = false): Unit =
          tempJarStream.writeFile(
            file.content,
            RelativePath(s"src/${file.origin.toString}")
          )
        commonFiles.foreach(storeSourceFile(_, isCommon = true))
        rootFiles.foreach(storeSourceFile(_))

        val categorizedSubstitutions = sourceSubstitutions.groupBy(_.category)
        categorizedSubstitutions.foreach { case (category, refs) =>
          val capturedReferences =
            Utils.generatePropertiesFileContent(
              refs
                .filterNot(ref => runConfSubstitutionsValidator.isIgnored(ref.category, ref.key))
                .flatMap { ref =>
                  ref.value.map(ref.key -> _)
                }
                .toMap
            )
          tempJarStream.writeFile(
            capturedReferences,
            RelativePath(s"src/$category.${NamingConventions.capturedPropertiesExtension}"))
        }

        def captureObtWorkspaceProperties(name: String, props: Map[String, String]): Unit = {
          // Validate assumption at this point
          require(name.isEmpty || !categorizedSubstitutions.keys.exists(_ == name))
          tempJarStream.writeFile(
            Utils.generatePropertiesFileContent(props),
            RelativePath(s"src/$name.${NamingConventions.capturedPropertiesExtension}")
          )
        }
        captureObtWorkspaceProperties("workspace", staticBindingSources.obtWorkspacePropertiesOfInterestAsProperties)
        captureObtWorkspaceProperties("", staticBindingSources.obtRootPropertiesOfInterestAsProperties)

        RunConfInventory.writeFile(
          tempJarStream,
          runConfs.apar
            .collect {
              case AnnotatedRunconf(sourceLocation, rc) if unquote(rc.id.tpe) == scopeId.tpe =>
                RunConfInventoryEntry(scopeId, rc.name, rc.tpe, Some(sourceLocation))
            }
        )

        applicationScripts.foreach {
          case ApplicationScriptResult(fileName, Left(script)) =>
            tempJarStream.writeFile(script, RelativePath(fileName))
          case _ => // Skip as we failed to render; messages will capture issues separately
        }
      } asyncFinally {
        tempJarStream.close()
      }
    }
    AT.CompiledRunconf.fromAsset(scopeId, jar)
  }

  @async private def readInput(id: SourceUnitId, content: HashedContent): InputFile = {
    val containerId = InputFile.idForPath(id.localRootToFilePath.path)
    InputFile(containerId, content.utf8ContentAsString, origin = id.localRootToFilePath.path)
  }

  private def validateArguments(runConfs: Seq[RunConf]): Seq[CompilationMessage] =
    runConfs
      .flatMap {
        case a: AppRunConf  => a.mainClassArgs ++ a.javaOpts ++ a.env.values.toSeq
        case t: TestRunConf => t.env.values.toSeq ++ t.javaOpts
        case _              => Seq.empty // Unsupported
      }
      .distinct
      .map { arg =>
        arg -> Try(ParameterExpansionParser.parse(arg))
      }
      .collect { case (arg, Failure(e)) =>
        CompilationMessage.message(s"Cannot parse '$arg': $e", CompilationMessage.Warning)
      }

  @node protected def output(
      scopeId: ScopeId,
      inputs0: NodeFunction0[AsyncRunConfCompiler.Inputs]
  ): RunConfCompilerOutput = {
    val inputs = inputs0()
    import inputs._

    def runConfLocationToObt(location: Location): Option[MessagePosition] = {
      location match {
        case atLocation: AtLocation =>
          Some(MessagePosition(atLocation.fileLocation, atLocation.line, -1, atLocation.line, -1, -1, -1))
        case _ => None
      }
    }

    def convertToMessages(problems: Seq[Problem]): Seq[CompilationMessage] =
      problems.flatMap {
        case Problem(message, location: Location, Level.Error) =>
          Some(CompilationMessage(runConfLocationToObt(location), message, CompilationMessage.Error))
        case Problem(_, _, Level.Warn) => // inducing a compile error if we add a new Level
          /* skip warnings for now; they produce noise because:
           * - we compile each parent RC once per scope (so each parent RC's warning shows up in every child scope), and
           * - we only compile parent scopes, so templates which are used in other child scopes nonetheless induce
           *   an unused-template warning.
           * Moreover, currently "unused template" is the only warning emitted.
           */
          None
      }

    val runconfTrace = ObtTrace.startTask(scopeId, Runconf)

    val (files, templates) = localArtifacts.toIndexedSeq.apar
      .map { case (id, hc) =>
        readInput(id, hc)
      }
      .partition { file =>
        RunConfFile.is(file.origin.toFile.getName)
      }

    import optimus.buildtool.runconf.compile.RunConfSupport.names.crossPlatformEnvVar._
    val runEnv = RunEnv.Cmdline(installVersion, OS.Linux)
    val envProperties = Seq(
      appData,
      defaultMsdeHome,
      hostName,
      localAppData,
      tempFolder,
      userHome,
      userName
    ).map(name => name -> s"$$$name").toMap ++ sys.env
    val systemProperties = System.getProperties.clone().asInstanceOf[Properties]
    val (commonFiles, allRootFiles) = files.filter(_.fileName.endsWith(RunConfFile.extension)).partition { f =>
      isCommonRunConf(f.origin)
    }
    // Local runconf need all the other roots, but no other local.runconf.
    // Root files need to be compiled individually
    val (localRootFiles, rootFiles) = allRootFiles.partition { f => RunConfFile.isLocal(f.fileName) }
    // Compile groups are bunches of root files for which the lst depends on the others
    val compileGroups =
      rootFiles.map(f => RunConfCompileGroup(f, Seq.empty, isLocal = false)) ++
        localRootFiles.map(f => f.copy(isLocal = true)).map(f => RunConfCompileGroup(f, rootFiles, isLocal = true))

    val (blockedWithoutLocation, blockedWithLocation) = blockedSubstitutions.partition(_.sourceLocation.isEmpty)
    val badReferenceMessagesWithoutLocation = blockedWithoutLocation
      .map(_.expression)
      .distinct
      .sorted
      .map { expr =>
        import RunConfSubstitutionsValidator._
        CompilationMessage.error(
          s"Substitution $expr is blocked. Please edit '${RunConfSubstitutions.path}' in sections ${names.allowList} or ${names.ignoreList}.")
      }

    val badReferenceMessagesWithLocation = blockedWithLocation
      .flatMap { blocked => blocked.sourceLocation.map { loc => (blocked.expression, loc) } }
      .map { case (expr, loc) =>
        import RunConfSubstitutionsValidator._
        CompilationMessage.error(
          s"${loc.source.localRootToFilePath}[${loc.oneBasedLineIndex}:${loc.startInLine}] Substitution $expr is blocked. Please edit '${RunConfSubstitutions.path}' in sections ${names.allowList} or ${names.ignoreList}.")
      }

    // Allow duplication between common and project (e.g. ird/hedgesumo/projects/hedgesumo)
    def computeDuplicates(files: Seq[InputFile]): Map[String, Seq[InputFile]] =
      files.groupBy(_.origin.toFile.getName).filter(_._2.size > 1)
    val duplicateFilesByName = computeDuplicates(commonFiles) ++ computeDuplicates(allRootFiles)
    val duplicationMessages = duplicateFilesByName.map { case (name, files) =>
      CompilationMessage.error(
        s"Cannot store original .runconf files due to file name conflict: there are ${files.size} files with the name $name.")
    }

    val resultWithDuplicatesInLocalRunConfs: RunConfCompilationResult = compileGroups
      .map { compileGroup =>
        Try {
          RunconfCompiler.compile(
            files = commonFiles ++ compileGroup.dependencyFiles ++ Seq(compileGroup.sourceFile),
            sourceRoot = workspaceSourceRoot.path,
            config = stratoConfig,
            runEnv = runEnv,
            projectProperties = obtWorkspaceProperties,
            enableDTC = true,
            useAppDefaults = true,
            envProperties = envProperties,
            systemProperties = systemProperties,
            validator = Some(obtConfig.appValidator),
            generateCrossPlatformEnvVars = false,
            isLocal = compileGroup.isLocal
          )
        } match {
          case Success(CompileResult(problems, runConfs, _)) =>
            val extraWarnings = validateArguments(runConfs)
            val annotatedRunconfs = runConfs.map(rc => AnnotatedRunconf(compileGroup.sourceFile.origin.toString, rc))
            if (compileGroup.isLocal)
              RunConfCompilationResult(Seq.empty, annotatedRunconfs, problems, extraWarnings)
            else
              RunConfCompilationResult(annotatedRunconfs, Seq.empty, problems, extraWarnings)
          case Failure(e) =>
            RunConfCompilationResult(Seq.empty, Seq.empty, Seq.empty, Seq(CompilationMessage.error(e)))
        }
      }
      .foldLeft(RunConfCompilationResult(Seq.empty, Seq.empty, Seq.empty, Seq.empty)) { (acc, partialResults) =>
        RunConfCompilationResult(
          acc.rootRunConfs ++ partialResults.rootRunConfs,
          acc.localRunConfs ++ partialResults.localRunConfs,
          acc.problems ++ partialResults.problems,
          acc.extraMessages ++ partialResults.extraMessages
        )
      }
    val rootRunConfNames = resultWithDuplicatesInLocalRunConfs.rootRunConfs.map(_.rc.name)
    val result =
      resultWithDuplicatesInLocalRunConfs.copy(
        // Since we need the root files to compile the local runconf, we end up with "duplicates".
        // We need to remove to avoid false report of actual duplicate names.
        localRunConfs = resultWithDuplicatesInLocalRunConfs.localRunConfs
          .filterNot { lrc =>
            rootRunConfNames.contains(lrc.rc.name)
          }
      )

    val applicationScripts =
      generateApplicationScripts(
        scopeId,
        templates,
        AsyncRunConfCompilerImpl.getJavaModule(obtWorkspaceProperties),
        result.rootRunConfs.apar.map(_.rc), // We do not generate runscripts for local runconfs
        upstreamInputs,
        installVersion
      )

    val allRunConfs = result.rootRunConfs ++ result.localRunConfs
    val compiledArtifact: Option[CompiledRunconfArtifact] =
      if (
        badReferenceMessagesWithoutLocation.nonEmpty || badReferenceMessagesWithLocation.nonEmpty || duplicationMessages.nonEmpty || result.extraMessages.nonEmpty
      )
        None
      else
        Some(
          createJar(
            scopeId,
            commonFiles,
            rootFiles ++ localRootFiles,
            sourceSubstitutions,
            applicationScripts,
            allRunConfs,
            outputJar
          )
        )

    val output = RunConfCompilerOutput(
      compiledArtifact,
      storeMessages(
        scopeId,
        outputJar,
        convertToMessages(result.problems) ++ result.extraMessages ++ applicationScripts.flatMap {
          case ApplicationScriptResult(_, Right(messages)) => messages
          case _                                           => None
        } ++ badReferenceMessagesWithoutLocation ++ badReferenceMessagesWithLocation ++ duplicationMessages
      ),
      allRunConfs.apar.map(_.rc)
    )

    runconfTrace.complete(Try(output).map(_.messages.messages))
    output
  }

  @node private def storeMessages(
      scopeId: ScopeId,
      outputJar: JarAsset,
      messages: Seq[CompilationMessage]
  ): CompilerMessagesArtifact = {
    val messagesArtifact = CompilerMessagesArtifact.create(
      id = InternalArtifactId(scopeId, AT.CompiledRunconfMessages, None),
      messageFile = Utils.outputPathForType(outputJar, AT.CompiledRunconfMessages).asJson,
      messages = messages,
      taskCategory = trace.Runconf,
      incremental = false
    )
    messagesArtifact.storeJson()
    messagesArtifact
  }

  // Note: This doesn't contain the scope type
  private def scopedName(rc: RunConf): String = s"${rc.id.fullModule.properPath}.${rc.name}"
}

@entity object AsyncRunConfCompilerImpl {

  private val noPath: ConfigValue = ConfigValueFactory.fromAnyRef("")

  @node def load(
      obtConfig: ObtConfig,
      stratoConfig: StratoConfig,
      dependencyCopier: DependencyCopier): AsyncRunConfCompiler = {

    // Ensure some level of RT-ness by not passing along paths (the format may change, so it will be a rabbit chase)
    val rtIshConfig = stratoConfig.config
      .withValue("stratosphereWorkspace", ConfigValueFactory.fromAnyRef("unnamed-ws"))
      .withValue("stratosphereHome", noPath)
      .withValue("stratosphereWsDir", noPath)
      .withValue("stratosphereSrcDir", noPath)
      .withValue("userHome", noPath)

    AsyncRunConfCompilerImpl(obtConfig, rtIshConfig, dependencyCopier)
  }

  @node private[compilers] def getJavaModule(c: Config): Option[JavaModule] =
    // Caution: keep in sync with RunConfSourceSubstitutions.workspaceDependencies
    if (c.hasPath("workspace.javaProject") && c.hasPath("workspace.javaVersion"))
      Some(
        JavaModule(
          Option(if (c.hasPath("workspace.javaMeta")) c.getString("workspace.javaMeta") else "msjava"),
          Option(c.getString("workspace.javaProject")),
          Option(c.getString("workspace.javaVersion")),
          None
        )
      )
    else None

  private val dirNameEnvVar: String = "DIRNAME"
  private val oldTrainOverride: String = "oldTrain"
  private val kerberosKeytab: String = "kerberosKeytab"
  private val appNameOverride: String = "appNameOverride"
  private val linuxFilenameOverride: String = "linuxFilenameOverride"
  private val windowsFilenameOverride: String = "windowsFilenameOverride"
  private val specialPurposeCustomVariables: Seq[String] =
    Seq(oldTrainOverride, kerberosKeytab, linuxFilenameOverride, windowsFilenameOverride)

  private def isEmptyAllocOpts(pair: (String, String)): Boolean =
    pair match { case (k, v) => k == "ALLOC_OPTS" && v.isEmpty }
  private def isSpecialPurposeCustomVariable(pair: (String, String)): Boolean =
    pair match { case (k, _) => specialPurposeCustomVariables.contains(k) }
  private def getCustomVariable(arc: AppRunConf, name: String, defaultIfMissing: String): String =
    arc.scriptTemplates.customVariables.getOrElse(name, defaultIfMissing)
  private def getCustomVariableOption(arc: AppRunConf, name: String): Option[String] =
    arc.scriptTemplates.customVariables.get(name)
  private def hasCustomVariable(arc: AppRunConf, name: String): Boolean =
    arc.scriptTemplates.customVariables.contains(name)
  private def getTemplateFilenameOverride(arc: AppRunConf, templateDescription: TemplateDescription): Option[String] = {
    templateDescription.name match {
      case Templates.linuxTemplateName =>
        getCustomVariableOption(arc, linuxFilenameOverride)
      case Templates.windowsTemplateName =>
        getCustomVariableOption(arc, windowsFilenameOverride)
      case _ =>
        None
    }
  }

  output.setCustomCache(NodeCaching.reallyBigCache)
}
