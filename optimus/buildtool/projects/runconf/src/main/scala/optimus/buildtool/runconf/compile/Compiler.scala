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
package optimus.buildtool.runconf
package compile

import java.nio.file.Path
import java.util.Properties
import java.util.{Map => JMap}
import com.typesafe.config.Config
import com.typesafe.config.ConfigException
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigResolveOptions
import com.typesafe.config.ConfigUtil
import com.typesafe.config.ConfigValueFactory
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ParentId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.config.WorkspaceId
import optimus.buildtool.runconf.compile.RunConfSupport.names
import optimus.buildtool.runconf.compile.plugins.DTCSupport
import optimus.buildtool.runconf.compile.plugins.ExtraExecOptionsSupport
import optimus.buildtool.runconf.compile.plugins.JavaModulesSupport
import optimus.buildtool.runconf.compile.plugins.ScopeDependentReferences
import optimus.buildtool.runconf.compile.plugins.TestSuitesSupport
import optimus.buildtool.runconf.compile.plugins.TreadmillOptionsSupport
import optimus.buildtool.runconf.compile.plugins.native.NativeLibrariesSupport
import optimus.buildtool.runconf.compile.plugins.scriptTemplates.ScriptTemplatesSupport
import optimus.buildtool.utils.CliArgs
import optimus.buildtool.utils.JavaOpts
import optimus.buildtool.utils.OS

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.util.Try
import optimus.scalacompat.collection._

object Compiler {
  // Dark secret: javaOptionFiltering can also be a FQCN
  private final case class JavaFilteringConfig(filtering: String, jvmVersion: String, obtInstall: Option[String])

  def compile(
      files: Seq[InputFile],
      sourceRoot: Path,
      config: Config,
      runEnv: RunEnv,
      projectProperties: Config,
      enableDTC: Boolean,
      systemProperties: Properties = System.getProperties,
      externalCache: ExternalCache = NoCache,
      editedScope: Option[ParentId] = None,
      validator: Option[Validator[AppRunConf]] = None,
      // TODO (OPTIMUS-36303): remove after tests' java version is set from runconf
      // currently we set java version from --java-version in test-runner command line; this is that
      reallyRealJavaVersionOverride: Option[String] = None,
      envProperties: Map[String, String] = sys.env,
      installPathOverride: Option[Path] = None,
      generateCrossPlatformEnvVars: Boolean = true,
      modifier: Option[(ScopedName, Endo[UnresolvedRunConf])] = None,
      reportDuplicates: Boolean = false
  ): CompileResult = {
    val compiler = new Compiler(
      envProperties,
      systemProperties,
      installPathOverride,
      runEnv,
      config,
      projectProperties,
      enableDTC,
      externalCache,
      editedScope,
      Some(sourceRoot),
      validator,
      reallyRealJavaVersionOverride,
      generateCrossPlatformEnvVars,
      modifier,
      reportDuplicates
    )
    compiler.compile(files)
  }

  private val expectedTypes: Map[String, Type] = {
    val envType = T.Object(T.Union(Set(T.String, T.Array(T.String))))
    Map(
      names.mainClass -> T.String,
      names.moduleName -> T.String,
      names.javaOpts -> T.Array(T.String),
      names.env -> envType,
      names.mainClassArgs -> T.Array(T.String),
      names.parents -> T.Array(T.String),
      names.packageName -> T.String,
      names.isTest -> T.Boolean,
      names.methodName -> T.String,
      names.agents -> T.Array(T.String),
      names.launcher -> T.Union(Launcher.names.map(T.StringLiteral)),
      names.allowDTC -> T.Boolean,
      names.condition -> T.Any,
      names.includes -> T.Array(T.String),
      names.excludes -> T.Array(T.String),
      names.maxParallelForks -> T.Integer,
      names.forkEvery -> T.Integer,
      names.appendableEnv -> envType,
      names.extractTestClasses -> T.Boolean,
      names.moduleLoads -> T.Array(T.String),
      names.credentialGuardCompatibility -> T.Boolean,
      names.debugPreload -> T.Boolean,
      names.additionalScope -> T.String,
      names.treadmillOpts -> T.Object(T.Union(Set(T.String, T.Integer))),
      names.extraExecOpts -> T.Object(T.Union(Set(T.String, T.Integer))),
      names.category -> T.String,
      names.owner -> T.String,
      names.flags -> T.Object(T.Union(Set(T.String, T.String)))
    )
  }

  private val expectedProperties = expectedTypes.keySet ++
    TestSuitesSupport.expectedProperties ++
    NativeLibrariesSupport.expectedProperties ++
    JavaModulesSupport.expectedProperties ++
    ScriptTemplatesSupport.expectedProperties

  private val forbiddenSetsOfTestProperties = Map(
    names.packageName -> Seq(names.mainClass, names.methodName)
  )

  private val testOnlyProperties = Seq(
    names.packageName,
    names.methodName,
    names.includes,
    names.excludes,
    names.forkEvery,
    names.maxParallelForks,
    names.extractTestClasses,
    TestSuitesSupport.names.suites,
    names.treadmillOpts,
    names.extraExecOpts,
    names.category,
    names.owner,
    names.flags
  )

  object defaults {
    val maxParallelForks: Int = 1
    val forkEvery: Int = 0
    val launcher: Launcher = Launcher.Standard
    val defaultAppScopeType = "main"
    val defaultTestScopeType = "test"
    val defaultCredentialGuardCompatibility = false
    val defaultDebugPreload = false
  }

}

class Compiler(
    env: Map[String, String],
    sysProps: Properties,
    installOverride: Option[Path],
    runEnv: RunEnv,
    stratoConfig: Config,
    projectProperties: Config,
    enableDTC: Boolean,
    externalCache: ExternalCache,
    editedScope: Option[ParentId],
    sourceRoot: Option[Path] = None,
    validator: Option[Validator[AppRunConf]] = None,
    reallyRealJavaVersionOverride: Option[String] = None,
    generateCrossPlatformEnvVars: Boolean = true,
    /** Modify the runconf with the given scoped name thus. */
    modifier: Option[(ScopedName, Endo[UnresolvedRunConf])] = None,
    // for now reporting of duplicates, on compiler level, will only be enabled in IDE
    reportDuplicates: Boolean = false
) {
  import Compiler._

  private val scopeDependentReferences = new ScopeDependentReferences(runEnv, externalCache)
  private val dtcSupport = new DTCSupport(enableDTC, stratoConfig)
  private val testSuitesSupport = new TestSuitesSupport(runEnv, sysProps)
  private val scriptTemplatesSupport = new ScriptTemplatesSupport(runEnv)
  private val javaModuleSupport = new JavaModulesSupport(runEnv)

  private val javaFiltering =
    Try(stratoConfig.getString("javaOptionFiltering")).toOption
      .filter(s => s.contains('.') || s.toBoolean)
      .map(
        JavaFilteringConfig(
          _,
          jvmVersion = stratoConfig.getString("javaVersion"),
          obtInstall = {
            if (stratoConfig.hasPath("internal.obt.install"))
              Some(stratoConfig.getString("internal.obt.install"))
            else None
          }
        )
      )

  def filterJavaRuntimeOptions(reporter: Reporter, opts: Seq[String], jvmVersionOverride: Option[String]): Seq[String] =
    javaFiltering
      .map { conf =>
        jvmVersionOverride.fold(conf)(v => conf.copy(jvmVersion = v))
      }
      .fold(opts) { conf =>
        import conf._
        JavaOpts.filterJavaRuntimeOptions(jvmVersion, opts, reporter.atName)
      }

  private val globalReporter = new GlobalReporter

  def compile(files: Seq[InputFile]): CompileResult = {
    val parsed: Seq[(Config, ParentId)] = parseFiles(files)
    val scopedFiles: Map[ParentId, Path] = files.map(file => file.scope -> file.origin).toMap
    val scopedApps = applyScopesForRunConfs(parsed)
    val unresolvedConfig = toUnresolvedConfig(scopedApps)

    val result = for {
      resolvedConfig <- resolveSubstitutions(unresolvedConfig)
      definitionsByBlock <- definitionsFromConfig(resolvedConfig)
    } yield {
      val allRunConfs = typecheckAll(definitionsByBlock, resolvedConfig, scopedFiles)
      modify(allRunConfs)
      val typedRunConfs = allRunConfs.filter(_.isTyped)
      reportUnknownProperties(allRunConfs)
      validateAndAssignParents(allRunConfs, typedRunConfs)
      resolveRunConfs(parsed, typedRunConfs)
      postResolveSteps(installOverride, typedRunConfs)
      toCompileResult(allRunConfs)
    }
    result.left.map(_.withProblems(globalReporter.problems)).merge
  }

  private def parseFiles(files: Seq[InputFile]): Seq[(Config, ParentId)] = {
    def parse(file: InputFile): Either[(String, Int), Config] = {
      try {
        Right(ConfigFactory.parseString(file.content))
      } catch {
        case e: ConfigException => Left(e.getMessage, e.origin().lineNumber())
      }
    }

    files.flatMap { file =>
      externalCache.parse.cached(file.origin, file)(parse(file)) match {
        case Right(parsed) =>
          Some(parsed -> file.scope)
        case Left((error, line)) =>
          globalReporter.atFile(file.origin, line, error)
          None
      }
    }
  }

  private def applyScopesForRunConfs(parsed: Seq[(Config, ParentId)]): Seq[(Config, ParentId)] = {
    parsed.map { case (config, scope) =>
      val isScoped = scope != WorkspaceId
      if (isScoped) {
        val scopedConfig = externalCache.resolveWithAppliedScopes.cached(syntheticProperties, (config, scope)) {
          names.allBlocks.foldLeft(config)((config, block) => applyScopeForBlock(config, block, scope))
        }
        (scopedConfig, scope)
      } else (config, scope)
    }
  }

  private def applyScopeForBlock(config: Config, block: Block, scope: ParentId): Config = {

    /**
     * Array append is actually syntax sugar for concatenation with itself e.g. definitions { App { javaOpts =
     * ${definitions.App.javaOpts} [ "-ea" ] } } Such references need to be resolved before changing 'App' to
     * 'optimus.platform.core.App' as after repathing the old reference would be invalid and it is difficult to update
     * it.
     */
    def resolveLocalSubstitutions(config: Config): Config = {
      resolve(config, partially = true)
    }

    extractBlock(config, block) match {
      case Some(definitions) =>
        val runConfNames = definitions.keySet.asScala
        val partiallyResolved = resolveLocalSubstitutions(config)

        runConfNames.foldLeft(partiallyResolved) { (config, runConfName) =>
          val scopedKey = ConfigUtil.joinPath(scope.elements :+ runConfName.stripPrefix("\"").stripSuffix("\""): _*)
          val originalPath = ConfigUtil.joinPath(block, runConfName)
          val newPath = ConfigUtil.joinPath(block, scopedKey)
          val oldValue = config.getValue(originalPath)
          config.withoutPath(originalPath).withValue(newPath, oldValue)
        }
      case None => config
    }
  }

  /**
   * Currently sources are expected to be correct HOCON. For more robustness we could try to proceed ignoring incorrect
   * files. That would be useful for Intellij, but should be disabled for gradle.
   */
  private def toUnresolvedConfig(configs: Seq[(Config, ParentId)]): Config = {
    def extractConfig(scopedConfig: (Config, ParentId)): Config = scopedConfig._1

    def merge(configs: Seq[(Config, ParentId)]): Config = {
      configs.iterator.map(extractConfig).foldLeft(ConfigFactory.empty)(_ withFallback _)
    }

    val (edited, untouched) = configs.partition { case (_, scope) => editedScope.contains(scope) }

    val untouchedMerged = externalCache.merge.cached(None, untouched.map(extractConfig))(merge(untouched))
    val editedMerged = merge(edited)

    editedMerged.withFallback(untouchedMerged)
  }

  private def crossPlatformEnvVars: Map[String, String] =
    if (generateCrossPlatformEnvVars) {
      import names.crossPlatformEnvVar._
      val currentUser = env.getOrElse("USERNAME", env.getOrElse("USER", ""))
      val currentTmp = env.get("TEMP").orElse(env.get("TMP")).getOrElse(sys.props("java.io.tmpdir"))
      val userTmp = s"$currentTmp/$currentUser"
      // Folders must be valid for proids and humans
      Map(
        appData -> env.getOrElse("APPDATA", s"/var/tmp/$currentUser/"),
        defaultMsdeHome -> (if (runEnv.os == OS.Windows) StaticConfig.string("defaultMsdeHomeWindows") else "~/"),
        hostName -> env
          .get("COMPUTERNAME")
          .map(c => s"$c.${StaticConfig.string("domain")}")
          .orElse(env.get("HOSTNAME"))
          .getOrElse("localhost"),
        localAppData -> env.getOrElse("LOCALAPPDATA", userTmp),
        tempFolder -> currentTmp,
        userHome -> env.getOrElse("HOMEPATH", "~/"),
        userName -> currentUser
      )
    } else Map.empty

  private lazy val syntheticProperties: Config = {
    // ConfigValueFactory parses map keys as actual keys, rather than path expressions
    // for env vars this allows to avoid errors when they contain characters that need
    // quoting in HOCON like '='.
    // For system properties we still use ConfigFactory to allow syntax ${sys.a.b.c}
    // Otherwise, for properties with dots in path it would need to be ${sys."a.b.c"}.
    val systemPropertiesConfigObject = ConfigFactory.parseProperties(sysProps).root
    val environmentConfigObject = ConfigValueFactory.fromMap((crossPlatformEnvVars ++ env).asJava)
    val stratoConfigObject = stratoConfig.root
    val osIndicatorsConfigObject = ConfigValueFactory.fromMap {
      Map(
        names.os.windows -> (runEnv.os == OS.Windows),
        names.os.linux -> (runEnv.os == OS.Linux)
      ).asJava
    }
    val dtcConfigValue = ConfigValueFactory.fromAnyRef(Boolean box enableDTC)

    ConfigFactory.empty
      .withValue(names.scopeKey.sysProps, systemPropertiesConfigObject)
      .withValue(names.scopeKey.env, environmentConfigObject)
      .withValue(names.scopeKey.config, stratoConfigObject)
      .withValue(names.scopeKey.os, osIndicatorsConfigObject)
      .withValue(names.scopeKey.dtcEnabled, dtcConfigValue)
      .withFallback(scopeDependentReferences.syntheticProperties)
      .withFallback(projectProperties)
  }

  /**
   * Resolves all substitutions like ${path} and array appends (+=)
   */
  private def resolveSubstitutions(unresolvedConfigs: Config): Either[CompileResult, Config] = {
    try {
      Right(resolve(unresolvedConfigs))
    } catch {
      case e: ConfigException.UnresolvedSubstitution =>
        val BadSubstitution = """.+: (\d+): Could not resolve substitution to a value: \$\{(.*)\}""".r
        e.getMessage match {
          case BadSubstitution(line, substPath) =>
            val message = Messages.invalidSubstitution(substPath)
            val location = AtSubstitution(line.toInt, substPath)
            val problem = Problem(message, location, Level.Error)
            Left(CompileResult.fromProblem(problem))
        }
      case unsupportedError: Throwable => throw unsupportedError
    }
  }

  private def resolve(config: Config, partially: Boolean = false): Config = {
    config.withFallback(syntheticProperties).resolve(ConfigResolveOptions.noSystem.setAllowUnresolved(partially))
  }

  private def definitionsFromConfig(config: Config): Either[CompileResult, Map[Block, RawProperties]] = {
    val definitionsByBlock = names.allBlocks.flatMap { block =>
      extractBlock(config, block).map { blockObject =>
        block -> blockObject.entrySet.asScala.map(entry => entry.getKey -> entry.getValue.unwrapped).toMap
      }
    }.toMap

    if (definitionsByBlock.isEmpty) {
      Left(CompileResult.empty)
    } else {
      Right(definitionsByBlock)
    }
  }

  private def extractBlock(config: Config, block: Block): Option[ConfigObject] = {
    if (config.hasPath(block) && config.getValue(block).isInstanceOf[ConfigObject]) {
      Some(config.getObject(block))
    } else None
  }

  private def modify(runconfs: Seq[RunConfCompilingState]): Unit = {
    modifier.foreach { case (target, endo) =>
      runconfs.find(r => r.scopedName == target).foreach { state =>
        state.update(endo(state.runConf))
        assert(
          state.runConf.scopedName == state.scopedName,
          (state.runConf.scopedName, state.scopedName)
        ) // don't change scopedName!
      }
    }
  }

  private def typecheckAll(
      definitionsByBlock: Map[Block, RawProperties],
      config: Config,
      filesByScope: Map[ParentId, Path]
  ): Seq[RunConfCompilingState] = {
    definitionsByBlock.flatMap { case (block, definitions) =>
      definitions.map { case (scopedNameStr, properties) =>
        val scopedName = ScopedName.parse(scopedNameStr)
        typecheckRunConf(block, scopedName, properties, config, filesByScope(scopedName.id))
      }
    }.toList
  }

  private def typecheckRunConf(
      block: Block,
      scopedName: ScopedName,
      value: Any,
      config: Config,
      file: Path
  ): RunConfCompilingState = {
    toUntypedProperties(value) match {
      case Some(untypedProperties) =>
        typecheckProperties(scopedName, block, untypedProperties, config, file) match {
          case Right(_) =>
            val runConf = buildUnresolvedRunConf(block, scopedName, untypedProperties)
            RunConfCompilingState.typechecked(block, scopedName, untypedProperties, config, runConf, file, sourceRoot)
          case Left(typeProblems) =>
            RunConfCompilingState.incorrect(
              block,
              scopedName,
              typeProblems,
              config,
              file,
              sourceRoot,
              untypedProperties
            )
        }
      case None =>
        val incorrectType = Typer.resolve(value)
        val reporter: Reporter = Reporter(block, scopedName, config, file, sourceRoot).withLevel(Level.Error)
        reporter.atName(Messages.invalidRunConfType(incorrectType))
        RunConfCompilingState.incorrect(block, scopedName, reporter.problems, config, file, sourceRoot)
    }
  }

  private def toUntypedProperties(any: Any): Option[RawProperties] = {
    any match {
      case javaMap: JMap[_, _] =>
        val scalaMap = javaMap.asScala.toMap.asInstanceOf[RawProperties]
        Some(scalaMap)
      case _ => None
    }
  }

  private def typecheckProperties(
      scopedName: ScopedName,
      block: String,
      properties: RawProperties,
      config: Config,
      file: Path
  ): Either[Seq[Problem], Unit] = {
    externalCache.typer.cached(scopedName, properties) {
      val reporter = Reporter(block, scopedName, config, file, sourceRoot)
      Typecheck.forProperties(reporter, expectedTypes, properties)
      testSuitesSupport.typecheck(properties, reporter)
      scriptTemplatesSupport.typecheck(properties, reporter)
      NativeLibrariesSupport.typecheck(properties, reporter)
      reporter.toEither(())
    }
  }

  private def buildUnresolvedRunConf(block: Block, scopedName: ScopedName, map: RawProperties): UnresolvedRunConf = {
    externalCache.buildUnresolved.cached(scopedName, (block, map)) {
      val extractor = new PropertyExtractor(map)
      import extractor._

      val isTest = extractBoolean(names.isTest).getOrElse(false) ||
        block == names.testsBlock ||
        scopedName.name == names.TestDefaults

      UnresolvedRunConf(
        id = scopedName.id,
        name = scopedName.name,
        scopeType = extractString(names.scope) orElse extractString(names.moduleName),
        condition = extractAny(names.condition).map(_.toString),
        mainClass = extractString(names.mainClass),
        mainClassArgs = extractSeq(names.mainClassArgs),
        javaOpts = extractSeq(names.javaOpts),
        env = extractEnvMap(names.env),
        agents = extractSeq(names.agents),
        isTemplate = block == names.templatesBlock,
        parents = extractSeq(names.parents),
        isTest = isTest,
        packageName = extractString(names.packageName),
        methodName = extractString(names.methodName),
        launcher = extractString(names.launcher).flatMap(Launcher.fromString),
        allowDTC = extractBoolean(names.allowDTC),
        includes = extractSeq(names.includes),
        excludes = extractSeq(names.excludes),
        maxParallelForks = extractInt(names.maxParallelForks),
        forkEvery = extractInt(names.forkEvery),
        suites = testSuitesSupport.extractTyped(properties),
        scriptTemplates = scriptTemplatesSupport.extractTyped(properties),
        nativeLibraries = NativeLibrariesSupport.extractTyped(properties),
        extractTestClasses = extractBoolean(names.extractTestClasses),
        appendableEnv = extractEnvMap(names.appendableEnv),
        moduleLoads = extractSeq(names.moduleLoads),
        javaModule = javaModuleSupport.extractTyped(properties),
        credentialGuardCompatibility = extractBoolean(names.credentialGuardCompatibility),
        debugPreload = extractBoolean(names.debugPreload),
        additionalScope = extractString(names.additionalScope),
        treadmillOpts = extractTmOpts(names.treadmillOpts),
        extraExecOpts = extractExtraExecOpts(names.extraExecOpts),
        category = extractString(names.category),
        groups = extractString(names.groups).toSet,
        owner = extractString(names.owner),
        flags = extractMap(names.flags).mapValuesNow(_.toString)
      )
    }
  }

  private def reportUnknownProperties(runConfs: Iterable[RunConfCompilingState]): Unit = {
    runConfs.foreach { conf =>
      UnknownProperties.report(conf.reportError, expectedProperties, conf.untypedProperties)
      testSuitesSupport.reportUnknownProperties(conf)
      NativeLibrariesSupport.reportUnknownProperties(conf)
      scriptTemplatesSupport.reportUnknownProperties(conf)
      javaModuleSupport.reportUnknownProperties(conf)
    }
  }

  private def validateAndAssignParents(
      allRunConfs: Seq[RunConfCompilingState],
      typedRunConfs: Seq[RunConfCompilingState]
  ): Unit = {
    val runConfsByName = allRunConfs.map(conf => conf.scopedName -> conf).toMap

    def reportDuplicatedNames(): Unit = {
      val allRunConfName = allRunConfs
        .withFilter(_.block == names.applicationsBlock)
        .map(conf => conf -> conf.id)
        .collect { case (conf, id: ModuleId) =>
          conf -> (id.metaBundle, conf.name)
        }
        .toMap
      val duplicatedRunconfs = allRunConfName
        .groupBy(_._2)
        .collect {
          case (_, v) if v.size > 1 => v.keySet
        }
        .flatten
      duplicatedRunconfs.foreach(conf => conf.reportError.atName(Messages.duplicatedRunconfName(conf.name)))
    }

    def addExplicitParents(): Unit = {
      typedRunConfs.foreach { conf =>
        conf.runConf.parents.foreach { scopedParentName =>
          val scopedParent = ScopedName.parse(scopedParentName)
          val parentId = if (scopedParent.id == WorkspaceId) conf.id else scopedParent.id
          if (scopedParent.name == conf.name) {
            conf.reportError.atParent(scopedParentName, Messages.selfInheritance)
          } else {
            val resolvedParent = runConfsByName
              .get(ScopedName(parentId, scopedParent.name))
              .orElse(runConfsByName.get(ScopedName(WorkspaceId, scopedParent.name)))
            resolvedParent match {
              case Some(parent) =>
                conf.appendParent(parent)
              case None =>
                conf.reportError.atParent(scopedParentName, Messages.noSuchRunConf(scopedParentName))
            }
          }
        }
      }
    }

    def addImplicitParents(): Unit = {
      val unsafeToLookup = findCycles(typedRunConfs).flatten.toSet

      def allParents(conf: RunConfCompilingState): Seq[RunConfCompilingState] = {
        val safeParents = conf.parents.filterNot(unsafeToLookup)
        safeParents ++ safeParents.flatMap(allParents)
      }

      def possibleTestParents(conf: RunConfCompilingState): Seq[RunConfCompilingState] = {
        val parentIds = conf.id.parents
        parentIds.flatMap(id => runConfsByName.get(ScopedName(id, conf.name)))
      }

      def isTest(conf: RunConfCompilingState): Boolean = {
        def isDefinedAsTest = conf.isTyped && conf.runConf.isTest

        def explicitParentIsTest = allParents(conf).exists(isTest)

        def implicitParentCandidateIsTest = possibleTestParents(conf).exists(isTest)

        isDefinedAsTest || explicitParentIsTest || implicitParentCandidateIsTest
      }

      def defaultRunConfParents(conf: RunConfCompilingState): Seq[RunConfCompilingState] = {
        defaultParents(conf, names.RunConfDefaults)
      }

      def defaultAppParents(conf: RunConfCompilingState): Seq[RunConfCompilingState] = {
        defaultRunConfParents(conf) ++ defaultParents(conf, names.ApplicationDefaults)
      }

      def defaultTestParents(conf: RunConfCompilingState): Seq[RunConfCompilingState] = {
        defaultRunConfParents(conf) ++ defaultParents(conf, names.TestDefaults)
      }

      def defaultParents(conf: RunConfCompilingState, name: String): Seq[RunConfCompilingState] = {
        val allIds = if (conf.name != name) conf.id.withParents else conf.id.parents
        allIds.flatMap(id => runConfsByName.get(ScopedName(id, name)))
      }

      def isApp(conf: RunConfCompilingState) = {
        !conf.runConf.isTemplate || conf.name == names.ApplicationDefaults
      }

      def isRunConfDefaults(conf: RunConfCompilingState) = {
        conf.name == names.RunConfDefaults
      }

      typedRunConfs.foreach { conf =>
        if (isTest(conf)) {
          conf.prependParents(defaultTestParents(conf) ++ possibleTestParents(conf))
        } else if (isApp(conf)) {
          conf.prependParents(defaultAppParents(conf))
        } else if (isRunConfDefaults(conf)) {
          conf.prependParents(defaultRunConfParents(conf))
        }
      }
    }

    def findAndReportCyclicDependencies(): Unit = {
      val cycles = findCycles(typedRunConfs)
      cycles.foreach { cycle =>
        val namesInCycle = cycle.map(s => s.scopedName).toSet
        val errorMessage = Messages.cyclicInheritance(namesInCycle)
        cycle.foreach { cycleElement =>
          val declaredParents = cycleElement.parents.map(s => s.scopedName)
          declaredParents.foreach { declaredParentName =>
            if (namesInCycle.contains(declaredParentName)) {
              cycleElement.reportError.atParent(declaredParentName.name, errorMessage)
            }
          }
        }
      }
    }

    addExplicitParents()
    addImplicitParents()
    if (reportDuplicates)
      reportDuplicatedNames()
    findAndReportCyclicDependencies()
  }

  private def findCycles(typedRunConfs: Seq[RunConfCompilingState]): Seq[Seq[RunConfCompilingState]] = {
    TopologicalSort.sort(typedRunConfs)((node, visitDep) => node.parents.foreach(visitDep)).loops
  }

  private def resolveRunConfs(
      unresolvedConfigs: Seq[(Config, ParentId)],
      typedRunConfs: Seq[RunConfCompilingState]
  ): Unit = {
    val configsByScope = unresolvedConfigs
      .groupBy { case (_, scope) => scope }
      .map { case (scope, configsWithScopes) =>
        scope -> configsWithScopes.map { case (config, _) => config }.reduce(_ withFallback _)
      }
    typedRunConfs.foreach(resolve(configsByScope, _))
  }

  private def resolve(unresolvedConfigs: Map[ParentId, Config], conf: RunConfCompilingState): Unit = {
    if (!conf.isResolved) {
      conf.parents match {
        case Seq() =>
          resolveFlatConfig(conf)
        case parents =>
          parents.foreach(parent => if (!parent.isResolved && !parent.hasErrors) resolve(unresolvedConfigs, parent))
          parents.foreach(_.markAsUsed())
          val parentsWithProblems = parents.filter(_.hasErrors)
          if (parentsWithProblems.nonEmpty) {
            parentsWithProblems.foreach { parent =>
              reportOnParent(conf, parent.name, Messages.parentHasErrors)
            }
            conf.resolvedAsIncorrect()
          } else {
            val mergedConf = applyParents(unresolvedConfigs, conf, parents)
            conf.update(mergedConf)
            resolveFlatConfig(conf)
          }
      }
    }
  }

  private def reportOnParent(
      conf: RunConfCompilingState,
      parentName: String,
      message: String
  ): Unit = {
    if (conf.runConf.parents.contains(parentName)) {
      conf.reportError.atParent(parentName, message)
    } else {
      conf.reportError.atName(Messages.atImplicitParent(parentName, message))
    }
  }

  private def applyParents(
      unresolvedConfigs: Map[ParentId, Config],
      conf: RunConfCompilingState,
      parents: Seq[RunConfCompilingState]
  ): UnresolvedRunConf = {
    def isApplicable(conf: RunConfCompilingState): Boolean = {
      // resolving config removes all properties with condition set to unresolved value
      val rawPath = ConfigUtil.joinPath(conf.block, conf.name, names.condition)
      val hasConditionProperty = unresolvedConfigs(conf.id).hasPathOrNull(rawPath)
      !hasConditionProperty || conf.runConf.condition.map(_.trim).exists(cond => cond.nonEmpty && cond != "false")
    }

    val applicableParents = parents.filter(isApplicable)
    val applyChain = (applicableParents :+ conf).map(_.runConf)
    externalCache.applyParents.cached(conf.scopedName, applyChain)(applyChain.reduceLeft(applyProperties))
  }

  private def applyProperties(target: UnresolvedRunConf, source: UnresolvedRunConf): UnresolvedRunConf = {
    val merger = new Merger(target, source)
    UnresolvedRunConf(
      id = source.id,
      name = source.name,
      scopeType = merger.merge(_.scopeType),
      condition = source.condition, // OPTIMUS-39104: inheriting condition is not correct
      mainClass = merger.merge(_.mainClass),
      mainClassArgs = merger.merge(_.mainClassArgs),
      javaOpts = merger.merge(_.javaOpts),
      env = merger.merge(_.env),
      agents = merger.mergeDistinct(_.agents),
      isTemplate = source.isTemplate,
      isTest = target.isTest || source.isTest,
      parents = source.parents, // so that we know which parents was the config defined with
      packageName = merger.merge(_.packageName),
      methodName = merger.merge(_.methodName),
      launcher = merger.merge(_.launcher),
      allowDTC = merger.merge(_.allowDTC),
      includes = merger.mergeDistinct(_.includes),
      excludes = merger.mergeDistinct(_.excludes),
      maxParallelForks = merger.merge(_.maxParallelForks),
      forkEvery = merger.merge(_.forkEvery),
      suites = testSuitesSupport.merge(target.suites, source.suites),
      nativeLibraries = NativeLibrariesSupport.merge(target.nativeLibraries, source.nativeLibraries),
      scriptTemplates = scriptTemplatesSupport.merge(target.scriptTemplates, source.scriptTemplates),
      extractTestClasses = merger.merge(_.extractTestClasses),
      appendableEnv = merger.merge(_.appendableEnv),
      moduleLoads = merger.mergeDistinct(_.moduleLoads),
      javaModule = javaModuleSupport.merge(target.javaModule, source.javaModule),
      credentialGuardCompatibility = merger.merge(_.credentialGuardCompatibility),
      debugPreload = merger.merge(_.debugPreload),
      additionalScope = merger.merge(_.additionalScope),
      treadmillOpts = TreadmillOptionsSupport.mergeTreadmillOptions(target.treadmillOpts, source.treadmillOpts),
      extraExecOpts = ExtraExecOptionsSupport.mergeExtraExecOpts(target.extraExecOpts, source.extraExecOpts),
      category = merger.merge(_.category),
      groups = merger.merge(_.groups.toIndexedSeq).toSet,
      owner = merger.merge(_.owner),
      flags = merger.mergeMaps(_.flags)
    )
  }

  /**
   * Attempts to resolve RunConf assuming that there are no parent configs
   */
  private def resolveFlatConfig(conf: RunConfCompilingState): Unit = {
    import conf.runConf
    dtcSupport.validate(conf)
    validateTestProperties(conf)
    validateBlocks(conf)
    scopeDependentReferences.validate(conf)
    if (conf.hasErrors) {
      conf.resolvedAsIncorrect()
    } else if (runConf.isTemplate) {
      val template = createTemplate(runConf)
      conf.resolvedAsTemplate(template)
    } else if (runConf.isTest) {
      val testRunConf = runConf.scopeType match {
        case Some(scopeType) =>
          createTestRunConf(runConf, scopeType)
        case None =>
          conf.reportError.atName("Each runconf should have scope defined.")
          createTestRunConf(runConf, defaults.defaultTestScopeType)
      }
      if (testRunConf.includes.nonEmpty && testRunConf.mainClass.nonEmpty)
        conf.reportError.atName("Cannot combine 'mainClass' and 'includes' in the single test runconf.")
      conf.resolvedAsRunConf(testRunConf)
    } else
      runConf.mainClass match {
        case Some(mainClass) =>
          val appRunConf = createAppRunConf(runConf, mainClass)
          validator.map { _.validationErrors(appRunConf) } match {
            case Some(errors) if errors.nonEmpty =>
              errors.foreach(error => conf.reportError.atName(s"${runConf.name}: $error"))
              conf.resolvedAsIncorrect()
            case _ => conf.resolvedAsRunConf(appRunConf)
          }
        case None =>
          conf.reportError.atName(Messages.appRunConfsRequiresMainClass)
          conf.resolvedAsIncorrect()
      }
  }

  private def validateTestProperties(conf: RunConfCompilingState): Unit = {
    val runConf = conf.runConf

    def propertyNonEmpty(name: String): Boolean = name match {
      case names.packageName              => runConf.packageName.isDefined
      case names.mainClass                => runConf.mainClass.isDefined
      case names.methodName               => runConf.methodName.isDefined
      case names.includes                 => runConf.includes.nonEmpty
      case names.excludes                 => runConf.excludes.nonEmpty
      case names.maxParallelForks         => runConf.maxParallelForks.isDefined
      case names.forkEvery                => runConf.forkEvery.isDefined
      case names.extractTestClasses       => runConf.extractTestClasses.isDefined
      case TestSuitesSupport.names.suites => runConf.suites.nonEmpty
      case names.treadmillOpts            => runConf.treadmillOpts.isDefined
      case names.`extraExecOpts`          => runConf.extraExecOpts.isDefined
      case names.category                 => runConf.category.isDefined
      case names.owner                    => runConf.owner.isDefined
      case names.flags                    => runConf.flags.nonEmpty
    }

    def isDefined(propertyName: String): Boolean = {
      propertyNonEmpty(propertyName) || conf.untypedProperties.contains(propertyName)
    }

    def ensureLegalSetsOfTestProperties(): Unit = {
      forbiddenSetsOfTestProperties.foreach { case (property, forbidden) =>
        if (isDefined(property)) {
          val badFields = forbidden.filter(isDefined)
          if (badFields.nonEmpty) {
            val allBadFields = property +: badFields
            val message = Messages.invalidSetOfProperties(allBadFields.toSet)
            allBadFields.foreach { badField =>
              conf.reportError.atKey(badField, message)
            }
          }
        }
      }
    }

    def ensureNoAppOnlyProperties(): Unit = {
      def validate(name: String, isDefined: UnresolvedRunConf => Boolean): Unit = {
        if (isDefined(conf.runConfWithoutParents)) {
          conf.reportError.atKey(name, Messages.propertyOnlyAllowedForApps(name))
        } else if (isDefined(conf.runConf)) {
          val badParents = conf.parents.filter(p => isDefined(p.runConf)).map(_.name)
          badParents.foreach { parent =>
            reportOnParent(conf, parent, Messages.testInheritsFromApp(name))
          }
        }
      }

      validate(names.mainClassArgs, _.mainClassArgs.nonEmpty)
      validate(names.appendableEnv, _.appendableEnv.nonEmpty)
      validate(names.moduleLoads, _.moduleLoads.nonEmpty)
    }

    def ensureNoTestOnlyProperties(): Unit = {
      testOnlyProperties.foreach { property =>
        if (isDefined(property)) {
          conf.reportError.atKey(property, Messages.propertyOnlyAllowedForTests(property))
        }
      }
    }

    if (runConf.isTest) {
      ensureLegalSetsOfTestProperties()
      ensureNoAppOnlyProperties()
    } else if (runConf.isApp) {
      ensureNoTestOnlyProperties()
    }
  }

  private def validateBlocks(conf: RunConfCompilingState): Unit = {
    if (conf.block == names.applicationsBlock && conf.runConf.isTest) {
      if (conf.untypedProperties.contains(names.isTest)) {
        conf.reportError.atValue(names.isTest, Messages.testInApplicationsBlock)
      } else {
        conf.reportError.atName(Messages.testInApplicationsBlock)
      }
    }
  }

  private def createAppRunConf(runConf: UnresolvedRunConf, mainClass: String): AppRunConf = {
    AppRunConf(
      scope(runConf.id, runConf.scopeType.getOrElse(defaults.defaultAppScopeType)),
      runConf.name,
      mainClass,
      runConf.mainClassArgs,
      runConf.javaOpts,
      resolveEnv(runConf),
      runConf.agents,
      runConf.launcher.getOrElse(defaults.launcher),
      runConf.nativeLibraries,
      runConf.moduleLoads,
      runConf.javaModule,
      runConf.scriptTemplates,
      runConf.credentialGuardCompatibility.getOrElse(defaults.defaultCredentialGuardCompatibility),
      runConf.debugPreload.getOrElse(defaults.defaultDebugPreload),
      runConf.additionalScope.filter(_.nonEmpty).map(ScopeId.parse)
    )
  }

  private def createTestRunConf(runConf: UnresolvedRunConf, scopeType: String): TestRunConf = {
    TestRunConf(
      id = scope(runConf.id, scopeType),
      name = runConf.name,
      env = resolveEnv(runConf),
      javaOpts = runConf.javaOpts,
      packageName = runConf.packageName,
      mainClass = runConf.mainClass,
      methodName = runConf.methodName,
      agents = runConf.agents,
      launcher = runConf.launcher.getOrElse(defaults.launcher),
      includes = runConf.includes,
      excludes = runConf.excludes,
      maxParallelForks = runConf.maxParallelForks.getOrElse(defaults.maxParallelForks),
      forkEvery = runConf.forkEvery.getOrElse(defaults.forkEvery),
      nativeLibraries = runConf.nativeLibraries,
      extractTestClasses = runConf.extractTestClasses.getOrElse(false),
      javaModule = runConf.javaModule,
      credentialGuardCompatibility =
        runConf.credentialGuardCompatibility.getOrElse(defaults.defaultCredentialGuardCompatibility),
      debugPreload = runConf.debugPreload.getOrElse(defaults.defaultDebugPreload),
      suites = runConf.suites,
      additionalScope = runConf.additionalScope.filter(_.nonEmpty).map(ScopeId.parse),
      treadmillOpts = runConf.treadmillOpts,
      extraExecOpts = runConf.extraExecOpts,
      category = runConf.category,
      groups = runConf.groups,
      owner = runConf.owner,
      flags = runConf.flags
    )
  }

  private def scope(id: ParentId, scopeType: String): ScopeId = id match {
    case m: ModuleId => m.scope(scopeType)
    case x           => throw new RuntimeException(s"'$x' is not a module ID")
  }

  private def createTemplate(runConf: UnresolvedRunConf): Template = {
    Template(
      id = runConf.id,
      scopeType = runConf.scopeType,
      name = runConf.name,
      isTest = runConf.isTest,
      env = resolveEnv(runConf),
      javaOpts = runConf.javaOpts,
      mainClassArgs = runConf.mainClassArgs,
      packageName = runConf.packageName,
      mainClass = runConf.mainClass,
      methodName = runConf.methodName,
      agents = runConf.agents,
      launcher = runConf.launcher.getOrElse(defaults.launcher),
      includes = runConf.includes,
      excludes = runConf.excludes,
      maxParallelForks = runConf.maxParallelForks.getOrElse(defaults.maxParallelForks),
      forkEvery = runConf.forkEvery.getOrElse(defaults.forkEvery),
      nativeLibraries = runConf.nativeLibraries,
      extractTestClasses = runConf.extractTestClasses.getOrElse(false),
      moduleLoads = runConf.moduleLoads,
      javaModule = runConf.javaModule,
      scriptTemplates = runConf.scriptTemplates,
      credentialGuardCompatibility =
        runConf.credentialGuardCompatibility.getOrElse(defaults.defaultCredentialGuardCompatibility),
      debugPreload = runConf.debugPreload.getOrElse(defaults.defaultDebugPreload),
      suites = runConf.suites,
      additionalScope = runConf.additionalScope,
      treadmillOpts = runConf.treadmillOpts,
      extraExecOpts = runConf.extraExecOpts,
      category = runConf.category,
      groups = runConf.groups,
      owner = runConf.owner,
      flags = runConf.flags
    )
  }

  private def resolveEnv(runConf: UnresolvedRunConf): Map[String, String] = {
    val convertedEnv = envInternalToExternal(runConf.env)
    val convertedAppendableEnv = envInternalToExternal(runConf.appendableEnv)

    val appendEnv = runEnv match {
      case _: RunEnv.Ide => convertedAppendableEnv
      case env: RunEnv.Cmdline =>
        convertedAppendableEnv.map { case (name, value) =>
          name -> s"$value ${env.os.makeVar(name)}"
        }
    }
    convertedEnv ++ appendEnv
  }

  private def postResolveSteps(
      installOverride: Option[Path],
      typedRunConfs: Seq[ResolvedRunConfCompilingState]
  ): Unit = {
    typedRunConfs.foreach { conf: ResolvedRunConfCompilingState =>
      scopeDependentReferences.resolve(conf, installOverride)
      dtcSupport.apply(conf)
      testSuitesSupport.apply(conf)
      warnOnUnusedTemplate(conf)
      normalizeJavaOpts(conf)
      normalizeCliArgs(conf)
    }
  }

  private def warnOnUnusedTemplate(conf: ResolvedRunConfCompilingState): Unit = {
    if (conf.runConf.isTemplate && !conf.isUsed) {
      conf.reportWarning.atName(Messages.unusedTemplate)
    }
  }

  private def normalizeCliArgs(conf: ResolvedRunConfCompilingState): Unit = {
    conf.transformRunConf {
      case app: AppRunConf =>
        app.copy(mainClassArgs = CliArgs.normalize(app.mainClassArgs))
      case test: TestRunConf => test
    }
    conf.transformTemplate { template =>
      template.copy(mainClassArgs = CliArgs.normalize(template.mainClassArgs))
    }
  }

  private def normalizeJavaOpts(conf: ResolvedRunConfCompilingState): Unit = {
    def normalize(javaOpts: Seq[String], javaVersionOverride: Option[String]): Seq[String] = {
      val effectiveJVO = reallyRealJavaVersionOverride orElse javaVersionOverride
      externalCache.javaOpts.cached(conf.scopedName -> effectiveJVO, javaOpts) {
        JavaOpts.normalize {
          filterJavaRuntimeOptions(conf.reportWarning, javaOpts, effectiveJVO)
        }
      }
    }

    conf.transformRunConf {
      case app: AppRunConf =>
        app.copy(javaOpts = normalize(app.javaOpts, app.javaModule.version))
      case test: TestRunConf =>
        test.copy(javaOpts = normalize(test.javaOpts, test.javaModule.version))
    }

    conf.transformTemplate { template =>
      template.copy(javaOpts = normalize(template.javaOpts, template.javaModule.version))
    }
  }

  private def toCompileResult(allRunConfs: Seq[RunConfCompilingState]): CompileResult = {
    val problems = globalReporter.problems ++ allRunConfs.flatMap(_.allProblems)
    val runConfs = allRunConfs.flatMap(_.resolvedRunConf).sortBy(rc => (rc.id.properPath, rc.name))
    val templates = allRunConfs.flatMap(_.resolvedTemplate).sortBy(_.scopedName.properPath.toLowerCase)
    CompileResult(problems, runConfs, templates)
  }
}
