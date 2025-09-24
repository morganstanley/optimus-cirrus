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

import java.util.regex.PatternSyntaxException
import com.typesafe.config.Config
import ConfigUtils._
import com.typesafe.config.ConfigFactory
import com.typesafe.config.ConfigList
import com.typesafe.config.ConfigObject
import com.typesafe.config.ConfigUtil
import com.typesafe.config.ConfigValue
import com.typesafe.config.ConfigValueType
import optimus.buildtool.config.AllDependencies
import optimus.buildtool.config.CppBuildConfiguration
import optimus.buildtool.config.CppConfiguration
import optimus.buildtool.config.CppConfiguration.OutputType
import optimus.buildtool.config.CppToolchain
import optimus.buildtool.config.Dependencies
import optimus.buildtool.config.DependencyDefinition
import optimus.buildtool.config.DependencyId
import optimus.buildtool.config.ForbiddenDependencyConfiguration
import optimus.buildtool.config.Id
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.config.ModuleSet
import optimus.buildtool.config.ModuleSetId
import optimus.buildtool.config.NamingConventions._
import optimus.buildtool.config.NativeDependencyDefinition
import optimus.buildtool.config.ScalacConfiguration
import optimus.buildtool.config.ScopeConfiguration
import optimus.buildtool.config.ScopeFlags
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.ScopePaths
import optimus.buildtool.dependencies.CentralDependencies
import optimus.buildtool.dependencies.JvmDependenciesLoader
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.Keys.KeySet
import optimus.buildtool.utils.OsUtils
import optimus.buildtool.utils.PathUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory.getLogger
import spray.json._

import java.util.concurrent.ConcurrentHashMap
import scala.util.matching.Regex
import scala.collection.compat._
import scala.jdk.CollectionConverters._
import scala.collection.mutable

final case class CustomScopesDefinition(
    include: Seq[Regex],
    exclude: Seq[Regex],
    includeDownstreamsOf: Seq[Regex],
    excludeCircularDownstreams: Boolean,
    compile: Boolean,
    id: ScopeId,
    origin: ObtFile,
    line: Int
)

class ScopeDefinitionCompiler(
    loadConfig: ObtFile.Loader,
    centralDependencies: CentralDependencies,
    structure: WorkspaceStructure,
    toolchains: Map[String, CppToolchain],
    cppOsVersions: Seq[String],
    useMavenLibs: Boolean = false
) {
  import ScopeDefinitionCompiler._

  private val moduleSetsByModule: Map[ModuleId, (ModuleSetDefinition, Boolean)] =
    structure.moduleSets.flatMap { case (_, moduleSet) =>
      moduleSet.modules.map(module => module.id -> (moduleSet, module.public))
    }

  private val transitiveCanDependOn: Map[ModuleSetId, Set[ModuleSetId]] = {
    def transitiveCdo(moduleSet: ModuleSetId, accum: mutable.Set[ModuleSetId]): Unit = {
      // avoid walking the same path repeatedly (also prevents stack overflows due to circular dependencies)
      if (accum.add(moduleSet)) {
        structure.moduleSets(moduleSet).canDependOn.foreach { case (id, _) =>
          transitiveCdo(id, accum)
        }
      }
    }
    structure.moduleSets.keySet.map { id =>
      val set = mutable.Set[ModuleSetId]()
      transitiveCdo(id, set)
      id -> set.toSet
    }.toMap
  }

  private def expandParents(scopes: Map[ScopeId, ScopeDefinition]): Result[Map[ScopeId, ScopeDefinition]] = {
    val problems = List.newBuilder[Message]

    def expandParentScope(scope: ScopeDefinition): ScopeDefinition = {
      val parentScopes = scope.parents.map(scope.module.id.scope).filter(scopes.contains)
      if (parentScopes.isEmpty) scope
      else {
        val (valid, closed) = parentScopes.map(scopes(_)).partition(_.configuration.open)
        problems ++= closed.map { closedScope =>
          val msg = s"Scope ${scope.id} extends closed scope ${closedScope.id}. " +
            s"This should be fixed by adding 'open = true' to ${closedScope.id}"
          Error(msg, scope.module, scope.line)
        }
        val cfg = scope.configuration
        val newCompile = cfg.compileDependencies
          .copy(
            modules = valid.map(_.id) ++ cfg.compileDependencies.modules
          )
          .distinct
        val newRuntime = cfg.runtimeDependencies
          .copy(
            modules = valid.map(_.id) ++ cfg.runtimeDependencies.modules
          )
          .distinct
        val newDeps = cfg.dependencies.copy(compileDependencies = newCompile, runtimeDependencies = newRuntime)
        scope.copy(configuration = scope.configuration.copy(dependencies = newDeps))
      }
    }

    val expandedScopes = scopes.map { case (k, v) => k -> expandParentScope(v) }
    Success(expandedScopes, problems.result())
  }

  private def expandAllCustomScopes(scopesToExpand: Map[ScopeId, ScopeDefinition]): Map[ScopeId, ScopeDefinition] = {
    val allScopes = scopesToExpand.keySet.to(Seq).sorted

    def expand(scope: ScopeDefinition, csd: CustomScopesDefinition)(f: ScopeId => Boolean): ScopeDefinition = {
      val cfg = scope.configuration
      val exc = csd.exclude

      val scopesToAdd = allScopes.filter { scopeId =>
        val properPath = scopeId.properPath
        csd.id != scopeId && f(scopeId) && exc.forall(_.unapplySeq(properPath).isEmpty)
      }
      val dependenciesToAdd = Dependencies(scopesToAdd, Nil, Nil)
      val newDeps =
        if (csd.compile) cfg.dependencies appendCompile dependenciesToAdd
        else cfg.dependencies appendRuntime dependenciesToAdd
      val newRels =
        scopesToAdd.map(d => ScopeRelationship(d, custom = true, csd.origin, csd.line))
      scope.copy(
        configuration = cfg.copy(dependencies = newDeps),
        relationships = scope.relationships ++ newRels
      )
    }

    // first expand all the includes, and then afterwards expand the downstream includes. we do these in two phases
    // so that we can avoid cycles due to the includes in the dependency graph.
    val partiallyExpanded = scopesToExpand.map { case (id, scope) =>
      id -> scope.customScopesDefinitions.foldLeft(scope) { case (scope, csd) =>
        val inc = csd.include

        expand(scope, csd) { candidateScopeId =>
          val properPath = candidateScopeId.properPath
          inc.exists(_.unapplySeq(properPath).isDefined)
        }
      }
    }

    def reverseGrouped(ids: Seq[(ScopeId, ScopeId)]) =
      ids.groupBy(_._2).map { case (target, rels) => target -> rels.map(_._1).toSet }

    def allDownstreams(s: ScopeId, reverseDeps: Map[ScopeId, Set[ScopeId]]): Set[ScopeId] = {
      val accum = mutable.Set[ScopeId]()
      def inner(target: ScopeId): Unit = {
        if (accum.add(target)) {
          reverseDeps.getOrElse(target, Set.empty[ScopeId]).foreach(inner)
        }
      }
      inner(s)
      accum.toSet
    }

    val allRelationships =
      partiallyExpanded.toSeq.flatMap { case (source, scope) => scope.relationships.map(source -> _.target) }
    val reverseDependencies = reverseGrouped(allRelationships)

    val newCandidateReverseDependencies = reverseGrouped(for {
      (id, scope) <- partiallyExpanded.toSeq
      csd <- scope.customScopesDefinitions
      ido <- csd.includeDownstreamsOf
      idoScope <- allScopes.filter(candidateId => ido.matches(candidateId.properPath))
      idoDownstream <- allDownstreams(idoScope, reverseDependencies)
    } yield id -> idoDownstream)

    // if we don't try to prevent cycles due to colliding `includeDownstreamsOf`, there are the relationships we'll get
    val allPotentialReverseDependencies =
      (reverseDependencies.keySet ++ newCandidateReverseDependencies.keySet).map { id =>
        id -> (reverseDependencies.getOrElse(id, Set.empty) ++ newCandidateReverseDependencies.getOrElse(id, Set.empty))
      }.toMap

    partiallyExpanded.map { case (id, scope) =>
      id -> scope.customScopesDefinitions.foldLeft(scope) { case (scope, csd) =>
        val ido = csd.includeDownstreamsOf
        val includedDownstreamScopes = if (ido.nonEmpty) {

          val excludedDownstreams = if (csd.excludeCircularDownstreams) {
            // exclude this scope and any of its potential downstreams to prevent a cycle, at the cost
            // of potentially excluding scopes we might be interested in
            allDownstreams(scope.id, allPotentialReverseDependencies)
          } else {
            // only exclude other colliding `includeDownstreamsOf`
            allDownstreams(scope.id, newCandidateReverseDependencies)
          }

          val scopes = allScopes
            .filter { scopeId =>
              val properPath = scopeId.properPath
              ido.exists(_.unapplySeq(properPath).isDefined)
            }
            .flatMap(allDownstreams(_, reverseDependencies))
            .filterNot(excludedDownstreams)
            .toSet
          scopes
        } else Set.empty[ScopeId]

        expand(scope, csd) { candidateScopeId =>
          includedDownstreamScopes.contains(candidateScopeId)
        }
      }
    }
  }

  def compile(workspaceSrcRoot: Directory): Result[Map[ScopeId, ScopeDefinition]] = {
    val bundleDefaults: Result[Map[MetaBundle, ScopeDefaults]] = for {
      workspaceDefaults <- loadDefaults(WorkspaceDefaults, ScopeDefaults.empty)
      bundleDefaults <- loadBundleDefaults(workspaceDefaults)
    } yield bundleDefaults

    val scopes = for {
      defaults <- ResultSeq.single(bundleDefaults)
      module: Module <- ResultSeq(Success(structure.modules.values.to(Seq)))
      scope <- ResultSeq(loadScopes(module, defaults(module.id.metaBundle), workspaceSrcRoot))
    } yield scope

    val allScopes = for {
      scopes <- scopes.value
      scopesById = scopes.map(s => s.id -> s).toMap
      // `enrichScopes` needs to happen before `expandAllCustomScopes`, so that it can see the non-main -> main rels
      enriched = enrichScopes(scopesById)
      withParents <- expandParents(enriched)
      withCustom = expandAllCustomScopes(withParents)
    } yield withCustom

    if (!allScopes.hasErrors) {
      allScopes.withProblems { scopes =>
        val scopeModules = scopes.keySet.map(_.fullModule)
        val missingModuleProblems = (structure.modules.keySet -- scopeModules)
          .map(structure.modules)
          .map { module =>
            val msg = s"Module ${module.id} has no defined scopes in ${module.path}"
            Error(msg, BundlesConfig, module.line)
          }
          .to(Seq)

        val scopeProblems = checkScopes(scopes)
        missingModuleProblems ++ scopeProblems
      }
    } else allScopes
  }

  private def enrichScopes(scopes: Map[ScopeId, ScopeDefinition]): Map[ScopeId, ScopeDefinition] =
    scopes.map { case (id, scopeDef) =>
      val compileDeps = scopeDef.configuration.compileDependencies
      val runtimeDeps = scopeDef.configuration.runtimeDependencies

      // We unconditionally add "main" to all non-main scopes, for convenience
      val (mainScope, mainRels): (Seq[ScopeId], Seq[ScopeRelationship]) = {
        val mainScope = id.copy(tpe = "main")
        if (id.tpe != "all" && !id.isMain && scopes.contains(mainScope)) {
          (Seq(mainScope), Seq(ScopeRelationship(mainScope, custom = false, scopeDef.module, 0)))
        } else (Nil, Nil)
      }

      // We need the testworker on the classpath of any test scopes, so that tests can be launched by OTR.
      // Previously this was done by adding a reference to the test scope to every other non-main scope, and
      // explicitly adding the testworker jar to the test scope in workspace.obt. In the interest of flattening
      // the dependency graph, we no longer do that; instead, to ensure that the testworker jar is still present,
      // we add it implicitly for non-main scopes here.
      val (testWorker, testWorkerRels) = {
        val testWorkerExists = scopes.contains(testworkerScope) // potentially false for non-codetree workspaces
        if (testWorkerExists && id.isTest) {
          (Seq(testworkerScope), Seq(ScopeRelationship(testworkerScope, custom = false, scopeDef.module, 0)))
        } else (Nil, Nil)
      }

      id -> scopeDef.copy(
        configuration = scopeDef.configuration.copy(
          dependencies = scopeDef.configuration.dependencies.copy(
            compileDependencies = compileDeps.copy(modules = (compileDeps.modules ++ mainScope).distinct),
            runtimeDependencies = runtimeDeps.copy(modules = (runtimeDeps.modules ++ mainScope ++ testWorker).distinct)
          )
        ),
        relationships = (scopeDef.relationships ++ mainRels ++ testWorkerRels).distinct
      )
    }

  private def checkScopes(scopes: Map[ScopeId, ScopeDefinition]): Seq[Message] = {
    val modules = scopes.keySet.map(_.fullModule)
    val relationships = scopes.to(Seq).flatMap { case (id, scope) => scope.relationships.map(id -> _) }
    relationships
      .flatMap { case (sourceId, rel) =>
        val source = scopes(sourceId)
        val sourceModuleSet = source.configuration.moduleSet.id

        def mavenOnly(scope: ScopeDefinition): Boolean = scope.configuration.flags.mavenOnly

        def mavenCompatible(scope: ScopeDefinition): Boolean = {

          // check all transitive modules also provided mavenLibs =[]
          def transitiveMavenCompatibleOrEmpty: Boolean = {
            val deps = scope.configuration.dependencies
            val allDepsWithoutNative =
              deps.compileDependencies ++ deps.compileOnlyDependencies ++ deps.runtimeDependencies
            val transitiveIds = allDepsWithoutNative.modules
            val allExternalLibsCompatible =
              deps.externalNativeDependencies.isEmpty && allDepsWithoutNative.hasMavenLibsOrEmpty

            if (!allExternalLibsCompatible) {
              val nativeInfo =
                if (deps.externalNativeDependencies.isEmpty) ""
                else s"native deps: ${deps.externalNativeDependencies.map(_.name).mkString(", ")}"
              val depsInfo =
                if (allDepsWithoutNative.hasMavenLibsOrEmpty) ""
                else s"predefined libs: ${allDepsWithoutNative.dualExternalDeps.map(_.key).mkString(", ")}"
              log.warn(s"${scope.id} is not maven compatible! $nativeInfo $depsInfo")
            }

            val transitiveIdsCompatible =
              if (transitiveIds.isEmpty) true
              else {
                val searchedMap = mutable.Map[ScopeId, Boolean]()
                transitiveIds.foreach { id =>
                  searchedMap.getOrElseUpdate(
                    id, {
                      val newSearch = scopes(id)
                      newSearch != scope && mavenCompatible(newSearch)
                    })
                }
                searchedMap.values.forall(_ == true)
              }

            transitiveIdsCompatible && allExternalLibsCompatible
          }

          scope.configuration.flags.mavenOnly || transitiveMavenCompatibleOrEmpty
        }

        def javaRel(d: ScopeDefinition): Int = d.configuration.javacConfig.release

        def error(msg: String) = Error(msg, rel.origin, rel.line)

        def problem(msg: String, error: Boolean) =
          if (error) Error(msg, rel.origin, rel.line) else Warning(msg, rel.origin, rel.line)

        val checks = Seq[PartialFunction[Option[ScopeDefinition], Seq[Message]]](
          {
            case None if modules.contains(rel.target.fullModule) =>
              Seq(error(s"Module ${rel.target.fullModule} exists but scope ${rel.target} does not"))
          },
          {
            case None if !modules.contains(rel.target.fullModule) =>
              Seq(error(s"Module ${rel.target.fullModule} does not exist"))
          },
          {
            case Some(target) if !target.configuration.open && !rel.custom =>
              // Note - we allow dependency on closed scopes from customScopes/customModules declaration
              Seq(error(invalidDependencyMsg(target.id.toString, "is not open", sourceId.toString)))
          },
          {
            case Some(target) if mavenOnly(source) && !mavenCompatible(target) =>
              val listLoadedLibs = target.configuration.dependencies.allExternal.listExternalLibs()
              val msg =
                if (!mavenOnly(target))
                  s"is not maven compatible(both libs & mavenLibs defined): $listLoadedLibs"
                else "is not mavenOnly"
              // just warn on cross-maven/AFS dependencies from customScopes/customModules declaration for now
              Seq(problem(invalidDependencyMsg(target.id.toString, msg, sourceId.toString), !rel.custom))
          },
          // TODO (OPTIMUS-58917): clean up special case that allow maven release frontier depends on mavenOnly modules
          {
            case Some(target)
                if (!mavenOnly(source) && source.id.module != MavenReleaseFrontier) && mavenOnly(
                  target) && !rel.custom =>
              // just warn on cross-maven/AFS dependencies from customScopes/customModules declaration for now
              Seq(problem(invalidDependencyMsg(target.id.toString, "is mavenOnly", sourceId.toString), !rel.custom))
          },
          {
            case Some(target) if javaRel(source) < javaRel(target) =>
              // just warn on invalid java versions from customScopes/customModules declaration for now
              Seq(
                problem(
                  invalidDependencyMsg(
                    target.id.toString,
                    s"uses javac.release ${javaRel(target)}",
                    sourceId.toString,
                    s"which uses earlier javac.release ${javaRel(source)}"),
                  !rel.custom
                ))
          },
          {
            case Some(target) if source.configuration.flags.installIvy && !target.configuration.flags.installIvy =>
              Seq(
                error(
                  invalidDependencyMsg(
                    target.id.toString,
                    "has 'installIvy = false'",
                    sourceId.toString,
                    "which has 'installIvy = true'")))
          },
          {
            case Some(target)
                if !transitiveCanDependOn(sourceModuleSet).contains(target.configuration.moduleSet.id)
                  && (!rel.custom || (!source.configuration.flags.allowSetViolatingCustomScopes && !target.configuration.flags.allowSetViolatingCustomScopes)) =>
              // relationships breaking can-depends on rules are only allowed for relationships from customScopes/customModules
              // and only if either the source or the target has `allowSetViolatingCustomScopes = true`
              val sourceModuleSetName = sourceModuleSet.name
              val targetModuleSetName = target.configuration.moduleSet.id.name
              Seq(
                error(invalidDependencyMsg(
                  target.id.toString,
                  s"is in module set $targetModuleSetName",
                  sourceId.toString,
                  s"in module set $sourceModuleSetName ($targetModuleSetName is not a transitive 'can-depend-on' of $sourceModuleSetName)"
                )))
          },
          {
            case Some(target)
                if source.configuration.moduleSet != target.configuration.moduleSet && !target.configuration.flags.public && !rel.custom =>
              // Note - we allow dependency on private scopes from customScopes/customModules declaration
              val sourceModuleSet = source.configuration.moduleSet.id.name
              val targetModuleSet = target.configuration.moduleSet.id.name
              Seq(
                error(
                  invalidDependencyMsg(
                    target.id.toString,
                    s"is private to module set $targetModuleSet",
                    sourceId.toString,
                    s"in module set $sourceModuleSet"
                  )))
          }
        )

        checks.flatMap { check =>
          val target = scopes.get(rel.target)
          check.lift(target).getOrElse(Nil)
        }

      }
      .to(Seq)
      .distinct
  }

  private def loadDefaultsFromConfig(file: ObtFile)(cfg: Config): Result[ScopeDefaults] = {
    val allDepsAndSources: Result[Seq[(String, InheritableScopeDefinition)]] =
      Result
        .traverse(cfg.withoutPath("conditionals").resolve().nested(file)) { case (name, config) =>
          loadInheritableScopeDefinition(file, config, InheritableScopeDefinition.empty, None)
            .map { scopeDef =>
              name -> scopeDef
            }
            .withProblems(config.checkExtraProperties(file, Keys.inheritableScopeDefinition))
        }
        .map(_.to(Seq))

    val conditionals =
      if (!cfg.hasPath("conditionals")) Success(Nil)
      else
        Result.sequence {
          val configurations = cfg.getConfig("conditionals").resolve().nested(file).getOrElse(Seq.empty)
          configurations.map { case (name, config) =>
            val conditionalDefaults = for {
              defaults <- loadDefaultsFromConfig(file)(config.getConfig("configuration"))
            } yield loadConditional(config, name, defaults)

            // ids is a required field unless forbiddenDependencies are defined
            if (config.nestedKeyConfigOrEmpty(file, config, "forbiddenDependencies").nonEmpty) {
              config.optionalValue("ids").orElse(config.optionalValue("module-sets")) match {
                case None => conditionalDefaults
                case Some(confValue) =>
                  conditionalDefaults.withProblems(
                    Seq(
                      file.errorAt(
                        confValue,
                        "Invalid keys: pick only one between 'ids'/'module-sets' and 'forbiddenDependencies'")))
              }
            } else conditionalDefaults.withProblems(config.checkEmptyProperties(file, KeySet("ids")))
          }
        }

    for {
      ds <- allDepsAndSources
      c <- conditionals
    } yield ScopeDefaults(ds.toMap, c.to(Seq).sortBy(_.name))
  }

  private def loadConditional(config: Config, name: String, defaults: ScopeDefaults): ConditionalDefaults = {
    val ids = config.stringListOrEmpty("ids").map(id => Id.parse(id))
    val moduleSets = config.stringListOrEmpty("module-sets").map(id => ModuleSetId(id))
    val exclude = config.hasPath("exclude") && config.getBoolean("exclude")
    ConditionalDefaults(name, ids, moduleSets, defaults, exclude)
  }

  private def loadDefaults(file: ObtFile, parent: ScopeDefaults): Result[ScopeDefaults] =
    loadConfigOrEmpty(file).flatMap(loadDefaultsFromConfig(file)).map(_.withParent(parent))

  private def loadBundleDefaults(workspaceDefaults: ScopeDefaults) = {
    val problems = List.newBuilder[Message]
    val all = structure.bundles.map { bundle =>
      val loaded = loadDefaults(bundle, workspaceDefaults)
      val default = loaded.getOrElse(workspaceDefaults)
      problems ++= loaded.problems
      bundle.id -> default
    }

    Success(all.toMap).withProblems(problems.result())
  }

  private def processDependencyList(
      config: Config,
      name: String,
      error: (String, Int) => Unit,
      allowUnorderedDependencies: Boolean
  )(op: (String, Int) => Unit): Unit =
    if (config.hasPath(name)) {
      val originalList = config.getList(name).asScala.flatMap { value =>
        val line = value.origin().lineNumber()
        value.unwrapped() match {
          case id: String =>
            Some((id, line))
          case _ =>
            error(s"Dependencies should be defined as strings, but got: ${value.valueType()}", line)
            None
        }
      }

      if (allowUnorderedDependencies) {
        originalList.foreach { case (id, line) => op(id, line) }
      } else {
        val multipleDependencies = originalList.groupBy(_._1).filter(_._2.size > 1).keySet
        val sortedList = originalList.sorted

        originalList.zip(sortedList).foreach { case ((origId, line), (sortedId, _)) =>
          if (origId != sortedId) error(s"Dependency $origId is not in correct alphabetical order", line)
          else if (multipleDependencies.contains(origId)) error(s"Dependency $origId is present multiple times", line)
          else op(origId, line)
        }

      }
    }

  private def loadScopeDeps(
      origin: ObtFile,
      dependencyType: String, // eg. "compile", "runtime"
      config: Config,
      parent: InheritableScopeDefinition,
      moduleSet: Option[ModuleSet],
      allowUnorderedDependencies: Boolean
  ): Result[(Dependencies, Seq[ScopeRelationship])] = if (config.hasPath(dependencyType)) {
    val problems = Array.newBuilder[Message]

    def error(msg: String, line: Int): Unit = problems += Error(msg, origin, line)

    def getJvmLibs(libsType: String): Seq[DependencyDefinition] = {
      val libs = Array.newBuilder[DependencyDefinition]
      val fromMavenLibs = libsType.contains(MavenLibsKey)

      val lookup = centralDependencies.jvmDependencies.lookup(moduleSet, fromMavenLibs)
      processDependencyList(config, libsType, error, allowUnorderedDependencies) { (id, loc) =>
        lookup.forId(id) match {
          case loadedDeps if loadedDeps.nonEmpty =>
            if (!fromMavenLibs) libs ++= loadedDeps
            else {
              // check when load mavenLibs we don't have any ivy artifacts
              val (mavenDeps, ivyDeps) = loadedDeps.partition(_.isMaven)
              if (ivyDeps.nonEmpty)
                error(s"[$id] mavenLibs contain non-maven ivy deps:${ivyDeps.mkString(", ")}", loc)
              else if (mavenDeps.isEmpty)
                error(s"[$id] no valid mavenLibs found!", loc)
              else libs ++= loadedDeps
            }
          case _ =>
            val msg =
              if (fromMavenLibs) mavenDepNotDefined(id)
              else depNotDefined(id)

            error(msg, loc)
        }
      }
      libs.result().to(Seq)
    }

    // if parent tpe is mavenOnly or current tpe is mavenOnly, for example all.mavenOnly with main.compile.libs
    val mavenOnly = parent.mavenOnly.getOrElse(false) || config.optionalBoolean(MavenOnlyKey).getOrElse(false)
    val hasLibsKey = config.getObject(dependencyType).containsKey(LibsKey)

    // we don't allow mavenOnly module define libs=[] in their obt file or from global workspace.obt file
    if (mavenOnly && hasLibsKey)
      error(s"cannot use libs[] with mavenOnly=true!", config.getConfig(dependencyType).origin().lineNumber())

    val dependencyIds = Array.newBuilder[ScopeId]
    val relationships = Seq.newBuilder[ScopeRelationship]
    // back compat
    val key = if (config.hasPath(s"$dependencyType.scopes")) s"$dependencyType.scopes" else s"$dependencyType.modules"
    processDependencyList(config, key, error, allowUnorderedDependencies) { (path, line) =>
      val id = ScopeDefinition.loadScopeId(path)
      dependencyIds += id
      relationships += ScopeRelationship(id, custom = false, origin, line)
    }

    val keysToUse =
      if (dependencyType == "web") Keys.webProperties
      else if (dependencyType == "electron") Keys.electronProperties
      else
        origin match {
          case _: Module => Keys.scopeDeps
          case _         => Keys.scopeDepsTemplate
        }

    val dependencies = Dependencies(
      modules = dependencyIds.result().to(Seq),
      libs = getJvmLibs(s"$dependencyType.$LibsKey"),
      mavenLibs = getJvmLibs(s"$dependencyType.$MavenLibsKey")
    )

    val extraLibs = {
      val loaded = getJvmLibs(s"$dependencyType.$ExtraLibsKey")
      val (mavenExtraLibs, afsExtraLibs) = loaded.partition(_.isMaven)
      Dependencies(Nil, libs = afsExtraLibs, mavenLibs = mavenExtraLibs)
    }

    Success((dependencies ++ extraLibs, relationships.result()))
      .withProblems(problems.result().to(Seq))
      .withProblems(config.getObject(dependencyType).toConfig.checkExtraProperties(origin, keysToUse))
  } else Success((Dependencies.empty, Nil))

  // Optimistically assume this is correct
  private[this] val testworkerScope = ScopeId.parse("optimus.buildtool.testworker.main")

  private def loadNativeDeps(origin: ObtFile, config: Config): Result[Seq[NativeDependencyDefinition]] = {
    if (config.hasPath("native")) {
      val problems = Array.newBuilder[Message]

      def error(msg: String, line: Int): Unit =
        problems += Error(msg, origin, line)

      // maven usages should not force to use native dependencies
      val deps = Array.newBuilder[NativeDependencyDefinition]
      if (!useMavenLibs) processDependencyList(config, "native", error, allowUnorderedDependencies = true) {
        (id, loc) =>
          centralDependencies.jvmDependencies.nativeDependencies.get(id) match {
            case Some(dep) => deps += dep
            case None      => error(s"Native dependency $id is not defined", loc)
          }
      }
      Success(deps.result().to(Seq)).withProblems(problems.result().to(Seq))
    } else Success(Nil)
  }

  private def loadInheritableScopeDefinition(
      origin: ObtFile,
      config: Config,
      parent: InheritableScopeDefinition,
      moduleSet: Option[ModuleSet]
  ): Result[InheritableScopeDefinition] =
    Result.tryWith(origin, config) {
      val scopeAllowUnorderedDependencies = config.optionalBoolean("allowUnorderedAndDuplicateDependencies")
      val allowUnorderedDependencies =
        scopeAllowUnorderedDependencies
          .orElse(parent.allowUnorderedAndDuplicateDependencies)
          .getOrElse(true)
      for {
        (compileDeps, compileRels) <-
          loadScopeDeps(origin, "compile", config, parent, moduleSet, allowUnorderedDependencies)
        (compileOnlyDeps, compileOnlyRels) <-
          loadScopeDeps(origin, "compileOnly", config, parent, moduleSet, allowUnorderedDependencies)
        (runtimeDeps, runtimeRels) <-
          loadScopeDeps(origin, "runtime", config, parent, moduleSet, allowUnorderedDependencies)
        (webDeps, webRels) <-
          loadScopeDeps(origin, "web", config, parent, moduleSet, allowUnorderedDependencies)
        (electronDeps, electronRels) <-
          loadScopeDeps(origin, "electron", config, parent, moduleSet, allowUnorderedDependencies)
        nativeDeps <- loadNativeDeps(origin, config)
        scalacConf <- loadScalac(config, origin)
        javacConf <- loadJavac(config, origin)
        cppConf <- loadInheritableCpps(config, origin)
        tokenConf <- config.stringMapOrEmpty("tokens", origin)
        copyFilesConf <- CopyFilesConfigurationCompiler.load(config, origin)
        extensionsConf <- ExtensionConfigurationCompiler.load(config, origin)
        postInstallApps <- loadPostInstallApps(config, origin)
        (extraLibs, extraRels) <-
          loadScopeDeps(origin, "extraLibs", config, parent, moduleSet, allowUnorderedDependencies)
        forbiddenDependencies <- loadForbiddenDependencies(config, origin)
        substitutions <- JvmDependenciesLoader.loadSubstitutions(config, origin)
        interopConf <- InteropConfigurationCompiler.load(config, origin)
      } yield {
        val archiveContentRoots = config.seqOrEmpty("archiveContents")
        InheritableScopeDefinition(
          compile = compileDeps ++ webDeps ++ electronDeps,
          compileOnly = compileOnlyDeps,
          runtime = runtimeDeps,
          native = nativeDeps,
          rawRoot = config.optionalString("root"),
          sourcesRoots = config.seqOrEmpty("sources"),
          resourcesRoots = config.seqOrEmpty("resources"),
          // TODO (OPTIMUS-55205): Remove webSources old name
          webSourcesRoots = config.seqOrEmpty("webSources"),
          electronSourcesRoots = config.seqOrEmpty("electronSources"),
          pythonSourcesRoots = config.seqOrEmpty("pythonSources"),
          archiveContentRoots = archiveContentRoots,
          docRoots = config.seqOrEmpty("docs"),
          scalac = scalacConf,
          javac = javacConf,
          cpp = cppConf,
          parents = config.seqOrEmpty("extends"),
          usePipelining = config.optionalBoolean("usePipelining"),
          resourceTokens = tokenConf,
          copyFiles = copyFilesConf,
          extensions = extensionsConf,
          postInstallApps = postInstallApps,
          empty = config.optionalBoolean("empty"),
          installSources = config.optionalBoolean("installSources"),
          installAppScripts = config.optionalBoolean("requiredAppScripts"),
          installIvy = config.optionalBoolean("installIvy"),
          bundle = config.optionalBoolean("bundle"),
          includeInClassBundle = config.optionalBoolean("includeInClassBundle"),
          mavenOnly = config.optionalBoolean(MavenOnlyKey),
          skipDependencyMappingValidation = config.optionalBoolean("skipDependencyMappingValidation"),
          allowUnorderedAndDuplicateDependencies = scopeAllowUnorderedDependencies,
          allowSetViolatingCustomScopes = config.optionalBoolean("allowSetViolatingCustomScopes"),
          relationships =
            (compileRels ++ compileOnlyRels ++ runtimeRels ++ webRels ++ electronRels ++ extraRels).distinct,
          extraLibs = extraLibs,
          forbiddenDependencies = forbiddenDependencies,
          substitutions = substitutions,
          interop = interopConf
        ).withParent(parent)
      }

    }

  private def checkDefaults(
      id: ScopeId,
      module: Module,
      scopeDefaults: InheritableScopeDefinition,
      mavenOnly: Boolean): Seq[Error] = {
    val allExternalDeps = scopeDefaults.allExternalDependencies
    if (mavenOnly && allExternalDeps.withLibs) {
      val msg =
        s"MavenOnly module $id cannot depend on defaults ${allExternalDeps.listExternalLibs(false)} " +
          s"(in ${WorkspaceDefaults.path.pathString} or bundle.obt)"
      Seq(Error(msg, module))
    } else Nil
  }

  private def loadScopes(
      module: Module,
      defaults: ScopeDefaults,
      workspaceSrcRoot: Directory
  ): Result[Seq[ScopeDefinition]] =
    module.tryWith {
      loadConfig(module).flatMap { config =>
        val newDefaultsRes = if (config.hasPath("all")) {
          loadDefaultsFromConfig(module)(config.getConfig("all").atKey("all"))
            .map(_.withParent(defaults))
        } else Success(defaults)
        val finalDefaults = newDefaultsRes.getOrElse(defaults)

        val res = for {
          (tpe, config) <- ResultSeq(config.withoutPath("all").resolve().nested(module))
          mavenOnly = config.optionalBoolean(MavenOnlyKey).contains(true)
          scopeId = module.id.scope(tpe)
          (moduleSet, public) <- ResultSeq.single(moduleSet(scopeId, config, module))
          scopeDefaults =
            finalDefaults.forScope(
              scopeId,
              moduleSet,
              centralDependencies.jvmDependencies.mavenDefinition,
              useMavenDepsRules = useMavenLibs || mavenOnly,
              useMavenOnlyRules = mavenOnly
            )
          cs <- ResultSeq.single(loadCustomScopesDefinitions(scopeId, config, module))
          scope <- ResultSeq.single(
            loadScope(
              module,
              tpe,
              moduleSet,
              public,
              config,
              scopeDefaults,
              workspaceSrcRoot,
              cs
            ).withProblems(checkDefaults(scopeId, module, scopeDefaults, mavenOnly)))
        } yield scope

        res.value
          .withProblems { scopes =>
            if (scopes.isEmpty) Seq(Error(s"At least one scope should be defined in ${module.id}!", module))
            else Nil
          }
          .withProblems(newDefaultsRes.problems)
      }
    }

  private def loadJarDefinition(config: Config, module: Module): Result[JarDefinition] = {
    if (!config.hasPath("jar")) Success(JarDefinition.default)
    else {
      val jarConfig = config.getObject("jar").toConfig
      jarConfig
        .stringMapOrEmpty("manifest", module)
        .map { manifest =>
          JarDefinition(
            manifest = manifest
          )
        }
        .withProblems(jarConfig.checkExtraProperties(module, Keys.jarDefinition))
    }
  }

  private def loadCustomScopesDefinitions(
      scopeId: ScopeId,
      config: Config,
      module: Module): Result[Seq[CustomScopesDefinition]] = {
    def loadSingle(wholeConfig: Config, path: String, isCompile: Boolean) =
      Result.tryWith(module, wholeConfig) {
        val config = wholeConfig.getConfig(path)
        def loadRegs(name: String): Result[Seq[Regex]] =
          if (!config.hasPath(name)) Success(Nil)
          else {
            val configs: Seq[ConfigValue] = config.getList(name).asScala.to(Seq)
            Result.traverse(configs) {
              case str if str.valueType() == ConfigValueType.STRING =>
                try Success(str.unwrapped().toString.r)
                catch {
                  case e: PatternSyntaxException =>
                    module.failure(str, s"Problem with parsing regex: ${e.getMessage}")
                }
              case other =>
                module.failure(other, s"Entries in $name should be string but has ${other.valueType()}")
            }
          }
        val includes = loadRegs("includes")
        val excludes = loadRegs("excludes")
        val includeDownstreamsOf = loadRegs("includeDownstreamsOf")

        val res = for {
          is <- includes
          es <- excludes
          idos <- includeDownstreamsOf
          ecd = config.optionalBoolean("excludeCircularDownstreams").getOrElse(false)
        } yield CustomScopesDefinition(is, es, idos, ecd, isCompile, scopeId, module, config.origin.lineNumber)

        res
          .withProblems { cs =>
            if (cs.include.isEmpty && cs.includeDownstreamsOf.isEmpty && cs.exclude.isEmpty && !res.hasErrors)
              Seq(module.errorAt(config.root(), s"Missing excludes and includes"))
            else Nil
          }
          .withProblems(config.checkExtraProperties(module, Keys.customModules))
      }

    Result.sequence(for {
      name <- Seq("compile", "runtime")
      path <- Seq(s"$name.customScopes", s"$name.customModules") if config.hasPath(path) // back compat
    } yield loadSingle(config, path, name == "compile"))
  }

  private def loadScope(
      module: Module,
      tpe: String,
      moduleSet: ModuleSet,
      public: Boolean,
      config: Config,
      defaults: InheritableScopeDefinition,
      workspaceSrcRoot: Directory,
      customScopesDefinitions: Seq[CustomScopesDefinition]
  ): Result[ScopeDefinition] = {
    val line = config.origin().lineNumber()

    def checkRoot(config: InheritableScopeDefinition): Seq[Message] =
      if (config.rawRoot.nonEmpty) Nil
      else {
        val msg = s"Module ${module.id}.$tpe does not define root. Probably global config for $tpe is broken."
        Seq(Warning(msg, module, line))
      }

    def allDependencies(
        defn: InheritableScopeDefinition,
        containsMacros: Boolean
    ): AllDependencies = {
      val tagAsMacroDependencyIfNeeded: Dependencies => Dependencies =
        if (containsMacros) d => d.externalMap(_.copy(containsMacros = true)) else identity
      AllDependencies(
        compileDependencies = tagAsMacroDependencyIfNeeded(defn.compile.distinct),
        compileOnlyDependencies = defn.compileOnly.distinct,
        runtimeDependencies = (defn.runtime ++ defn.compile).distinct,
        externalNativeDependencies = defn.native,
        extraLibs = defn.extraLibs.distinct,
        substitutions = defn.substitutions,
        forbiddenDependencies = defn.forbiddenDependencies
      )
    }

    Result
      .tryWith(module, config) {
        val scopeId = module.id.scope(tpe)
        for {
          resolvedConfiguration <- loadInheritableScopeDefinition(module, config, defaults, Some(moduleSet))
            .withProblems(config.checkExtraProperties(module, Keys.scopeDefinition))
          jar <- loadJarDefinition(config, module).withProblems(checkRoot(resolvedConfiguration))
          agentConf <- AgentConfigurationCompiler.load(config, module)
          generatorConfiguration <- GeneratorConfigurationCompiler.load(config, module)
          processorConfiguration <- ProcessorConfigurationCompiler.load(config, module)
          archiveConfiguration <- ArchiveConfigurationCompiler.load(config, module)
          cppConfiguration <- toCpp(module, resolvedConfiguration.cpp)
          webConfiguration <- WebConfigurationCompiler.load(config, module)
          pythonConfiguration <- PythonConfigurationCompiler.load(
            config,
            module,
            centralDependencies.pythonDependencies)
          electronConfiguration <- ElectronConfigurationCompiler.load(config, module)
          javacConfig <- resolvedConfiguration.javac.resolve(scopeId, module, centralDependencies.jdkDependencies)
          interopConfiguration <- InteropConfigurationCompiler.load(config, module)
        } yield {
          val configFile = module.path
          val moduleRoot = configFile.parent
          val scopeRoot = resolvedConfiguration.rawRoot.map(moduleRoot.resolvePath).getOrElse(moduleRoot)

          val List(containsPlugin, definesMacros, containsMacros, jmh) =
            List("isCompilerPlugin", "hasMacros", "implementsMacros", "jmh").map {
              config.booleanOrDefault(_, default = false)
            }

          val isAgent = agentConf.isDefined
          val mavenOnly = resolvedConfiguration.mavenOnly.getOrElse(false)
          val skipDependencyMappingValidation = resolvedConfiguration.skipDependencyMappingValidation.getOrElse(false)

          val scopePaths = ScopePaths(
            workspaceSrcRoot,
            scopeRoot,
            configFile,
            resolvedConfiguration.sourcesRoots.map(RelativePath(_)),
            resolvedConfiguration.resourcesRoots.map(RelativePath(_)),
            resolvedConfiguration.webSourcesRoots.map(RelativePath(_)),
            resolvedConfiguration.electronSourcesRoots.map(RelativePath(_)),
            resolvedConfiguration.pythonSourcesRoots.map(RelativePath(_)),
            resolvedConfiguration.archiveContentRoots.map(RelativePath(_)),
            resolvedConfiguration.docRoots.map(RelativePath(_))
          )

          val scopeFlags = ScopeFlags(
            open = config.booleanOrDefault("open", default = tpe == "main"),
            public = public,
            containsPlugin = containsPlugin,
            containsAgent = isAgent,
            definesMacros = definesMacros,
            containsMacros = containsMacros,
            jmh = jmh,
            javaOnly = !resolvedConfiguration.compile
              .externalDeps(useMavenLibs)
              .exists(dep => "scala-library" == dep.name || dep.isScalaSdk),
            usePipelining = resolvedConfiguration.usePipelining.getOrElse(true),
            empty = resolvedConfiguration.empty.getOrElse(false),
            installSources = resolvedConfiguration.installSources.getOrElse(false),
            installAppScripts = resolvedConfiguration.installAppScripts.getOrElse(false),
            installIvy = resolvedConfiguration.installIvy.getOrElse(false),
            pathingBundle = resolvedConfiguration.bundle.getOrElse(false),
            mavenOnly = mavenOnly,
            skipDependencyMappingValidation = skipDependencyMappingValidation,
            allowSetViolatingCustomScopes = resolvedConfiguration.allowSetViolatingCustomScopes.getOrElse(false)
          )

          val cfg = ScopeConfiguration(
            moduleSet = moduleSet,
            paths = scopePaths,
            flags = scopeFlags,
            generatorConfig = generatorConfiguration,
            resourceTokens = resolvedConfiguration.resourceTokens,
            runConfConfig = None,
            sourceExclusionsStr = config.seqOrEmpty("sourceExcludes"),
            dependencies = allDependencies(resolvedConfiguration, containsMacros),
            scalacConfig = resolvedConfiguration.scalac,
            javacConfig = javacConfig,
            cppConfigs = cppConfiguration,
            webConfig = webConfiguration,
            pythonConfig = pythonConfiguration,
            electronConfig = electronConfiguration,
            agentConfig = agentConf,
            processorConfig = processorConfiguration,
            interopConfig = interopConfiguration,
            useMavenLibs = useMavenLibs
          )
          ScopeDefinition(
            id = scopeId,
            module = module,
            configuration = cfg,
            parents = resolvedConfiguration.parents,
            copyFiles = resolvedConfiguration.copyFiles,
            extensions = resolvedConfiguration.extensions,
            archive = archiveConfiguration,
            line = line,
            jar = jar,
            includeInClassBundle = resolvedConfiguration.includeInClassBundle.getOrElse(true),
            postInstallApps = resolvedConfiguration.postInstallApps,
            relationships = resolvedConfiguration.relationships,
            customScopesDefinitions = customScopesDefinitions
          )
        }
      }
      .withProblems { sd =>
        if (sd.configuration.flags.installIvy) {
          val libs = sd.configuration.dependencies.allExternal.externalDeps()
          libs.flatMap { dep =>
            if (dep.isMaven) {
              val externalDeps = centralDependencies.externalDependencies(sd.configuration.moduleSet)
              externalDeps.mavenToAfsMap.get(dep.key) match {
                case None =>
                  Some(Error(
                    s"Maven dependency ${dep.key} does not have an AFS equivalent for use by ivy-enabled scope ${sd.id}",
                    JvmDependenciesConfig,
                    dep.line))
                case Some(afsDep) if afsDep.noVersion =>
                  Some(
                    Error(
                      s"AFS equivalent ${afsDep.key} for maven dependency ${dep.key} does not have a version for use by ivy-enabled scope ${sd.id}",
                      JvmDependenciesConfig,
                      dep.line
                    ))
                case _ => None
              }
            } else if (dep.noVersion) {
              Some(
                Error(
                  s"AFS dependency ${dep.key} does not have a version for use by ivy-enabled scope ${sd.id}",
                  JvmDependenciesConfig,
                  dep.line))
            } else None
          }
        } else Nil
      }
  }

  // prevent multiple instances of the same module set being created
  private val moduleSetCache = new ConcurrentHashMap[ModuleSetId, Result[ModuleSet]]()

  private def moduleSet(id: ScopeId, config: Config, file: ObtFile): Result[(ModuleSet, Boolean)] = {
    moduleSetsByModule.get(id.fullModule) match {
      case Some((msd, public)) => moduleSet(msd).map(_ -> public)
      case None                => Failure(Seq(file.errorAt(config.root, s"Module set not found for scope $id")))
    }
  }

  private def moduleSet(msd: ModuleSetDefinition): Result[ModuleSet] = {
    // Can't just use `computeIfAbsent` here, since CHM doesn't support updating recursively
    val cached = Option(moduleSetCache.get(msd.id))
    cached.getOrElse {
      val ms = for {
        canDependOnDefns <- loadSets(msd.file, msd.canDependOn, structure.moduleSets)
        canDependOn <- Result.sequence[ModuleSet](canDependOnDefns.map(moduleSet).to(Seq))
      } yield ModuleSet(msd.id, canDependOn.toSet, msd.dependencySets.map(_._1), msd.variantSets.map(_._1))
      Option(moduleSetCache.putIfAbsent(msd.id, ms)).getOrElse(ms)
    }
  }

  private def loadSets[A, B](
      file: ObtFile,
      ids: Set[(A, Int)],
      allSets: Map[A, B]
  ): Result[Set[B]] =
    Result
      .traverse(ids.to(Seq)) { case (id, line) =>
        allSets
          .get(id)
          .map(s => Success(s))
          .getOrElse(Failure(Seq(Error(s"Unknown module set '$id'", file, line))))
      }
      .map(_.toSet)

  // TODO (OPTIMUS-32045): Stop printing to stderr when strato info is practical.
  private def loadJavac(config: Config, origin: ObtFile): Result[InheritableJavacConfiguration] = {
    val warnings = loadWarnings("javac", config, origin)
    val release = if (config.hasPath("javac.release")) Some(config.getInt("javac.release")) else None
    val opts = config.seqOrEmpty("javac.options")
    warnings
      .map { ws => InheritableJavacConfiguration(opts, release, ws) }
      .withProblems(
        if (config.hasPath("javac")) config.getConfig("javac").checkExtraProperties(origin, Keys.javac)
        else Nil
      )
  }

  private def loadScalac(
      config: Config,
      origin: ObtFile
  ): Result[ScalacConfiguration] = {
    val macroLanguageFlag =
      if (config.booleanOrDefault("hasMacros", default = false)) List("-language:experimental.macros")
      else Nil
    val opts = config.seqOrEmpty("scalac.options") ++ macroLanguageFlag
    val target = if (config.hasPath("scalac.target")) Some(config.getString("scalac.target")) else None
    val warnings = loadWarnings("scalac", config, origin)
    warnings
      .map { ws =>
        ScalacConfiguration(
          opts,
          config.seqOrEmpty("scalac.ignoredPlugins").map(ScopeDefinition.loadScopeId),
          target,
          ws
        )
      }
      .withProblems(
        if (config.hasPath("scalac"))
          config.getConfig("scalac").checkExtraProperties(origin, Keys.scalac)
        else Nil
      )
  }

  private def loadInheritableCpps(
      config: Config,
      origin: ObtFile
  ): Result[Seq[InheritableCppConfiguration]] =
    Result.traverse(cppOsVersions)(osVersion => loadInheritableCpp(osVersion, config, origin))

  private def loadInheritableCpp(
      osVersion: String,
      config: Config,
      origin: ObtFile
  ): Result[InheritableCppConfiguration] = {
    val release = loadInheritableCppBuild(osVersion, config, origin, "release")
    val debug = loadInheritableCppBuild(osVersion, config, origin, "debug")
    for {
      r <- release
      d <- debug
    } yield InheritableCppConfiguration(osVersion, r, d)
  }

  private def loadInheritableCppBuild(
      osVersion: String,
      config: Config,
      origin: ObtFile,
      buildType: String
  ): Result[Option[InheritableCppBuildConfiguration]] = {
    if (config.hasPath("cpp")) {
      val cppCfg = config.getConfig("cpp")

      val os = OsUtils.osType(osVersion)
      val quotedOsVersion = ConfigUtil.quoteString(osVersion) // handle "." in path without treating it as a separator
      val osBuild = ConfigUtil.quoteString(s"$os.$buildType")
      val osVersionBuild = ConfigUtil.quoteString(s"$osVersion.$buildType")

      // Order here defines precedence from lowest to highest
      val paths = Seq(os, quotedOsVersion, osBuild, osVersionBuild)

      def resolve[A](f: Config => Seq[A]): Seq[A] = {
        f(cppCfg) ++ paths.flatMap { p =>
          if (cppCfg.hasPath(p)) f(cppCfg.getConfig(p))
          else Nil
        }
      }

      def resolveOpt[A](f: Config => Option[A]): Option[A] = {
        def inner(ps: Seq[String]): Option[A] = ps match {
          case h +: t if cppCfg.hasPath(h) => f(cppCfg.getConfig(h)) orElse inner(t)
          case _ +: t                      => inner(t)
          case Nil                         => f(cppCfg)
        }

        // reverse here to ensure we check highest precedence first
        inner(paths.reverse)
      }

      def resolveMap[A, B](f: Config => Result[Map[A, B]]): Result[Map[A, B]] =
        paths.foldLeft(f(cppCfg)) { (r, p) =>
          for {
            m1 <- r
            m2 <- if (cppCfg.hasPath(p)) f(cppCfg.getConfig(p)) else Success(Map.empty)
          } yield m1 ++ m2
        }

      val toolchainName = resolveOpt(_.optionalString("toolchain"))

      // 3 options here:
      // toolchainName == foo && "foo" toolchain exists
      // toolchainName == foo && "foo" toolchain doesn't exist
      // toolchainName not defined (accidentally omitted or defined in a parent)
      val toolchain = toolchainName.flatMap(toolchains.get)
      (toolchainName, toolchain) match {
        case (Some(n), None) =>
          Failure(s"Unrecognized toolchain: $n", origin)
        case _ =>
          // toolchainName may be empty here (if we're inheriting it from a parent definition)
          val outputType = cppCfg.optionalString("outputType").map {
            case "library"    => OutputType.Library
            case "executable" => OutputType.Executable
          }

          val preload = resolveOpt(_.optionalString("preload")).map(_.toBoolean)

          val compilerFlags =
            resolve(_.stringListOrEmpty("compilerFlags")).map(CppToolchainStructure.parseCompilerFlag).toSet
          val symbols = resolve(_.stringListOrEmpty("symbols"))
          val includes = resolve(_.stringListOrEmpty("includes").map(p => Directory(PathUtils.get(p))))
          val warningLevel = resolveOpt(_.optionalString("warningLevel")).map(_.toInt)
          val precompiledHeaderRes =
            resolveMap(_.stringMapOrEmpty("precompiledHeader", origin)).map(_.map { case (src, header) =>
              RelativePath(src) -> RelativePath(header)
            })
          val compilerArgs = resolve(_.stringListOrEmpty("compilerArgs"))

          val linkerFlags =
            resolve(_.stringListOrEmpty("linkerFlags")).map(CppToolchainStructure.parseLinkerFlag).toSet
          val isMavenOnly = resolveOpt(_.optionalBoolean(MavenOnlyKey)).contains(true)
          val dependenciesKey = if (isMavenOnly) MavenLibsKey else LibsKey
          val libs = resolve(_.stringListOrEmpty(dependenciesKey))
          val libPath = resolve(_.stringListOrEmpty("libPath").map(p => Directory(PathUtils.get(p))))
          val systemLibs = resolve(_.stringListOrEmpty("systemLibs").map(p => FileAsset(PathUtils.get(p))))
          val manifest = config.optionalString("manifest").map(p => FileAsset(PathUtils.get(p)))
          val linkerArgs = resolve(_.stringListOrEmpty("linkerArgs"))
          val fallbackPath = resolve(_.stringListOrEmpty("fallbackPath")).map(p => Directory(PathUtils.get(p)))

          precompiledHeaderRes.flatMap { pch =>
            val pchProblems = if (pch.size > 1) {
              resolveOpt(
                _.optionalValue("precompiledHeader")
              ) // in practice we know this will always exist if we've got to this point
                .map(v => origin.errorAt(v, "Multiple precompiled headers are not supported"))
            } else None

            def osFilter(key: String) = key.startsWith("linux") || key.startsWith("windows")
            val coreProblems = cppCfg.checkExtraProperties(origin, Keys.cpp, filter = !osFilter(_))
            val osProblems = cppCfg.keySet
              .filter(osFilter)
              .flatMap(k => cppCfg.getConfig(ConfigUtil.quoteString(k)).checkExtraProperties(origin, Keys.cpp))

            val problems = coreProblems ++ osProblems ++ pchProblems

            Success(
              Some(
                InheritableCppBuildConfiguration(
                  toolchain = toolchain,
                  outputType = outputType,
                  preload = preload,
                  compilerFlags = compilerFlags,
                  symbols = symbols,
                  includes = includes,
                  warningLevel = warningLevel,
                  precompiledHeader = pch,
                  compilerArgs = compilerArgs,
                  linkerFlags = linkerFlags,
                  libs = libs,
                  libPath = libPath,
                  systemLibs = systemLibs,
                  manifest = manifest,
                  linkerArgs = linkerArgs,
                  fallbackPath = fallbackPath
                )
              )
            )
              .withProblems(problems)
          }
      }
    } else Success(None) // !config.hasPath("cpp")
  }

  private def toCpp(
      module: Module,
      iccs: Seq[InheritableCppConfiguration]
  ): Result[Seq[CppConfiguration]] = Result.traverse(iccs) { icc =>
    for {
      r <- toCppBuild(module, icc.release)
      d <- toCppBuild(module, icc.debug)
    } yield CppConfiguration(osVersion = icc.osVersion, release = r, debug = d)
  }

  private def toCppBuild(
      module: Module,
      icc: Option[InheritableCppBuildConfiguration]
  ): Result[Option[CppBuildConfiguration]] = {
    icc match {
      case Some(cfg) =>
        cfg.toolchain
          .map { tc =>
            Success(
              Some(
                CppBuildConfiguration(
                  toolchain = tc,
                  outputType = cfg.outputType,
                  preload = cfg.preload.getOrElse(false),
                  compilerFlags = cfg.compilerFlags,
                  symbols = cfg.symbols,
                  includes = cfg.includes,
                  warningLevel = cfg.warningLevel,
                  precompiledHeader = cfg.precompiledHeader,
                  compilerArgs = cfg.compilerArgs,
                  linkerFlags = cfg.linkerFlags,
                  libs = cfg.libs,
                  libPath = cfg.libPath,
                  systemLibs = cfg.systemLibs,
                  manifest = cfg.manifest,
                  linkerArgs = cfg.linkerArgs,
                  fallbackPath = cfg.fallbackPath
                )
              )
            )
          }
          .getOrElse {
            Failure("No cpp toolchain specified", module)
          }
      case None => Success(None)
    }
  }

  private def loadForbiddenDependencies(
      config: Config,
      origin: ObtFile): Result[Seq[ForbiddenDependencyConfiguration]] =
    if (config.hasPath("forbiddenDependencies")) {
      ForbiddenDependencyConfigurationCompiler.loadForbiddenDependencies(config, origin)
    } else Success(Seq.empty)

  private def loadWarnings(compiler: String, config: Config, origin: ObtFile): Result[WarningsConfiguration] = {
    if (config.hasPath(compiler)) {
      WarningsConfiguration.load(config.getConfig(compiler), origin)
    } else Success(WarningsConfiguration.empty)
  }

  private def loadConfigOrEmpty(file: ObtFile) =
    loadConfig(file).map(conf => if (conf.isEmpty) ConfigFactory.empty() else conf)

  private val postInstallApps = "postInstallApps"
  private def loadPostInstallApps(config: Config, origin: ObtFile): Result[Seq[Set[PostInstallApp]]] = {
    def app(v: ConfigValue): Result[PostInstallApp] = v.valueType() match {
      case ConfigValueType.OBJECT =>
        val c = v.asInstanceOf[ConfigObject].toConfig
        Success(
          PostInstallApp(c.getString("name"), c.seqOrEmpty("args"), c.getBoolean("afterInstall"))
        ).withProblems(c.checkExtraProperties(origin, Keys.postInstallApp))
      case ConfigValueType.STRING =>
        Success(PostInstallApp(v.unwrapped().asInstanceOf[String], Nil, afterInstall = false))
      case t =>
        origin.failure(v, s"Expected object or string but got $t")

    }
    Result.tryWith(origin, config) {
      if (config.hasPath(postInstallApps)) {
        Result.sequence {
          config.values(postInstallApps).map {
            case l: ConfigList =>
              Result.sequence[PostInstallApp](l.asScala.map(v => app(v)).to(Seq)).map(_.toSet)
            case v: ConfigValue =>
              app(v).map(Set(_))
          }
        }
      } else Success(Nil)
    }
  }
}

object ScopeDefinitionCompiler {
  private val log: Logger = getLogger(this.getClass)

  private[buildtool] def invalidDependencyMsg(target: String, reason: String, source: String, explain: String = "") =
    s"Scope $target $reason, so it cannot be a dependency of $source $explain"

  private[buildtool] def mavenDepNotDefined(id: String): String =
    s"Maven dependency $id is not defined"

  private[buildtool] def depNotDefined(id: String): String = s"Dependency $id is not defined"

  def asJson(sd: ScopeDefinition, useMavenLibs: Boolean): JsObject = {
    val cfg = Map(
      "compile" -> asJson(sd.configuration.compileDependencies, useMavenLibs),
      "compileOnly" -> asJson(sd.configuration.compileOnlyDependencies, useMavenLibs),
      "runtime" -> asJson(sd.configuration.runtimeDependencies, useMavenLibs),
      "native" -> JsArray(sd.configuration.externalNativeDependencies.map(_.name).map(JsString.apply): _*),
      "sources" -> JsArray(sd.configuration.paths.sources.map(s => JsString(s.pathString)): _*),
      "sourceExcludes" -> JsArray(sd.configuration.sourceExclusions.map(r => JsString(r.pattern.pattern)): _*),
      "resources" -> JsArray(sd.configuration.paths.resources.map(r => JsString(r.pathString)): _*),
      "webSources" -> JsArray(sd.configuration.paths.webSources.map(r => JsString(r.pathString)): _*),
      "electronSources" -> JsArray(sd.configuration.paths.electronSources.map(r => JsString(r.pathString)): _*),
      "pythonSources" -> JsArray(sd.configuration.paths.pythonSources.map(r => JsString(r.pathString)): _*),
      "scalac" -> sd.configuration.scalacConfig.asJson,
      "javac" -> sd.configuration.javacConfig.asJson,
      "generators" -> GeneratorConfigurationCompiler.asJson(sd.configuration.generatorConfig),
      "open" -> JsBoolean(sd.configuration.open),
      "hasMacros" -> JsBoolean(sd.configuration.definesMacros),
      "implementsMacros" -> JsBoolean(sd.configuration.containsMacros),
      "isCompilerPlugin" -> JsBoolean(sd.configuration.containsPlugin),
      "jar" -> asJson(sd.jar),
      "extends" -> JsArray(sd.parents.map(JsString(_)): _*),
      "root" -> JsString(sd.configuration.paths.scopeRoot.pathString)
    ) ++
      sd.configuration.agentConfig.map("agent" -> AgentConfigurationCompiler.asJson(_))

    JsObject(sd.id.tpe -> JsObject(cfg))
  }

  private def asJson(deps: Dependencies, useMavenLibs: Boolean): JsObject = {
    def asJsonScope(list: Seq[ScopeId]) = JsArray(list.map(id => asJson(id)): _*)
    def asJsonDependency(list: Seq[DependencyId]) = JsArray(list.map(id => asJson(id)): _*)
    JsObject(
      "scopes" -> asJsonScope(deps.modules),
      LibsKey -> asJsonDependency(deps.externalDeps(useMavenLibs).map(_.id)))
  }

  private val jdWriter: JsonWriter[JarDefinition] = {
    import DefaultJsonProtocol._
    jsonFormat1(JarDefinition.apply)
  }
  private def asJson(jd: JarDefinition) = jdWriter.write(jd)

  def asJson(id: ScopeId): JsString =
    jsonKey(Seq(id.meta, id.bundle, id.module, id.tpe))

  def asJson(mb: MetaBundle): JsString =
    jsonKey(Seq(mb.meta, mb.bundle))

  def asJson(id: DependencyId): JsString =
    jsonKey(Seq(id.group, id.name) ++ id.variant ++ id.configuration ++ id.keySuffix)

  private def jsonKey(segments: Seq[String]) =
    JsString(segments.map(segment => if (segment.contains('.')) s""""$segment"""" else segment).mkString("."))

}
