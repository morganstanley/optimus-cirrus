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
package optimus.buildtool.builders.postbuilders.installer.component

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.builders.postbuilders.installer.component.fingerprintdiffing.FingerprintDiffChanges
import optimus.buildtool.builders.postbuilders.installer.component.testplans.GitChanges
import optimus.buildtool.builders.postbuilders.installer.component.testplans._
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.StaticConfig
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.Directory.PathRegexFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.JsonSupport
import optimus.buildtool.format.WorkspaceStructure
import optimus.buildtool.runconf.ModuleScopedName
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.runconf.TestRunConf
import optimus.buildtool.utils.AssetUtils
import optimus.buildtool.utils.GitLog
import optimus.platform._
import optimus.platform.util.Log
import optimus.scalacompat.collection._
import optimus.tools.testplan.model.TestplanField.TestCases

import java.nio.file.Files
import java.nio.file.Path
import scala.collection.compat._
import scala.jdk.CollectionConverters._

final case class TestData(testType: TestType, testplan: TestPlan, testModules: Seq[String])

final case class TestplanTemplate(header: Seq[String], values: Seq[String])

final class TestplanInstaller(
    installer: Installer,
    versionConfig: VersionConfiguration,
    stratoOverride: Option[String],
    workspaceStructure: WorkspaceStructure
) extends ComponentBatchInstaller
    with Log
    with CustomPassfailTestplanInstaller
    with PyTestplanTestplanInstaller
    with PactContractTestplanInstaller
    with PythonTestplanInstaller {

  import installer._

  override val descriptor = "testplan files"

  // all path strings in one place
  object PathNames {
    val TestGroups: RelativePath = RelativePath("profiles/testGroups")
    val TestplanDir: RelativePath = RelativePath("config/testplans")
    val InModuleTestplanDir: RelativePath = RelativePath("testplans")
    def templateFile(os: String): RelativePath = RelativePath(s"testplan.$os.pre.template")
    def testplanFile(suffix: String = "merged", prefix: String = ""): String = s"${prefix}downstream-$suffix.testplan"
  }

  private def gitLogOpt: Option[GitLog] = if (testplanConfig.useDynamicTests) gitLog else None

  private val fingerprintPrefix = "fingerprint-"

  // We need to install testplans at the end because we need the access to all the built modules for scoping.
  @async def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    val scopes = Artifact.scopeIds(installable.artifacts)
    val metaBundles = scopes.map(_.metaBundle).distinct

    val gitChanges: GitChanges =
      GitChanges(gitLogOpt, scopeConfigSource, testplanConfig.ignoredPaths)
    val fingerprintChanges: Changes =
      FingerprintDiffChanges.create(
        scopeConfigSource,
        installer.fingerprintsConfiguration,
        installer.buildDir.path,
        versionConfig.installVersion,
        gitChanges)

    val testTypes = readTestTypes()
    val allTestData: Seq[TestData] = prepareFor(testTypes, gitChanges, installable.allScopes)
    val allFingerprintTestData: Seq[TestData] = prepareFor(testTypes, fingerprintChanges, installable.allScopes)

    metaBundles.apar.flatMap { metaBundle =>
      installTestplanFiles(metaBundle, allTestData)
      installTestplanFiles(metaBundle, allFingerprintTestData, fingerprintPrefix)
    } ++ writeChangesFiles(gitChanges) ++
      writeGitChangedFiles(gitChanges) ++
      writeChangesFiles(fingerprintChanges, fingerprintPrefix)
  }

  @async private def writeGitChangedFiles(gitChanges: Changes): Seq[FileAsset] = {
    if (gitChanges.isDefined) {
      val changedFilesFile = installDir.resolveFile("changed-files.txt")
      val changedPaths: Set[Path] =
        gitLogOpt.map(gitLog => GitChanges.changedPaths(gitLog, testplanConfig.ignoredPaths)).getOrElse(Set.empty)
      AssetUtils.atomicallyWrite(changedFilesFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, changedPaths.toSeq.sorted.mkString("\n").getBytes)
      }

      Seq(changedFilesFile)
    } else Nil
  }

  @async private def writeChangesFiles(changes: Changes, prefix: String = ""): Seq[FileAsset] = {
    // This file is used to scope other test types on Jenkins, for example Quick Smokes
    if (changes.isDefined) {
      val directlyChangedScopes = changes.directlyChangedScopes
      val changedScopes = scopeConfigSource.compilationScopeIds.apar.filter(changes.scopeDependenciesChanged)
      val changedModules: Set[String] = changedScopes.map(_.fullModule.toString)
      val changedModulesFile = installDir.resolveFile(s"${prefix}changed-modules.txt")
      AssetUtils.atomicallyWrite(changedModulesFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, changedModules.mkString(",").getBytes)
      }
      val changedScopesFile = installDir.resolveFile(s"${prefix}changed-scopes.txt")
      AssetUtils.atomicallyWrite(changedScopesFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, changedScopes.map(_.properPath).toSeq.sorted.mkString(",").getBytes)
      }

      val directlyChangedScopesFile = installDir.resolveFile(s"${prefix}directly-changed-scopes.txt")

      AssetUtils.atomicallyWrite(directlyChangedScopesFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, directlyChangedScopes.map(_.properPath).toSeq.sorted.mkString(",").getBytes)
      }

      Seq(changedModulesFile, changedScopesFile, directlyChangedScopesFile)
    } else Nil
  }

  @node private def readTestTypes(): Seq[TestType] = {
    val groupsDir = directoryFactory.reactive(sourceDir.resolveDir(PathNames.TestGroups))
    val groupFiles = groupsDir.findFiles(PathRegexFilter(""".*\.json"""))
    val testplanFiles = sourceDir.testplanFiles
    val testTypes = (groupFiles ++ testplanFiles)
      .map(JsonSupport.readValue[TestType])
      .groupBy(t => t.testGroupsOpts.os -> t.testGroupsOpts.testGroupsName)
      .map { case (_, toMerge) =>
        toMerge.reduce(_ mergeWith _)
      }
      .to(Seq)
    testTypes
  }

  @node protected def loadTestplanTemplate(templatePath: RelativePath): TestplanTemplate = {
    val testplanDir = directoryFactory.reactive(sourceDir.resolveDir(PathNames.TestplanDir))
    val templateFile = testplanDir.resolveFile(templatePath)

    require(
      directoryFactory.fileExists(templateFile),
      s"Couldn't find a testplan template for platform 'platform': ${templateFile.pathString} does not exist!")

    testplanDir.declareVersionDependence()
    Files.readAllLines(templateFile.path).asScala.foldLeft(TestplanTemplate(Seq.empty[String], Seq.empty[String])) {
      case (TestplanTemplate(headers, values), line) =>
        val Array(header, value) = line.split(TestPlan.sep, 2)
        TestplanTemplate(headers :+ header.trim(), values :+ value.trim())
    }
  }

  @node protected def generateTestplan(data: Seq[TestplanEntry], template: TestplanTemplate): TestPlan =
    TestPlan.merge(
      data.apar.map(datum => TestPlan(template.header, Seq(template.values.map(datum.bind))))
    )

  private def getData(testType: TestType, includedScopes: Set[ScopeId]): Seq[TestplanEntry] = {
    val estimates = testType.estimates

    val allRows = testType.testGroups.flatMap { testGroup =>
      val testTasks = testGroup.entries.map { e =>
        TestplanTask(e.moduleId, e.testName.getOrElse(testType.testGroupsOpts.testGroupsName))
      }
      val additionalBindings =
        testType.bindingsFor(testGroup, versionConfig, testplanConfig, workspaceStructure)

      val allRowsForGroup = testGroup.programmingLanguage match {
        case ProgrammingLanguage.CustomPassfail =>
          val validBindings =
            CustomPassfailTestplanTemplate.requiredAdditionalBindings.forall(additionalBindings.contains)
          if (!validBindings)
            throw new IllegalArgumentException(
              s"Not all valid bindings provided for ${testGroup.name}: ${CustomPassfailTestplanTemplate.requiredAdditionalBindings
                  .mkString(", ")}")
          val entry =
            createCustomPassfailTestplanEntry(
              testType.displayName,
              testGroup.name,
              testGroup.metaBundle,
              testType.treadmillOptsFor(testGroup, testplanConfig),
              additionalBindings,
              testTasks,
              Map(),
              testType.testGroupsOpts.testModulesFileName
            )
          Seq(entry)
        case ProgrammingLanguage.PyTestplan =>
          val validBindings =
            PyTestplanTestplanTemplate.requiredAdditionalBindings.forall(additionalBindings.contains)
          if (!validBindings)
            throw new IllegalArgumentException(
              s"Not all valid bindings provided for ${testGroup.name}: ${PyTestplanTestplanTemplate.requiredAdditionalBindings
                  .mkString(", ")}")
          val entry =
            createPyTestplanTestplanEntry(
              testType.displayName,
              testGroup.name,
              testGroup.metaBundle,
              testType.treadmillOptsFor(testGroup, testplanConfig),
              additionalBindings,
              testTasks,
              Map(),
              testType.testGroupsOpts.testModulesFileName
            )
          Seq(entry)
        case ProgrammingLanguage.PactContract =>
          // Special handling for pact contract testing - we need to work out if we are generating a test entry for
          // both consumer and provider, or just consumer, or just provider.  We do this by scanning the rest of the
          // scopes to see if there is a counterpart
          val contractTestTypes = Set("consumerContractTest", "providerContractTest")
          val testTaskOverrides = testTasks.map { task =>
            val taskOverrides = includedScopes
              .filter(s => s.fullModule == task.module && contractTestTypes.contains(s.tpe))
              .map(_.tpe)
            task -> taskOverrides
          } toMap

          if (testTaskOverrides.values.head.isEmpty) {
            // No pact contract testing in the defined scopes so skip this
            Seq()
          } else {
            val entry = createPactContractTestplanEntry(
              displayTestName = testType.displayName,
              groupName = testGroup.name,
              metaBundle = testGroup.metaBundle,
              treadmillOpts = testType.treadmillOptsFor(testGroup, testplanConfig),
              additionalBindings = additionalBindings,
              stratoOverride = stratoOverride,
              testModulesFileName = testType.testGroupsOpts.testModulesFileName,
              testTasks = testTasks,
              testTaskOverrides = testTaskOverrides
            )
            Seq(entry)
          }
        case ProgrammingLanguage.Python =>
          val entry = createPythonTestplanEntry(testType, testGroup, testTasks, additionalBindings, testplanConfig)
          Seq(entry)
        case _ =>
          val entries = estimates.get(testGroup.name) match {
            case Some(estimatedNoContainers) if estimatedNoContainers > 1 =>
              val projectsPerContainer = testTasks.size / estimatedNoContainers
              testTasks.sliding(projectsPerContainer, projectsPerContainer).zipWithIndex.map {
                case (subTestTasks, groupNo) =>
                  ScalaTestplanEntry(
                    displayTestName = testType.displayName,
                    groupName = s"${testGroup.name}-${groupNo + 1}",
                    metaBundle = testGroup.metaBundle,
                    treadmillOpts = testType.treadmillOptsFor(testGroup, testplanConfig),
                    additionalBindings = additionalBindings,
                    stratoOverride = stratoOverride,
                    testModulesFileName = testType.testGroupsOpts.testModulesFileName,
                    testTasks = subTestTasks,
                    testTaskOverrides = Map()
                  )
              }
            case _ =>
              Seq(
                ScalaTestplanEntry(
                  displayTestName = testType.displayName,
                  groupName = testGroup.name,
                  metaBundle = testGroup.metaBundle,
                  treadmillOpts = testType.treadmillOptsFor(testGroup, testplanConfig),
                  additionalBindings = additionalBindings,
                  stratoOverride = stratoOverride,
                  testModulesFileName = testType.testGroupsOpts.testModulesFileName,
                  testTasks = testTasks,
                  testTaskOverrides = Map()
                ))
          }
          entries.toSeq
      }
      allRowsForGroup
    }

    allRows
  }

  @node private def prepareFor(
      testTypes: Seq[TestType],
      changes: Changes,
      includedScopes: Set[ScopeId]): Seq[TestData] = {
    val templatesPerOS: Map[String, TestplanTemplate] = {
      val operatingSystems = testTypes.map(tt => tt.testGroupsOpts.os).toSet
      operatingSystems.apar.map(os => os -> loadTestplanTemplate(PathNames.templateFile(os))).toMap
    }

    val allTestData = testTypes.apar.map(testType => testType -> getData(testType, includedScopes))

    // we cannot validate the entries until we have all of them
    validateTestplanEntries(includedScopes, allTestData.apar.flatMap(_._2))

    val includedRunconfs = testRunConfs(includedScopes)

    val testDataRows = allTestData.apar.flatMap { case (testType, fullTestData) =>
      val filteredTestData: Seq[TestplanEntry] =
        fullTestData.apar.flatMap(changes.onlyChanged(_, includedRunconfs))

      if (filteredTestData.nonEmpty) {
        val os = testType.testGroupsOpts.os

        val (pythonTestData, unitTestData, pactContractTestData, customPassfailData, pyTestplanData) =
          filteredTestData.foldLeft(
            (
              Seq.empty[PythonTestplanEntry],
              Seq.empty[ScalaTestplanEntry],
              Seq.empty[PactContractTestplanEntry],
              Seq.empty[CustomPassfailTestplanEntry],
              Seq.empty[PyTestplanTestplanEntry])) {
            case ((pythons, units, pacts, customPassfails, pyTestplans), python: PythonTestplanEntry) =>
              (pythons :+ python, units, pacts, customPassfails, pyTestplans)
            case ((pythons, units, pacts, customPassfails, pyTestplans), unit: ScalaTestplanEntry) =>
              (pythons, units :+ unit, pacts, customPassfails, pyTestplans)
            case ((pythons, units, pacts, customPassfails, pyTestplans), pact: PactContractTestplanEntry) =>
              (pythons, units, pacts :+ pact, customPassfails, pyTestplans)
            case ((pythons, units, pacts, customPassfails, pyTestplans), customPassFail: CustomPassfailTestplanEntry) =>
              (pythons, units, pacts, customPassfails :+ customPassFail, pyTestplans)
            case ((pythons, units, pacts, customPassfails, pyTestplans), pyTestplan: PyTestplanTestplanEntry) =>
              (pythons, units, pacts, customPassfails, pyTestplans :+ pyTestplan)
          }

        val customPassfailTestplans =
          if (customPassfailData.nonEmpty) generateCustomPassfailTestplan(customPassfailData) else Seq.empty

        val pyTestplanTestplans =
          if (pyTestplanData.nonEmpty) generatePyTestplanTestplan(pyTestplanData) else Seq.empty

        val pythonQualityTestplans =
          if (pythonTestData.nonEmpty) generatePythonQualityTestplan(os, pythonTestData) else Seq.empty

        val pactContractTestplans =
          if (pactContractTestData.nonEmpty) generatePactContractTestplan(os, pactContractTestData) else Seq.empty

        val unitTestplans =
          if (unitTestData.nonEmpty) Seq(generateTestplan(unitTestData, templatesPerOS(os))) else Seq.empty

        val testplan =
          TestPlan.merge(
            customPassfailTestplans ++ pyTestplanTestplans ++ pactContractTestplans ++ pythonQualityTestplans ++ unitTestplans)

        val testModules =
          unitTestData.map(_.moduleName).distinct.sorted ++ pactContractTestData
            .map(_.moduleName)
            .distinct
            .sorted ++ pyTestplanData.map(_.moduleName).distinct.sorted

        Some(TestData(testType, testplan, testModules))
      } else None
    }

    testDataRows
  }

  @node private def validateTestplanEntries(scopeIds: Set[ScopeId], testData: Seq[TestplanEntry]): Unit = {
    val testScopeIds = scopeIds.filter(_.isTest)
    val runConfNames = testRunConfNames(scopeIds)

    val testRunConfs = testScopeIds.apar.map(id => id -> testRunConfNames(Set(id)))
    val missingRunConfs = testRunConfs.filter { case (_, confs) => confs.isEmpty }
    if (missingRunConfs.nonEmpty) {
      val scopeIdsText = missingRunConfs.map { case (name, _) => name.properPath }.toSeq.sorted.mkString(",")
      val helpUrl =
        s"${StaticConfig.string("codetreeDocsUrl")}/optimus/docs/Stratosphere/StratosphereFaq#how-do-i-add-a-new-test-group"
      val msg =
        s"""Test scope(s) '$scopeIdsText' are missing an associated runconf!
           |
           |To find how to configure tests correctly, please check: $helpUrl""".stripMargin
      // TODO (OPTIMUS-79609): make fatal
      log.error(msg)
    }

    val allTestTasks = testData.flatMap(_.testTasks).toSet
    val allTestTaskOverrides = testData
      .flatMap(_.testTaskOverrides)
      .flatMap(t => {
        val task = t._1
        val overrides = t._2
        overrides.map { o => task.forTestName(o).moduleScoped }
      })
      .toSet

    // 1. Check for test runconfs for the scopes we've build that are defined in .runconf files but not mentioned
    //    anywhere in testplans
    val allTestplanTestNames = allTestTasks.map(t => t.moduleScoped)
    val missingTestNames = runConfNames -- allTestplanTestNames -- allTestTaskOverrides
    if (missingTestNames.nonEmpty) {
      val missing = missingTestNames.map(_.properPath).to(Seq).sorted.mkString(", ")
      val helpUrl = s"${StaticConfig.string("codetreeDocsUrl")}/profiles/testGroups/README"
      val msg =
        s"""Test(s) '$missing' are not included in any of the downstream test groups!
           |
           |To find how to add test to downstream test groups please check: $helpUrl""".stripMargin
      throw new IllegalStateException(msg)
    }

    // 2. Check for testplan entries for modules that we've built (either completely or partially) that don't
    //    exist in runconfs
    //    Note: Testplans are only scoped at the module level, so we need to ensure we collect all potential scopes
    //    for the modules we've built
    val moduleIds = scopeIds.map(_.fullModule)
    val testplanTestNamesForModules = allTestTasks.filter(t => moduleIds.contains(t.module)).map(t => t.moduleScoped)

    val allScopesForModules = scopeConfigSource.compilationScopeIds.filter(s => moduleIds.contains(s.fullModule))
    val testRunConfNamesForModules = testRunConfNames(allScopesForModules)

    // Note: These aren't real runconf names, but we ignore them for now to maintain backward compatibility
    val fakeRunconfNames = allScopesForModules.map(s => ModuleScopedName(s.fullModule, s.tpe))

    // Special test types that are treated different to the rest
    val specialTestNames = Set("pactContractTest")
    val superfluousTestNames =
      testplanTestNamesForModules -- testRunConfNamesForModules -- fakeRunconfNames -- testplanTestNamesForModules
        .filter(t => specialTestNames.contains(t.name))

    if (superfluousTestNames.nonEmpty) {
      val superfluous = superfluousTestNames.map(_.properPath).to(Seq).sorted.mkString(", ")
      val msg =
        s"""Test(s) '$superfluous' do not exist in the workspace.
           |
           |Please make sure the configuration in the .testplan.json file matches the runconf names.""".stripMargin
      throw new IllegalStateException(msg)
    }

    // 3. Check for modules named in testplan entries that don't exist at all
    val allModuleIds = scopeConfigSource.compilationScopeIds.map(_.fullModule)
    val testplansByModuleId = allTestTasks.groupBy(_.module)

    val superfluousModuleTestNames = testplansByModuleId.filterKeysNow(k => !allModuleIds.contains(k))
    if (superfluousModuleTestNames.nonEmpty) {
      val superfluousTests = for {
        (_, tests) <- superfluousModuleTestNames
        t <- tests
      } yield t
      val superfluous = superfluousTests.map(_.moduleScoped.properPath).to(Seq).sorted.mkString(", ")
      val msg =
        s"""Test module(s) for '$superfluous' do not exist in the workspace.
           |
           |Please make sure the configuration in the .testplan.json file matches the module names.""".stripMargin
      throw new IllegalStateException(msg)
    }
  }

  @node private def testRunConfNames(scopeIds: Set[ScopeId]): Set[ModuleScopedName] =
    testRunConfs(scopeIds).map(_.runConfId.moduleScoped)

  @node private def testRunConfs(scopeIds: Set[ScopeId]): Set[RunConf] = {
    // Note the `filter` here - ScopedCompilation.runConfigurations returns all runconfs for the module,
    // not just for the given scopeId.
    scopeIds.apar
      .flatMap(s => factory.scope(s).runConfigurations.filter(_.id == s))
      .collect { case trc: TestRunConf if !trc.isLocal => trc }
  }

  @async private def installTestplanFiles(
      metaBundle: MetaBundle,
      allTestData: Seq[TestData],
      testplanFilePrefix: String = ""): Seq[FileAsset] = {
    val testplansDir = pathBuilder.etcDir(metaBundle).resolveDir(PathNames.InModuleTestplanDir)

    val testDataPerGroup: Map[String, Seq[TestData]] =
      allTestData.groupBy(_.testType.testGroupsOpts.testGroupsName)
    val testplans = testDataPerGroup
      .to(Seq)
      .map { case (_, elements) => TestPlan.merge(elements.map(_.testplan)) }

    val testDataPerModule: Map[String, Seq[TestData]] =
      allTestData.groupBy(_.testType.testGroupsOpts.testModulesFileName)
    val testModulesFiles = testDataPerModule
      .map { case (testModuleFileName, elements) =>
        val finalTestModuleFileName = s"$testplanFilePrefix$testModuleFileName"
        val testModules = elements.flatMap(_.testModules).sorted.mkString(",")
        val testModulesFile = testplansDir.resolveFile(finalTestModuleFileName)
        Files.createDirectories(testModulesFile.parent.path)
        AssetUtils.atomicallyWrite(testModulesFile, replaceIfExists = true, localTemp = true) { tempPath =>
          Files.write(tempPath, s"TEST_MODULE=$testModules".getBytes)
        }
        testModulesFile
      }
      .to(Seq)

    val testplanFile: Option[FileAsset] =
      if (testplans.nonEmpty) {
        val file = testplansDir.resolveFile(PathNames.testplanFile(prefix = testplanFilePrefix))
        val mergedTestplan = TestPlan.merge(testplans)

        validateUniqueTestCaseNames(mergedTestplan)

        val testplanContent = mergedTestplan.text
        Files.createDirectories(file.parent.path)
        AssetUtils.atomicallyWrite(file, replaceIfExists = true, localTemp = true) { tempPath =>
          Files.write(tempPath, testplanContent.getBytes)
        }
        Some(file)
      } else None

    testModulesFiles ++ testplanFile
  }

  private def validateUniqueTestCaseNames(testplan: TestPlan): Unit = {
    val testCaseColumn = testplan.headers.indexOf(TestCases.toString)
    val duplicateTestNames = testplan.values
      .groupBy(t => t(testCaseColumn))
      .collect { case (name, rows) if rows.size > 1 => name }
      .toSeq
      .sorted

    if (duplicateTestNames.nonEmpty) {
      val msg =
        s"""Found duplicate test names in merged testplan: ${duplicateTestNames.mkString(",")}
           |
           |Please make sure the test group names in the .testplan.json file are not the same.""".stripMargin
      throw new IllegalStateException(msg)
    }
  }

}
