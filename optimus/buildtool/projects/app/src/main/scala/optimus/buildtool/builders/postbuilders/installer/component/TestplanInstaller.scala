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

import java.nio.file.Files
import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.format.JsonSupport
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.Installer
import optimus.buildtool.builders.postbuilders.installer.component.testplans.Changes
import optimus.buildtool.builders.postbuilders.installer.component.testplans._
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ScopeId
import optimus.buildtool.config.VersionConfiguration
import optimus.buildtool.files.Directory.PathRegexFilter
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.WorkspaceStructure
import optimus.buildtool.runconf.ModuleScopedName
import optimus.buildtool.runconf.RunConf
import optimus.buildtool.runconf.TestRunConf
import optimus.buildtool.utils.AssetUtils
import optimus.scalacompat.collection._
import optimus.platform.util.Log
import optimus.platform._
import optimus.tools.testplan.model.TestplanField.TestCases

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.compat._

final case class TestData(testType: TestType, testplan: TestPlan, testModules: Seq[String])

final case class TestplanTemplate(header: Seq[String], values: Seq[String])

final class TestplanInstaller(
    installer: Installer,
    versionConfig: VersionConfiguration,
    stratoOverride: Option[String],
    workspaceStructure: WorkspaceStructure
) extends ComponentInstaller
    with ComponentBatchInstaller
    with Log
    with PythonTestplanInstaller {
  import installer._

  override val descriptor = "testplan files"

  // all path strings in one place
  object PathNames {
    val Testplans: RelativePath = RelativePath("testplans")
    val TestGroups: RelativePath = RelativePath("profiles/testGroups")
    val TestplanDir: RelativePath = RelativePath("testplans")
    def templateFile(os: String): RelativePath = RelativePath(s"testplan.$os.pre.template")
    def testplanFile(suffix: String = "merged"): String = s"downstream-$suffix.testplan"
  }

  @node private def testData(scopeIds: Set[ScopeId]): Seq[TestData] =
    prepareFor(readTestTypes(), changes, scopeIds)

  @node private def changes: Changes = {
    val git = if (testplanConfig.useDynamicTests) gitLog else None
    Changes(git, scopeConfigSource, testplanConfig.ignoredPaths)
  }

  // We need to install testplans at the end because we need the access to all the built modules for scoping.
  @async def install(installable: BatchInstallableArtifacts): Seq[FileAsset] = {
    val scopes = Artifact.scopeIds(installable.artifacts)
    val metaBundles = scopes.map(_.metaBundle).distinct
    metaBundles.apar.flatMap { metaBundle =>
      installTestplanFiles(metaBundle, installable.allScopes)
    } ++ writeChangesFiles(changes)
  }

  @async private def writeChangesFiles(changes: Changes): Seq[FileAsset] = {
    // This file is used to scope other test types on Jenkins, for example Quick Smokes
    if (changes.isDefined) {
      val directlyChangedScopes = changes.directlyChangedScopes
      val changedScopes = scopeConfigSource.compilationScopeIds.apar.filter(changes.scopeDependenciesChanged(_))
      val changedModules: Set[String] = changedScopes.map(_.fullModule.toString)
      val changedModulesFile = installDir.resolveFile("changed-modules.txt")
      AssetUtils.atomicallyWrite(changedModulesFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, changedModules.mkString(",").getBytes)
      }
      val changedScopesFile = installDir.resolveFile("changed-scopes.txt")
      AssetUtils.atomicallyWrite(changedScopesFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, changedScopes.map(_.properPath).toSeq.sorted.mkString(",").getBytes)
      }
      val directlyChangedScopesFile = installDir.resolveFile("directly-changed-scopes.txt")

      AssetUtils.atomicallyWrite(directlyChangedScopesFile, replaceIfExists = true, localTemp = true) { tempPath =>
        Files.write(tempPath, directlyChangedScopes.map(_.properPath).toSeq.sorted.mkString(",").getBytes)
      }
      Seq(changedModulesFile, changedScopesFile, directlyChangedScopesFile)
    } else Nil
  }

  @async def install(installable: InstallableArtifacts): Seq[FileAsset] = Seq.empty

  @node private def readTestTypes(): Seq[TestType] = {
    val groupsDir = directoryFactory.reactive(sourceDir.resolveDir(PathNames.TestGroups))
    val groupFiles = groupsDir.findFiles(PathRegexFilter(""".*\.json"""))
    val testplanFiles = sourceDir.testplanFiles
    (groupFiles ++ testplanFiles)
      .map(JsonSupport.readValue[TestType])
      .groupBy(t => t.testGroupsOpts.os -> t.testGroupsOpts.testGroupsName)
      .map { case (_, toMerge) =>
        toMerge.reduce(_ mergeWith _)
      }
      .to(Seq)
  }

  @node protected def loadTestplanTemplate(os: String, templatePath: RelativePath): TestplanTemplate = {
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

  private def getData(testType: TestType): Seq[TestplanEntry] = {
    val estimates = testType.estimates

    testType.testGroups.flatMap { testGroup =>
      val testTasks = testGroup.entries.map { e =>
        TestplanTask(e.moduleId, e.testName.getOrElse(testType.testGroupsOpts.testGroupsName))
      }
      val additionalBindings =
        testType.bindingsFor(testGroup, versionConfig, testplanConfig, workspaceStructure)
      val testplanEntries = estimates.get(testGroup.name) match {
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
                testTasks = subTestTasks
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
              testTasks = testTasks
            ))
      }
      if (testGroup.programmingLanguage == ProgrammingLanguage.Python)
        testplanEntries.toSeq :+ createPythonTestplanEntry(
          testType,
          testGroup,
          testTasks,
          additionalBindings,
          testplanConfig)
      else
        testplanEntries
    }
  }

  @node private def prepareFor(
      testTypes: Seq[TestType],
      changes: Changes,
      includedScopes: Set[ScopeId]): Seq[TestData] = {
    val templatesPerOS: Map[String, TestplanTemplate] = {
      val operatingSystems = testTypes.map(tt => tt.testGroupsOpts.os).toSet
      operatingSystems.apar.map(os => os -> loadTestplanTemplate(os, PathNames.templateFile(os))).toMap
    }

    val allTestData = testTypes.apar.map(testType => testType -> getData(testType))
    // we cannot validate the entries until we have all of them
    validateTestplanEntries(includedScopes, allTestData.apar.flatMap(_._2))

    val includedRunconfs = testRunConfs(includedScopes)

    allTestData.apar.flatMap { case (testType, fullTestData) =>
      val filteredTestData: Seq[TestplanEntry] =
        fullTestData.apar.flatMap(changes.onlyChanged(_, includedRunconfs))

      if (filteredTestData.nonEmpty) {
        val os = testType.testGroupsOpts.os

        val (pythonTestData, unitTestData) = filteredTestData.partition(_.isInstanceOf[PythonTestplanEntry])
        val pythonQualityTestplans =
          if (pythonTestData.nonEmpty) generatePythonQualityTestplan(os, pythonTestData) else Seq.empty
        val unitTestplan = generateTestplan(unitTestData, templatesPerOS(os))
        val testplan = TestPlan.merge(pythonQualityTestplans ++ Seq(unitTestplan))

        val testModules = unitTestData.map(_.moduleName).distinct.sorted
        Some(TestData(testType, testplan, testModules))
      } else None
    }
  }

  @node private def validateTestplanEntries(scopeIds: Set[ScopeId], testData: Seq[TestplanEntry]): Unit = {
    val runConfNames = testRunConfNames(scopeIds)

    val allTestTasks = testData.flatMap(_.testTasks).toSet

    // 1. Check for test runconfs for the scopes we've build that are defined in .runconf files but not mentioned
    //    anywhere in testplans
    val allTestplanTestNames = allTestTasks.map(t => t.moduleScoped)
    val missingTestNames = runConfNames -- allTestplanTestNames
    if (missingTestNames.nonEmpty) {
      val missing = missingTestNames.map(_.properPath).to(Seq).sorted.mkString(", ")
      val msg =
        s"""Test(s) '$missing' are not included in any of the downstream test groups!
           |
           |To find how to add test to downstream test groups please check /profiles/testGroups/README.MD on local machine or on stash.""".stripMargin
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

    val superfluousTestNames = testplanTestNamesForModules -- testRunConfNamesForModules -- fakeRunconfNames
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
      .collect { case trc: TestRunConf => trc }
  }

  @async private def installTestplanFiles(metaBundle: MetaBundle, allScopeIds: Set[ScopeId]): Seq[FileAsset] = {
    val testplansDir = pathBuilder.etcDir(metaBundle).resolveDir(PathNames.Testplans)

    val allTestData = testData(allScopeIds)

    val testDataPerGroup: Map[String, Seq[TestData]] =
      allTestData.groupBy(_.testType.testGroupsOpts.testGroupsName)
    val testplans = testDataPerGroup
      .to(Seq)
      .map { case (_, elements) => TestPlan.merge(elements.map(_.testplan)) }

    val testDataPerModule: Map[String, Seq[TestData]] =
      allTestData.groupBy(_.testType.testGroupsOpts.testModulesFileName)
    val testModulesFiles = testDataPerModule
      .map { case (testModuleFileName, elements) =>
        val testModules = elements.flatMap(_.testModules).sorted.mkString(",")
        val testModulesFile = testplansDir.resolveFile(testModuleFileName)
        Files.createDirectories(testModulesFile.parent.path)
        AssetUtils.atomicallyWrite(testModulesFile, replaceIfExists = true, localTemp = true) { tempPath =>
          Files.write(tempPath, s"TEST_MODULE=$testModules".getBytes)
        }
        testModulesFile
      }
      .to(Seq)

    val testplanFile: Option[FileAsset] =
      if (testplans.nonEmpty) {
        val file = testplansDir.resolveFile(PathNames.testplanFile())
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
