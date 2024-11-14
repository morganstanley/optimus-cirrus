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
package optimus.buildtool.builders.postbuilders.installer.component.testplans

import optimus.buildtool.config.MetaBundle
import optimus.buildtool.config.ModuleId
import optimus.buildtool.runconf.ModuleScopedName
import org.apache.commons.text.StringSubstitutor

import scala.collection.immutable.Seq

final case class TestplanTask(module: ModuleId, testName: String) {
  def moduleScoped: ModuleScopedName = ModuleScopedName(module, testName)

  def forTestName(otherTestName: String) = TestplanTask(module, otherTestName)
}

/**
 * Data used to generate testplan entry, i.e. each row that represents a single regression.
 */
sealed trait TestplanEntry {
  val displayTestName: String
  val groupName: String
  val metaBundle: MetaBundle
  val treadmillOpts: Map[String, String]
  val additionalBindings: Map[String, String]
  val testTasks: Seq[TestplanTask]
  val testTaskOverrides: Map[TestplanTask, Set[String]]
  val testModulesFileName: String

  def bind(value: String): String
  def copyEntry(testTasks: Seq[TestplanTask] = this.testTasks, groupName: String = this.groupName): TestplanEntry

  val moduleName: String = additionalBindings.getOrElse("MODULE_NAME", groupName)
  val metaBundleName = s"${metaBundle.meta}-${metaBundle.bundle}"

  protected def bind(text: String, replacements: Map[String, String]): String = {
    val sub =
      new StringSubstitutor((key: String) =>
        replacements.getOrElse(key, "")) // default everything non-set to empty string
    sub.setEscapeChar('\\') // we use \$ as escape, not $$
    sub.setDisableSubstitutionInValues(true) // don't bind variables in values
    sub.replace(text)
  }

  def commonBindings: Map[String, String] = Map(
    "APP_NAME" -> additionalBindings.getOrElse("APP_NAME", displayTestName),
    "DISPLAY_TEST_NAME" -> displayTestName,
    "GROUP_NAME" -> groupName,
    "MODULE_GROUP_NAME" -> additionalBindings.getOrElse("MODULE_GROUP_NAME", s"$metaBundleName-$displayTestName"),
    "MODULE_NAME" -> moduleName,
    // e.g. -cpu 100% -mem 42G -disk 16G -priority 40
    "TREADMILL_OPTS" -> treadmillOpts.toSeq.sortBy(_._1).map { case (k, v) => s"-$k $v" }.mkString(" ")
  )
}

final case class ScalaTestplanEntry(
    displayTestName: String,
    groupName: String,
    metaBundle: MetaBundle,
    treadmillOpts: Map[String, String],
    additionalBindings: Map[String, String],
    stratoOverride: Option[String],
    testTasks: Seq[TestplanTask],
    testTaskOverrides: Map[TestplanTask, Set[String]],
    testModulesFileName: String
) extends TestplanEntry {

  override def copyEntry(
      testTasks: Seq[TestplanTask] = this.testTasks,
      groupName: String = this.groupName): ScalaTestplanEntry =
    copy(testTasks = testTasks, groupName = groupName)

  def bind(value: String): String = {
    val replacements = withAppId(
      Map(
        "GRADLE_ARGS" -> testTasks.map(t => s"-t ${t.module.properPath}.${t.testName}").mkString(" "),
        "REG_PATH_VAR" -> additionalBindings.getOrElse("REG_PATH_VAR", "REG_PATH"),
        // if override is set, pass it to the runner
        "RUNNER_EXTRA" -> stratoOverride.fold("")(value => s"--strato-override $value")
      ) ++ commonBindings ++ additionalBindings)

    bind(value, replacements)
  }

  private def withAppId(bindings: Map[String, String]): Map[String, String] = {
    val key = "EXTRA_STRATO_OPTIONS"
    val groupAppId = s"-Doptimus.testsuite.override.appId=$groupName"
    val newValue = bindings.get(key).fold(groupAppId)(_ + s" $groupAppId")
    bindings.updated(key, newValue)
  }
}

final case class PythonTestplanEntry(
    displayTestName: String,
    groupName: String,
    metaBundle: MetaBundle,
    treadmillOpts: Map[String, String],
    additionalBindings: Map[String, String],
    testTasks: Seq[TestplanTask],
    testTaskOverrides: Map[TestplanTask, Set[String]],
    testModulesFileName: String
) extends TestplanEntry {

  override def copyEntry(
      testTasks: Seq[TestplanTask] = this.testTasks,
      groupName: String = this.groupName): PythonTestplanEntry =
    this.copy(testTasks = testTasks, groupName = groupName)

  val scope: String = testTasks.map(t => t.module.properPath).mkString(" ")

  def bind(value: String): String = {
    // Generate python's artifacts directory, using environment variable e.g. OPTIMUS_PLATFORM_COMMON_REG_PATH
    val projectName = testTasks.map(t => t.module.bundle).headOption.getOrElse("PLATFORM")
    val pythonArtifactsDir = s"OPTIMUS_${projectName.toUpperCase}_COMMON_REG_PATH"

    val replacements =
      Map(
        "PYTHON_ARTIFACTS_DIR" -> pythonArtifactsDir,
        "SCOPE" -> scope
      ) ++ commonBindings ++ additionalBindings

    bind(value, replacements)
  }
}

final case class PactContractTestplanEntry(
    displayTestName: String,
    groupName: String,
    metaBundle: MetaBundle,
    treadmillOpts: Map[String, String],
    additionalBindings: Map[String, String],
    stratoOverride: Option[String],
    testTasks: Seq[TestplanTask],
    testTaskOverrides: Map[TestplanTask, Set[String]],
    testModulesFileName: String
) extends TestplanEntry {

  // Enforce there is only a single entry for each PactContract as otherwise it gets too tricky to manage for the cases
  // where 1 entry might only have a consumer test, another might only have a provider, and a 3rd case that might have
  // both of these
  require(testTasks.size == 1, "PactContract test plans can only contain 1 project per test group")
  require(
    testTaskOverrides.size == 1 && Set(1, 2).contains(testTaskOverrides.values.head.size),
    "PactContract test plans must have at least 1 consumer or provider test"
  )

  override def copyEntry(
      testTasks: Seq[TestplanTask] = this.testTasks,
      groupName: String = this.groupName): PactContractTestplanEntry =
    this.copy(testTasks = testTasks, groupName = groupName)

  def bind(value: String): String = {
    val replacements = {
      val consumerArgs = if (testTaskOverrides.values.head.contains("consumerContractTest")) {
        Map("CONSUMER_ARGS" -> s"-t ${testTasks.head.module.properPath}.consumerContractTest")
      } else Map()

      val providerArgs = if (testTaskOverrides.values.head.contains("providerContractTest")) {
        Map("PROVIDER_ARGS" -> s"-t ${testTasks.head.module.properPath}.providerContractTest")
      } else Map()

      Map(
        "BAS_PATH_VAR" -> additionalBindings.getOrElse("BAS_PATH_VAR", "BAS_PATH"),
        "REG_PATH_VAR" -> additionalBindings.getOrElse("REG_PATH_VAR", "REG_PATH"),
        // if override is set, pass it to the runner
        "RUNNER_EXTRA" -> stratoOverride.fold("")(value => s"--strato-override $value")
      ) ++ consumerArgs ++ providerArgs ++ commonBindings ++ additionalBindings
    }

    bind(value, replacements)
  }
}
