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
  val testModulesFileName: String

  def bind(value: String): String
  def copyEntry(testTasks: Seq[TestplanTask] = this.testTasks, groupName: String = this.groupName): TestplanEntry

  val moduleName: String = additionalBindings.getOrElse("MODULE_NAME", groupName)

  protected def bind(text: String, replacements: Map[String, String]): String = {
    val sub =
      new StringSubstitutor((key: String) =>
        replacements.getOrElse(key, "")) // default everything non-set to empty string
    sub.setEscapeChar('\\') // we use \$ as escape, not $$
    sub.setDisableSubstitutionInValues(true) // don't bind variables in values
    sub.replace(text)
  }
}

final case class ScalaTestplanEntry(
    displayTestName: String,
    groupName: String,
    metaBundle: MetaBundle,
    treadmillOpts: Map[String, String],
    additionalBindings: Map[String, String],
    stratoOverride: Option[String],
    testTasks: Seq[TestplanTask],
    testModulesFileName: String
) extends TestplanEntry {

  override def copyEntry(
      testTasks: Seq[TestplanTask] = this.testTasks,
      groupName: String = this.groupName): ScalaTestplanEntry =
    copy(testTasks = testTasks, groupName = groupName)

  def bind(value: String): String = {
    val metaBundleName = s"${metaBundle.meta}-${metaBundle.bundle}"
    val replacements = withAppId(
      Map(
        "APP_NAME" -> additionalBindings.getOrElse("APP_NAME", displayTestName),
        "DISPLAY_TEST_NAME" -> displayTestName,
        "GROUP_NAME" -> groupName,
        "GRADLE_ARGS" -> testTasks.map(t => s"-t ${t.module.properPath}.${t.testName}").mkString(" "),
        "MODULE_GROUP_NAME" -> additionalBindings.getOrElse("MODULE_GROUP_NAME", s"$metaBundleName-$displayTestName"),
        "MODULE_NAME" -> moduleName,
        "REG_PATH_VAR" -> additionalBindings.getOrElse("REG_PATH_VAR", "REG_PATH"),
        // if override is set, pass it to the runner
        "RUNNER_EXTRA" -> stratoOverride.fold("")(value => s"--strato-override $value"),
        // e.g. -cpu 100% -mem 42G -disk 16G -priority 40
        "TREADMILL_OPTS" -> treadmillOpts.toSeq.sortBy(_._1).map { case (k, v) => s"-$k $v" }.mkString(" ")
      ) ++ additionalBindings)

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
    testModulesFileName: String
) extends TestplanEntry {

  override def copyEntry(
      testTasks: Seq[TestplanTask] = this.testTasks,
      groupName: String = this.groupName): PythonTestplanEntry =
    this.copy(testTasks = testTasks, groupName = groupName)

  val scope: String = testTasks.map(t => t.module.properPath).mkString(" ")

  def bind(value: String): String = {
    val metaBundleName = s"${metaBundle.meta}-${metaBundle.bundle}"
    // Generate python's artifacts directory, using environment variable e.g. OPTIMUS_PLATFORM_COMMON_REG_PATH
    val projectName = testTasks.map(t => t.module.bundle).headOption.getOrElse("PLATFORM")
    val pythonArtifactsDir = s"OPTIMUS_${projectName.toUpperCase}_COMMON_REG_PATH"

    val replacements =
      Map(
        "APP_NAME" -> additionalBindings.getOrElse("APP_NAME", displayTestName),
        "DISPLAY_TEST_NAME" -> displayTestName,
        "GROUP_NAME" -> groupName,
        "MODULE_GROUP_NAME" -> additionalBindings.getOrElse("MODULE_GROUP_NAME", s"$metaBundleName-$displayTestName"),
        "MODULE_NAME" -> moduleName,
        "PYTHON_ARTIFACTS_DIR" -> pythonArtifactsDir,
        "SCOPE" -> scope,
        // e.g. -cpu 100% -mem 42G -disk 16G -priority 40
        "TREADMILL_OPTS" -> treadmillOpts.toSeq.sortBy(_._1).map { case (k, v) => s"-$k $v" }.mkString(" ")
      ) ++ additionalBindings

    bind(value, replacements)
  }

}
