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

import optimus.buildtool.builders.postbuilders.installer.component.testplans._
import optimus.buildtool.config.MetaBundle
import optimus.buildtool.files.RelativePath
import optimus.platform._

trait PyTestplanTestplanInstaller { self: TestplanInstaller =>

  object PyTestplanTestplanTemplate {
    val requiredAdditionalBindings: Set[String] = Set("PY_TESTPLAN_COMMAND", "PY_TESTPLAN_RESULTS")
    val templateFile: RelativePath = RelativePath("pyTestplan.testplan.unix.pre.template")
  }

  @node def generatePyTestplanTestplan(pyTestplanEntries: Seq[PyTestplanTestplanEntry]): Seq[TestPlan] =
    Seq(generateTestplan(pyTestplanEntries, loadTestplanTemplate(PyTestplanTestplanTemplate.templateFile)))

  def createPyTestplanTestplanEntry(
      displayTestName: String,
      groupName: String,
      metaBundle: MetaBundle,
      treadmillOpts: Map[String, String],
      additionalBindings: Map[String, String],
      testTasks: Seq[TestplanTask],
      testTaskOverrides: Map[TestplanTask, Set[String]],
      testModulesFileName: String): PyTestplanTestplanEntry = PyTestplanTestplanEntry(
    displayTestName = displayTestName,
    groupName = groupName,
    metaBundle = metaBundle,
    treadmillOpts = treadmillOpts,
    additionalBindings = additionalBindings,
    testModulesFileName = testModulesFileName,
    testTasks = testTasks,
    testTaskOverrides = testTaskOverrides
  )

}
