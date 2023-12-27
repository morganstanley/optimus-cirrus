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

import optimus.buildtool.files.RelativePath
import optimus.buildtool.builders.postbuilders.installer.component.testplans._
import optimus.buildtool.config.TestplanConfiguration
import optimus.platform._

import scala.collection.immutable.Seq

trait PythonTestplanInstaller { self: TestplanInstaller =>

  object PythonPathNames {
    def pylintTemplateFile(os: String): RelativePath = RelativePath(s"pylint.testplan.$os.python.template")
    def pycodeStyleTemplateFile(os: String): RelativePath = RelativePath(s"pycodestyle.testplan.$os.python.template")
  }

  val pythonQualityTestplans: Seq[(String, String => RelativePath)] = Seq(
    ("pylint", PythonPathNames.pylintTemplateFile(_)),
    ("pycodestyle", PythonPathNames.pycodeStyleTemplateFile(_))
  )

  @node def generatePythonQualityTestplan(os: String, pythonData: Seq[TestplanEntry]): Seq[TestPlan] =
    pythonData.apar.map { data =>
      // For each python testGroups add pylint and pycodestyle testplans
      val qualityTestplans =
        pythonQualityTestplans.apar.map { case (quality_test_name, templateFileFunc) =>
          generateTestplan(
            Seq(data.copyEntry(groupName = s"$quality_test_name-${data.groupName}")),
            loadTestplanTemplate(os, templateFileFunc(os)))
        }
      TestPlan.merge(qualityTestplans)
    }

  def createPythonTestplanEntry(
      testType: TestType,
      testGroup: Group,
      testTasks: Seq[TestplanTask],
      additionalBinding: Map[String, String],
      testplanConfig: TestplanConfiguration): TestplanEntry = PythonTestplanEntry(
    displayTestName = "PythonCodeQuality",
    groupName = testGroup.name,
    metaBundle = testGroup.metaBundle,
    treadmillOpts = testType.treadmillOptsFor(testGroup, testplanConfig),
    additionalBindings = additionalBinding,
    testModulesFileName = testType.testGroupsOpts.testModulesFileName,
    testTasks = testTasks
  )

}
