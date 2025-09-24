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
import optimus.buildtool.config.MetaBundle
import optimus.platform._

trait PactContractTestplanInstaller { self: TestplanInstaller =>

  object PactContractTestplanTemplate {
    def templateFile(contractTypes: Set[String]): RelativePath = {
      val hasConsumer = contractTypes.contains("consumerContractTest")
      val hasProvider = contractTypes.contains("providerContractTest")

      val template = {
        if (hasConsumer && !hasProvider) "pactContract-consumerOnly.testplan.unix.pre.template"
        else if (!hasConsumer && hasProvider) "pactContract-providerOnly.testplan.unix.pre.template"
        else "pactContract-consumerProvider.testplan.unix.pre.template"
      }

      RelativePath(template)
    }
  }

  @node def generatePactContractTestplan(
      os: String,
      pactContractEntries: Seq[PactContractTestplanEntry]): Seq[TestPlan] =
    pactContractEntries.apar.map { entry =>
      val testplanRows = {
        // We enforce each entry has only a single corresponding test task
        val contractTypes = entry.testTaskOverrides.values.head
        val template = loadTestplanTemplate(PactContractTestplanTemplate.templateFile(contractTypes))
        val newEntry = entry.copyEntry(groupName = entry.groupName)

        TestPlan(template.header, Seq(template.values.map(newEntry.bind)))
      }
      TestPlan.merge(Seq(testplanRows))
    }

  def createPactContractTestplanEntry(
      displayTestName: String,
      groupName: String,
      metaBundle: MetaBundle,
      treadmillOpts: Map[String, String],
      additionalBindings: Map[String, String],
      stratoOverride: Option[String],
      testModulesFileName: String,
      testTasks: Seq[TestplanTask],
      testTaskOverrides: Map[TestplanTask, Set[String]]): TestplanEntry = PactContractTestplanEntry(
    displayTestName = displayTestName,
    groupName = groupName,
    metaBundle = metaBundle,
    treadmillOpts = treadmillOpts,
    additionalBindings = additionalBindings,
    stratoOverride = stratoOverride,
    testModulesFileName = testModulesFileName,
    testTasks = testTasks,
    testTaskOverrides = testTaskOverrides
  )
}
