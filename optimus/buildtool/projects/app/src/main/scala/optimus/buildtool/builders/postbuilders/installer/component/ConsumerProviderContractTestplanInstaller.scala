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

import scala.collection.immutable.Seq

trait ConsumerProviderContractTestplanInstaller { self: TestplanInstaller =>

  object ConsumerProviderContractPathNames {
    def templateFile(os: String): RelativePath = RelativePath(s"consumerProviderContract.testplan.$os.pre.template")
  }

  val consumerProviderContractTestplan: Seq[(String, String => RelativePath)] = Seq(
    ("pact", ConsumerProviderContractPathNames.templateFile(_)),
  )

  @node def generateConsumerProviderContractTestplan(
    os: String,
    consumerProviderContractRunData: Seq[TestplanEntry]): Seq[TestPlan] =
    consumerProviderContractRunData.apar.map { data =>
      val consumerProviderTestplans =
        consumerProviderContractTestplan.apar.map { case (_, templateFileFunc) =>
          generateTestplan(
            Seq(data.copyEntry(groupName = data.groupName)),
            loadTestplanTemplate(os, templateFileFunc(os))
          )
        }
      TestPlan.merge(consumerProviderTestplans)
    }

  def createConsumerProviderContractTestplanEntry(
      displayTestName: String,
      groupName: String,
      metaBundle: MetaBundle,
      treadmillOpts: Map[String, String],
      additionalBindings: Map[String, String],
      stratoOverride: Option[String],
      testModulesFileName: String,
      testTasks: Seq[TestplanTask]): TestplanEntry = ConsumerProviderContractTestplanEntry(
    displayTestName = displayTestName,
    groupName = groupName,
    metaBundle = metaBundle,
    treadmillOpts = treadmillOpts,
    additionalBindings = additionalBindings,
    stratoOverride = stratoOverride,
    testModulesFileName = testModulesFileName,
    testTasks = testTasks
  )
}
