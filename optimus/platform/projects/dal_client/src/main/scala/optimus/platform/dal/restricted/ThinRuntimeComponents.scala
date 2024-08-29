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
package optimus.platform.dal.restricted

import java.time.Instant
import optimus.config.RuntimeComponents
import optimus.config.RuntimeConfiguration
import optimus.platform.dal.EntityResolver
import optimus.platform.dal.RestrictedEntityResolver
import optimus.platform.RuntimeEnvironment
import optimus.platform.Scenario
import optimus.platform.ScenarioStack

/**
 * Only Dist should use this runtime component
 */
private[optimus] final class ThinRuntimeComponents(
    runtimeConfig: RuntimeConfiguration,
    resolver: RestrictedEntityResolver)
    extends RuntimeComponents(runtimeConfig) {

  def createEntityResolver(): EntityResolver = resolver
  def createRuntime(): RuntimeEnvironment = new RuntimeEnvironment(this, resolver)

  private[optimus] def newTopScenario(resolver: EntityResolver): Scenario =
    throw new UnsupportedOperationException(
      "Method newTopScenario under ThinRuntimeCompenents is not allowed to be used")
  private[optimus] def initialRuntimeScenarioStack(ss: ScenarioStack, initialTime: Instant): ScenarioStack =
    throw new UnsupportedOperationException(
      "Method initialRuntimeScenarioStack under ThinRuntimeCompenents is not allowed to be used")

  private[optimus] override def isThin: Boolean = true
}
