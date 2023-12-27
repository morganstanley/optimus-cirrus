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
package optimus.buildtool.tools

class MavenTrackerTool extends MigrationTrackerTool {
  val frontierScope = "optimus.onboarding.artifactory_frontier.main"
}

private[buildtool] trait MavenTrackerParams extends TrackerToolParams {
  override val frontierScope = "optimus.onboarding.artifactory_frontier.main"
  override val frontierObtFile = "optimus/onboarding/projects/artifactory_frontier/artifactory_frontier.obt"
  override val rulesYaml = Some("auto-build-rules/obt-rules.yaml")
}

private[buildtool] object MavenTrackerTool extends MigrationTrackerToolT with MavenTrackerParams {
  def indent: Int = 8
}
