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

class MigrationTrackerToolWerr extends MigrationTrackerTool

private[buildtool] trait MigrationTrackerToolWerrParams extends TrackerToolParams {
  override val frontierScope = "optimus.onboarding.scala_2_13_frontier_werr.main"
  override val frontierObtFile = "optimus/onboarding/projects/scala_2_13_frontier_werr/scala_2_13_frontier_werr.obt"
  override val rulesYaml = None
  override val allFrontierScope: Option[String] = Some("optimus.onboarding.scala_2_13_frontier.main")
}

private[buildtool] trait MigrationTrackerToolWerrT extends MigrationTrackerToolT with MigrationTrackerToolWerrParams

private[buildtool] object MigrationTrackerToolWerr extends MigrationTrackerToolWerrT {
  def indent: Int = 12
}
