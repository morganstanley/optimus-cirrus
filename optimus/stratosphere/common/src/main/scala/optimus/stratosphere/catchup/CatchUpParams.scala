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
package optimus.stratosphere.catchup

import optimus.stratosphere.config.StratoWorkspaceCommon

final case class CatchUpParams(
    remoteName: String,
    branchToUse: String,
    strategy: CatchUpStrategy,
    ignoreLocalChanges: Boolean,
    maxDistance: Int = Integer.MAX_VALUE,
    noFetch: Boolean = false,
    checkout: Boolean = false,
    merge: Boolean = false,
    rebase: Boolean = false,
    scalaMajorVersion: Option[String] = None,
    dryRun: Boolean = false,
    ignoreWrongBranch: Boolean = false,
    allowUnverified: Boolean = false,
    disableInteractive: Boolean = false,
    gitRef: String = "FETCH_HEAD"
)

object CatchUpParams {

  def withWorkspaceDefaults(stratoWorkspace: StratoWorkspaceCommon): CatchUpParams =
    CatchUpParams(
      stratoWorkspace.catchUp.remote.name,
      stratoWorkspace.catchUp.defaultBranch,
      CatchUpStrategy(stratoWorkspace.catchUp.defaultStrategy),
      stratoWorkspace.catchUp.ignoreLocalChanges
    )
}
