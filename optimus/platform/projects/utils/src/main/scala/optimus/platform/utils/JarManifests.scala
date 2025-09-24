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
package optimus.platform.utils

import java.util.jar.Attributes.Name

object JarManifests {
  object nme {
    val CodeFingerprint = new Name("MS-code-fingerprint") // pathing jar fingerprint
    val PrId = new Name("MS-pr-id") // e.g. codetree-pr/123
    val CiBuildNr = new Name("MS-ci-build-nr")
    val BuildUser = new Name("MS-build-user")
    val BuildOSVersion = new Name("MS-build-OS-version")
    val BuildBranch = new Name("MS-build-branch")
    val CommitHash = new Name("MS-commit-hash") // in PR builds, this is the feature branch HEAD, not CI's merge commit
    val BaselineCommit = new Name("MS-baseline-commit")
    val BaselineDistance = new Name("MS-baseline-distance") // commit distance from the baseline commit
    val UncommittedChanges = new Name("MS-uncommitted-changes") // true/false
  }
}
