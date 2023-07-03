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
package scala.days.live

import optimus.platform._

// Starting point for live demos
final case class ScopeId(meta: String, bundle: String, module: String)

final case class ScopeConfiguration(scopeId: ScopeId, dependencies: Seq[ScopeId], sourceVersion: Int)

sealed trait Artifact {
  def scopeId: ScopeId
  def fingerprint: Map[ScopeId, Int]
}

final case class ClassFileArtifact(scopeId: ScopeId, fingerprint: Map[ScopeId, Int]) extends Artifact
final case class SignatureArtifact(scopeId: ScopeId, fingerprint: Map[ScopeId, Int]) extends Artifact

final case class BuildResult(artifacts: Set[ClassFileArtifact])
