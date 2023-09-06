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

import java.nio.file.Path

final case class ScopeId(org: String, name: String)
// example: ("optimus", "app")

final case class ScopeConfiguration(scopeId: ScopeId, scopeDependencies: Seq[ScopeId], sourceVersion: Int)
/*
scopeId: ("optimus", "app")
scopeDeps: [
  ScopeId("optimus", "model")
  ScopeId("optimus", "data")
]
sourceVersion: x1y1
 */

sealed trait Artifact {
  def scopeId: ScopeId
  def fingerprint: Map[ScopeId, Int]
}
/*
scopeId: ("optimus", "app")
fingerprint: {
  “optimus.app” -> 1a2b
  “optimus.data” -> 3c4d
  “optimus.model” -> 5e6f
  “scalaVersion” -> 2.13.11
  “-Xfatal-warnings” -> “true”
  …
}
 */
final case class ClassFileArtifact(scopeId: ScopeId, fingerprint: Map[ScopeId, Int], classFiles: Seq[Path])
    extends Artifact
final case class SignatureArtifact(scopeId: ScopeId, fingerprint: Map[ScopeId, Int], signature: Path) extends Artifact

final case class UpstreamDependencies(signatures: Seq[SignatureArtifact], fingerprint: Map[ScopeId, Int])

final case class BuildResult(artifacts: Set[ClassFileArtifact])

class BuildTool(configPerScope: Map[ScopeId, ScopeConfiguration]) {

  @node def extractSignature(config: ScopeConfiguration, sigDeps: Seq[SignatureArtifact]): Path = ???
  @node def doCompile(config: ScopeConfiguration, sigDeps: Seq[SignatureArtifact]): Seq[Path] = ???

  @node def buildScopes(scopes: Set[ScopeId]): BuildResult = {
    val configs = scopes.map(configPerScope)
    val artifacts = configs.map(compile)
    BuildResult(artifacts)
  }

  @node def compile(config: ScopeConfiguration): ClassFileArtifact = {
    val signatureArtifact = signatures(config)
    val classFileArtifact = classes(config, signatureArtifact)
    classFileArtifact
  }

  @node def signatures(config: ScopeConfiguration): SignatureArtifact = {
    val deps = inputsFromUpstreams(config)
    val signature = extractSignature(config, deps.signatures)
    SignatureArtifact(config.scopeId, deps.fingerprint + (config.scopeId -> config.sourceVersion), signature)
  }

  @node def classes(config: ScopeConfiguration, sig: SignatureArtifact): ClassFileArtifact = {
    val deps = inputsFromUpstreams(config)
    val classFiles = doCompile(config, sig +: deps.signatures)
    ClassFileArtifact(config.scopeId, deps.fingerprint + (config.scopeId -> config.sourceVersion), classFiles)
  }

  @node def inputsFromUpstreams(config: ScopeConfiguration): UpstreamDependencies = {
    val depConfigs = config.scopeDependencies.map(configPerScope)
    val depSignatures = depConfigs.map(signatures) // recursive step!
    val depFingerprint = depSignatures.flatMap(_.fingerprint).toMap
    UpstreamDependencies(depSignatures, depFingerprint)
  }
}
