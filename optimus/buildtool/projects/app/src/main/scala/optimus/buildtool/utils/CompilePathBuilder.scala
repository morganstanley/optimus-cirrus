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
package optimus.buildtool.utils

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.JarAsset
import optimus.platform._

final case class CompilePathBuilder(outputDir: Directory) {

  def outputDirFor(tpe: ArtifactType, discriminator: Option[String]): Directory =
    Utils.outputDirFor(outputDir, tpe, discriminator)

  @scenarioIndependent @node def outputPathFor(
      id: ScopeId,
      fingerprintHash: String,
      tpe: ArtifactType,
      discriminator: Option[String],
      incremental: Boolean
  ): FileAsset =
    Utils.outputPathFor(outputDir, tpe, discriminator, fingerprintHash, id, incremental)

  @scenarioIndependent @node def signatureOutPath(
      id: ScopeId,
      fingerprintHash: String,
      incremental: Boolean
  ): JarAsset =
    outputPathFor(id, fingerprintHash, ArtifactType.JavaAndScalaSignatures, None, incremental).asJar

  @scenarioIndependent @node def scalaOutPath(id: ScopeId, fingerprintHash: String, incremental: Boolean): JarAsset =
    outputPathFor(id, fingerprintHash, ArtifactType.Scala, None, incremental).asJar
  @scenarioIndependent @node def javaOutPath(id: ScopeId, fingerprintHash: String, incremental: Boolean): JarAsset =
    outputPathFor(id, fingerprintHash, ArtifactType.Java, None, incremental).asJar
  @scenarioIndependent @node def resourceOutPath(id: ScopeId, fingerprintHash: String): JarAsset =
    outputPathFor(id, fingerprintHash, ArtifactType.Resources, None, incremental = false).asJar
  @scenarioIndependent @node def electronOutPath(id: ScopeId, fingerprintHash: String): JarAsset =
    outputPathFor(id, fingerprintHash, ArtifactType.Electron, None, incremental = false).asJar

}
