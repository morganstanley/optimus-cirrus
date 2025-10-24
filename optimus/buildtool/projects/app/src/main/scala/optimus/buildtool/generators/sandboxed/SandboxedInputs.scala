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
package optimus.buildtool.generators.sandboxed

import optimus.buildtool.artifacts.FingerprintArtifact
import optimus.buildtool.files.Directory.PathFilter
import optimus.buildtool.files.ReactiveDirectory
import optimus.buildtool.files.SourceFolder
import optimus.platform._

/**
 * A builder for a file sandbox, pre-populated with the generator template files.
 */
@entity trait SandboxedInputs extends Sandboxed {
  import SandboxedFiles._

  protected def sourcePredicate: PathFilter

  /**
   * Add new files from the existing folders, with a different predicate.
   */
  @node def withFiles(
      fileType: FileType,
      sourcePredicate: PathFilter,
      fingerprintTypeSuffix: String = "",
      allowEmpty: Boolean = false
  ): SandboxedInputs

  /**
   * Add files from the provided folders, optionally with a different predicate.
   */
  @node def withFolders(
      fileType: FileType,
      internalFolders: Seq[SourceFolder],
      externalFolders: Seq[ReactiveDirectory],
      sourcePredicate: PathFilter = sourcePredicate,
      fingerprintTypeSuffix: String = "",
      allowEmpty: Boolean = false
  ): SandboxedInputs

  /**
   * Generate and hash the fingerprint for the sandboxed files, including the configuration fingerprint and
   * optionally any additional items.
   */
  @node def hashFingerprint(configuration: Map[String, String], extraFingerprint: Seq[String]): FingerprintArtifact

  /**
   * Once the sandbox is populated, converted it to a form usable for the generation phase.
   */
  def toFiles: SandboxedFiles
}
