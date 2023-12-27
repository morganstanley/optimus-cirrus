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
package optimus.buildtool.files

import optimus.buildtool.artifacts.GeneratedSourceArtifactType
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId
import optimus.buildtool.utils.OptimusBuildToolAssertions

private[buildtool] sealed trait SourceUnitId {

  // Relative path from some "root" -> source file. For handwritten sources this will be unique, but
  // it may not be for generated sources.
  def localRootToFilePath: RelativePath

  // Relative path from sourceFolder -> source file. Note that this path is not generally sufficient
  // to uniquely identify a source artifact.
  def sourceFolderToFilePath: RelativePath

  lazy val suffix: String = NamingConventions.suffix(id)

  lazy val id: String = sourceFolderToFilePath.pathString

  override def toString: String = id
}

object SourceUnitId {
  // Note: We're ordering based on String rather than Path here, since path comparison is case-sensitive on linux
  // but case-insensitive on windows. This leads to remote cache misses when reusing artifacts between OSes.
  implicit val ordering: Ordering[SourceUnitId] = Ordering.by(_.id)
  implicit val fileOrdering: Ordering[SourceFileId] = Ordering.by(_.sourceFolderToFilePathString)
}

private[buildtool] final case class SourceFileId(
    // Relative path from workspaceSrcRoot -> source file. Unique path within the workspace.
    workspaceSrcRootToSourceFilePath: RelativePath,
    override val sourceFolderToFilePath: RelativePath
) extends SourceUnitId {
  OptimusBuildToolAssertions.require(
    workspaceSrcRootToSourceFilePath.path.endsWith(sourceFolderToFilePath.path),
    s"workspace-relative path $workspaceSrcRootToSourceFilePath does not match $sourceFolderToFilePath"
  )
  lazy val sourceFolderToFilePathString: String = sourceFolderToFilePath.pathString
  override def localRootToFilePath: RelativePath = workspaceSrcRootToSourceFilePath
}

private[buildtool] final case class GeneratedSourceUnitId(
    scopeId: ScopeId,
    generatorName: String,
    tpe: GeneratedSourceArtifactType,
    jar: JarAsset,
    jarRootToSourceFilePath: RelativePath,
    override val sourceFolderToFilePath: RelativePath
) extends SourceUnitId {
  override def localRootToFilePath: RelativePath = jarRootToSourceFilePath
}
