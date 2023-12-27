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
package optimus.buildtool.scope.sources

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

private[sources] final case class SourceFileContent(
    content: Seq[(String, SortedMap[SourceUnitId, HashedContent])],
    generatedSourceArtifacts: Seq[Artifact]
)

private[sources] trait HashedSources {
  def content: Seq[(String, SortedMap[SourceUnitId, HashedContent])]
  def generatedSourceArtifacts: Seq[Artifact]
  def fingerprintHash: String
  def sourceFiles: SortedMap[SourceUnitId, HashedContent] = SortedMap(content.flatMap(_._2): _*)
}

private[sources] final case class HashedSourcesImpl(
    content: Seq[(String, SortedMap[SourceUnitId, HashedContent])],
    generatedSourceArtifacts: Seq[Artifact],
    fingerprintHash: String
) extends HashedSources

@entity private[scope] trait CompilationSources {
  def id: ScopeId

  @node def content: Seq[(String, SortedMap[SourceUnitId, HashedContent])] = hashedSources.content
  @node def compilationSources: SortedMap[SourceUnitId, HashedContent] = hashedSources.sourceFiles
  @node def compilationInputsHash: String = hashedSources.fingerprintHash
  @node def generatedSourceArtifacts: Seq[Artifact] = hashedSources.generatedSourceArtifacts
  @node def isEmpty: Boolean = compilationSources.isEmpty
  @node def containsFile(name: RelativePath): Boolean =
    compilationSources.keySet.exists(_.sourceFolderToFilePath == name)

  @node protected def hashedSources: HashedSources
}
