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

import optimus.buildtool.artifacts.ArtifactType.RegexMessagesFingerprint
import optimus.buildtool.config.CodeFlaggingRule
import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import scala.collection.immutable.SortedMap

@entity
class RegexMessagesCompilationSources(
    scope: CompilationScope,
    sourceSources: SourceCompilationSources,
    resourceSources: ResourceCompilationSourcesImpl,
    rules: Seq[CodeFlaggingRule]
) extends CompilationSources {
  import CompilationSources._

  override def id: ScopeId = scope.id

  // Note: we call `staticContent` here rather than `content` since we don't want to scan the generated sources
  @node private def allContent =
    Seq(staticContent(sourceSources.content, Source), staticContent(resourceSources.content, Resource))

  private def staticContent(content: Seq[(String, SortedMap[SourceUnitId, HashedContent])], tpe: String) = {
    val m = content.toMap
    tpe -> m(tpe)
  }

  @node def nonEmpty: Boolean = allContent.map(_._2).exists(_.nonEmpty)

  @node private def rulesFingerprint = rules.map(r => s"[Rule]${r.fingerprint}")

  @node override protected def hashedSources: HashedSources = {
    val sourceFingerprint = allContent.apar.flatMap { case (tpe, content) =>
      scope.fingerprint(content, tpe)
    }

    val fingerprint = sourceFingerprint ++ rulesFingerprint
    val hash = scope.hasher.hashFingerprint(fingerprint, RegexMessagesFingerprint)
    HashedSourcesImpl(allContent, hash, fingerprint)
  }

}
