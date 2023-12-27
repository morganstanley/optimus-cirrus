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
import optimus.buildtool.config.ScopeId
import optimus.buildtool.scope.CompilationScope
import optimus.platform._

import scala.collection.immutable.Seq

@entity
class RegexMessagesCompilationSources(
    scope: CompilationScope,
    sourceSources: SourceCompilationSources,
    resourceSources: ResourceCompilationSourcesImpl
) extends CompilationSources {

  override def id: ScopeId = scope.id

  // Note: we call `staticContent` here rather than `content` since we don't want to scan the generated sources
  @node private def allContent = Seq(sourceSources.staticContent, resourceSources.staticContent)

  @node def nonEmpty: Boolean = allContent.map(_._2).exists(_.nonEmpty)

  @node override protected def hashedSources: HashedSources = {
    val sourceFingerprint = allContent.apar.flatMap { case (tpe, content) =>
      scope.fingerprint(content, tpe)
    }
    val ruleFingerprint = scope.config.regexConfig.toIndexedSeq.flatMap(_.rules).map(r => s"[Rule]${r.fingerprint}")

    val hash = scope.hasher.hashFingerprint(sourceFingerprint ++ ruleFingerprint, RegexMessagesFingerprint)
    // `generatedSourceArtifacts = Nil` since we're not dependent on any source generation
    HashedSourcesImpl(allContent, generatedSourceArtifacts = Nil, hash)
  }
}
