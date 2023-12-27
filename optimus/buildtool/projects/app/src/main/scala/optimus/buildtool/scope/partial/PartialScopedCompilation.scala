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
package optimus.buildtool.scope.partial

import optimus.buildtool.artifacts.Artifact
import optimus.buildtool.artifacts.CachedArtifactType
import optimus.buildtool.artifacts.LocatorArtifact
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.CompilationSources
import optimus.buildtool.scope.sources.JavaAndScalaCompilationSources
import optimus.core.needsPlugin
import optimus.platform._
import optimus.platform.annotations.alwaysAutoAsyncArgs

import scala.collection.immutable.Seq

@entity private[partial] trait PartialScopedCompilation {
  protected def scope: CompilationScope
  protected def sources: CompilationSources

  @node protected def fingerprintHash: String = sources.compilationInputsHash

  @node protected def upstreamArtifacts: Seq[Artifact]

  // generatedSourceArtifacts can have errors too
  @node protected def prerequisites: Seq[Artifact] = sources.generatedSourceArtifacts ++ upstreamArtifacts
  @node private def upstreamErrors: Option[Seq[Artifact]] = Artifact.onlyErrors(prerequisites)

  @node protected def containsRelevantSources: Boolean

  @alwaysAutoAsyncArgs
  protected def compile[A <: CachedArtifactType](tpe: A, discriminator: Option[String])(
      f: => Option[A#A]
  ): Seq[Artifact] = needsPlugin
  @node protected def compile$NF[A <: CachedArtifactType](tpe: A, discriminator: Option[String])(
      f: NodeFunction0[Option[A#A]]
  ): Seq[Artifact] =
    if (containsRelevantSources) upstreamErrors.getOrElse(scope.cached$NF(tpe, discriminator, fingerprintHash)(f))
    else Nil

  override def toString: String = s"${getClass.getSimpleName}(${scope.id})"
}

private[buildtool] final case class AnalysisWithLocator(analysis: Seq[Artifact], locator: Option[LocatorArtifact])

@entity private[partial] trait PartialScopedClassCompilation extends PartialScopedCompilation {
  protected def sources: JavaAndScalaCompilationSources

  // externalCompileDependencies can have errors too
  @node override protected def prerequisites: Seq[Artifact] = sources.externalCompileDependencies ++ super.prerequisites

}
