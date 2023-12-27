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
import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.compilers.JarPackager
import optimus.buildtool.files.RelativePath
import optimus.buildtool.files.SourceUnitId
import optimus.buildtool.scope.CompilationScope
import optimus.buildtool.scope.sources.ArchivePackageSources
import optimus.buildtool.scope.sources.ResourceCompilationSources
import optimus.buildtool.trace.Resources
import optimus.buildtool.trace.WarSources
import optimus.buildtool.utils.HashedContent
import optimus.platform._

import scala.collection.immutable.Seq
import scala.collection.immutable.SortedMap

final case class HashedResources(
    resourceFiles: SortedMap[SourceUnitId, HashedContent],
    fingerprintHash: String
)

@entity private[scope] class ResourcePackaging(
    override protected val scope: CompilationScope,
    override protected val sources: ResourceCompilationSources,
    resourcePackager: JarPackager
) extends PartialScopedCompilation {
  import ResourcePackaging._
  import scope._

  @node override protected def containsRelevantSources: Boolean = !sources.isEmpty

  @node override protected def upstreamArtifacts: Seq[Artifact] = Seq()

  @node def resources: Seq[Artifact] =
    compile(ArtifactType.Resources, None)(resourcePackager.artifact(id, resourcePackagerInputsN))

  @node private[scope] def relevantResourcesForDownstreams: Seq[Artifact] = {
    val downstreamsNeedResources =
      config.containsMacros || config.containsPlugin || sources.containsFile(AnnotationProcessorSvcFile)
    if (downstreamsNeedResources) resources else Nil
  }

  private val resourcePackagerInputsN = asNode(() => resourcePackagerInputs)

  @node private def resourcePackagerInputs =
    JarPackager.Inputs(
      Resources,
      ArtifactType.Resources,
      pathBuilder.resourceOutPath(id, sources.compilationInputsHash),
      sources.compilationSources,
      scope.config.resourceTokens
    )
}

private[scope] object ResourcePackaging {
  final val AnnotationProcessorSvcFile =
    RelativePath(s"META-INF/services/" + classOf[javax.annotation.processing.Processor].getName)
}

@entity private[scope] class ArchivePackaging(
    override protected val scope: CompilationScope,
    override protected val sources: ArchivePackageSources,
    packager: JarPackager
) extends PartialScopedCompilation {
  import scope._

  @node override protected def containsRelevantSources: Boolean = !sources.isEmpty

  @node override protected def upstreamArtifacts: Seq[Artifact] = Seq()

  @node def archiveContents: Seq[Artifact] =
    compile(ArtifactType.ArchiveContent, None)(packager.artifact(id, packagerInputsN))

  private val packagerInputsN = asNode(() => packagerInputs)

  @node private def packagerInputs =
    JarPackager.Inputs(
      WarSources,
      ArtifactType.ArchiveContent,
      pathBuilder
        .outputPathFor(id, sources.compilationInputsHash, ArtifactType.ArchiveContent, None, incremental = false)
        .asJar,
      sources.compilationSources,
      scope.config.resourceTokens
    )
}
