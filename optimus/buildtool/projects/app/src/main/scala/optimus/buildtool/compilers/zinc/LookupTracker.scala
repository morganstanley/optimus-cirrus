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
package optimus.buildtool.compilers.zinc

import optimus.buildtool.artifacts.AnalysisArtifact
import optimus.buildtool.artifacts.ArtifactId
import optimus.buildtool.artifacts.ClassFileArtifact
import optimus.buildtool.artifacts.DependencyLookup
import optimus.buildtool.artifacts.ExternalId
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.SignatureArtifact
import optimus.buildtool.artifacts.VersionedExternalArtifactId
import optimus.buildtool.compilers.SyncCompiler.Inputs
import optimus.scalacompat.collection._
import xsbti.VirtualFileRef
import xsbti.api.AnalyzedClass

import scala.collection.compat._
import scala.collection.immutable.Seq

final case class SingleRawLookup(classname: String, file: Option[String])

class LookupTracker {
  val lookupHistory = List.newBuilder[SingleRawLookup]

  def update(binaryClassName: String, file: Option[VirtualFileRef], result: Option[AnalyzedClass]): Unit = {
    lookupHistory += SingleRawLookup(
      binaryClassName,
      file.map(_.id)
    )
  }

  def messages(inputs: Inputs): (Seq[DependencyLookup.Internal], Seq[DependencyLookup.External]) = {
    // note: plugins are not included within the input artifacts because they are never looked up by zinc
    val pathsToIds = inputs.inputArtifacts.collect {
      case c: ClassFileArtifact => c.file.pathString -> c.id
      case c: SignatureArtifact => c.signatureFile.pathString -> c.id
      case c: AnalysisArtifact  => c.analysisFile.pathString -> c.id
    }.toMap

    val rawLookups = lookupHistory.result()

    val resolvedLookups: Seq[(ArtifactId, String)] = for {
      rawLookup <- rawLookups
      (className, maybeFile) = (rawLookup.classname, rawLookup.file)
      lookupFile <- maybeFile
      artifactId <- pathsToIds.get(lookupFile)
    } yield artifactId -> className

    val lookups: Map[ArtifactId, Seq[String]] = resolvedLookups
      .groupBy { case (id, _) => id }
      .mapValuesNow { classes => classes.map { case (_, classes) => classes }.distinct }

    val internals = lookups
      .collect { case (id: InternalArtifactId, classes) =>
        DependencyLookup(id.scopeId, classes)
      }
      .to(Seq)
    val externals = lookups
      .collect { case (id: VersionedExternalArtifactId, classes) =>
        DependencyLookup(ExternalId(id), classes)
      }
      .to(Seq)

    (internals, externals)
  }
}
