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
package optimus.buildtool.compilers.zinc.mappers

import java.nio.file.Path

import optimus.buildtool.artifacts.ArtifactType
import optimus.buildtool.artifacts.InternalArtifactId
import optimus.buildtool.artifacts.PathedArtifact
import optimus.buildtool.config.NamingConventions
import optimus.buildtool.config.ScopeId

import scala.collection.mutable

private[zinc] class MappingTrace {
  var hashUpdatesOnRead = 0
  val unchangedProvenances = new mutable.HashSet[String]
}

object MappingTrace {
  // format: artifact-type/scopeId.hash
  def provenance(tpe: ArtifactType, scopeName: String, hash: String): String = s"${tpe.name}/$scopeName.$hash".intern()

  def provenance(id: InternalArtifactId, a: PathedArtifact): String =
    provenance(id.tpe, id.scopeId.properPath, hash(a.path))

  def hash(p: Path): String = {
    // safe to use `toString` here, since we're only parsing the filename portion of the path
    val pathString = p.getFileName.toString
    val hashIndex = pathString.indexOf(s".${NamingConventions.HASH}") + 1
    pathString.substring(hashIndex, pathString.indexOf('.', hashIndex))
  }

  def incr(p: Path): Boolean = {
    // safe to use `toString` here, since we're only parsing the filename portion of the path
    p.getFileName.toString.contains(".INCR.")
  }

  def artifactType(provenance: String): ArtifactType =
    ArtifactType.parse(provenance.substring(0, provenance.indexOf('/')))

  def scopeId(provenance: String): ScopeId =
    ScopeId.parse(provenance.substring(provenance.indexOf('/') + 1, provenance.lastIndexOf('.')))

}
