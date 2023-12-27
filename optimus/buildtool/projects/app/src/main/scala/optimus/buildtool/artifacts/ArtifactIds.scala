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
package optimus.buildtool.artifacts

import optimus.buildtool.config.ScopeId
import optimus.buildtool.files.Asset

import scala.collection.mutable

/**
 * The logical identity of an artifact *excluding* information about its current state (for example its path on local
 * disk or its content hash). For a given build, this uniquely maps to an actual Artifact.
 *
 * Think of this like an eref in the DAL (as opposed to a vref).
 */
private[buildtool] sealed trait ArtifactId {
  def fingerprint: String = toString
}

// Discriminator is additional information used to disambiguate IDs, though in general it will be `None`.
// For example: C++ osVersion or generated source template name.
private[buildtool] final case class InternalArtifactId(
    scopeId: ScopeId,
    tpe: ArtifactType,
    discriminator: Option[String]
) extends ArtifactId {
  override def toString: String = {
    val discriminatorStr = discriminator.map(d => s"($d)").getOrElse("")
    s"$scopeId:${tpe.name}$discriminatorStr"
  }
}

private[buildtool] sealed trait ExternalArtifactType {
  def name: String = getClass.getSimpleName.stripSuffix("$")
}
object ExternalArtifactType {
  private val parseMap = new mutable.HashMap[String, ExternalArtifactType]
  private def add(t: ExternalArtifactType): Unit = {
    parseMap += (t.name -> t)
  }
  def parse(name: String): ExternalArtifactType = parseMap(name)

  case object SignatureJar extends ExternalArtifactType
  add(SignatureJar)
  case object ClassJar extends ExternalArtifactType
  add(ClassJar)
  case object CppInclude extends ExternalArtifactType
  add(CppInclude)
}

private[buildtool] sealed trait ExternalArtifactId extends ArtifactId {
  def tpe: ExternalArtifactType
}

private[buildtool] final case class PathedExternalArtifactId(
    asset: Asset,
    tpe: ExternalArtifactType
) extends ExternalArtifactId {
  override def toString: String = asset.pathString
  override def fingerprint: String = asset.pathFingerprint
}

private[buildtool] final case class VersionedExternalArtifactId(
    group: String,
    name: String,
    version: String,
    artifactName: String,
    tpe: ExternalArtifactType
) extends ExternalArtifactId {
  override def toString: String = s"$group:$name:$version:$artifactName"
}

private[buildtool] final case class SingletonArtifactId(tpe: ArtifactType) extends ArtifactId {
  override def toString: String = tpe.toString.toLowerCase
}
