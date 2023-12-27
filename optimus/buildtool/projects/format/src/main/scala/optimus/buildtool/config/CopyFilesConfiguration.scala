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
package optimus.buildtool.config

import java.nio.file.Path

import optimus.buildtool.files.RelativePath

import scala.collection.immutable.Seq

final case class CopyFilesConfiguration(tasks: Set[CopyFileTask])

object CopyFilesConfiguration {
  def merge(
      configA: Option[CopyFilesConfiguration],
      configB: Option[CopyFilesConfiguration]): Option[CopyFilesConfiguration] =
    (configA, configB) match {
      case (Some(a), Some(b)) => Some(CopyFilesConfiguration(a.tasks ++ b.tasks))
      case _                  => configA.orElse(configB)
    }
}

final case class CopyFileTask(
    id: String,
    from: Path,
    into: RelativePath,
    skipIfMissing: Boolean,
    fileMode: Option[OctalMode],
    dirMode: Option[OctalMode],
    includes: Option[Seq[String]],
    excludes: Option[Seq[String]],
    targetBundle: Option[String],
    compressAs: Option[String],
    filters: Seq[TokenFilter]
)

final case class TokenFilter(token: String, replacement: Replacement)

sealed trait Replacement
object Replacement {

  final case object InstallVersion extends Replacement
  final case class FreeText(text: String) extends Replacement

  def apply(text: String): Replacement = text match {
    // TODO (OPTIMUS-35296): the install version should really come from the OBT file!
    case "obt.installVersion" => InstallVersion
    case text                 => FreeText(text)
  }
}
