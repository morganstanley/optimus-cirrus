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
package optimus.buildtool.format.docker

import java.nio.file.Path
import java.nio.file.Paths
import com.typesafe.config.Config
import optimus.buildtool.config.PartialScopeId
import optimus.buildtool.config.ScopeId
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.Keys
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.Success

import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.immutable.Set

final case class ImageDefinition(
    name: String,
    scopes: Seq[ScopeId],
    extraImages: Set[ExtraImageDefinition],
    baseImage: Option[String],
    imageSysName: Option[String])
final case class ExtraImageDefinition(name: String, pathsToIncluded: Seq[Path])

object ImageDefinition {
  val Empty: ImageDefinition = ImageDefinition("", Seq.empty, Set.empty, None, None)

  def load(origin: ObtFile, config: Config, key: String, validScopes: Set[ScopeId]): Result[Set[ImageDefinition]] =
    if (config.hasPath(key)) {
      val updatedConfig = config.getConfig(key)
      Result.tryWith(origin, updatedConfig) {
        Result
          .sequence {
            updatedConfig.keySet.to(Seq).map { key => loadImageDef(origin, updatedConfig, key, validScopes) }
          }
          .map(_.toSet)
      }
    } else Success(Set.empty)

  private def loadImageDef(
      origin: ObtFile,
      config: Config,
      key: String,
      validScopes: Set[ScopeId]): Result[ImageDefinition] =
    Result
      .tryWith(origin, config) {
        val imgConfig = config.getConfig(key)
        scopeIds(origin, imgConfig, validScopes).flatMap { scopes =>
          val extraImages = ExtraImageDefinition.load(origin, imgConfig, "extraImages")
          val baseImage = imgConfig.optionalString("baseImage")
          val imageSysName = imgConfig.optionalString("imageSysName")
          extraImages
            .map { ei =>
              ImageDefinition(key, scopes, ei, baseImage, imageSysName)
            }
            .withProblems(imgConfig.checkExtraProperties(origin, Keys.imageDefinition))
        }
      }

  private def scopeIds(origin: ObtFile, imgConfig: Config, validScopes: Set[ScopeId]): Result[Seq[ScopeId]] = {
    val scopes = imgConfig.stringConfigListOrEmpty("scopes")
    val scopeIds = scopes.map { case (s, cfg) =>
      val partialId = PartialScopeId.parse(s)
      val ids = validScopes.filter(partialId.contains)
      val problems =
        if (ids.nonEmpty) Nil
        else Seq(origin.errorAt(cfg, s"No matching scopes for '$s''"))
      Success(ids, problems)
    }
    Result.sequence(scopeIds).map(_.flatten)
  }
}

object ExtraImageDefinition {
  val Empty: ExtraImageDefinition = ExtraImageDefinition("", Seq.empty)

  def load(origin: ObtFile, config: Config, key: String): Result[Set[ExtraImageDefinition]] =
    if (config.hasPath(key)) {
      val srcImagesConf = config.getConfig(key)
      Result.tryWith(origin, srcImagesConf) {
        Result
          .sequence {
            srcImagesConf.keySet.to(Seq).map { imgName =>
              // s""""$imgName"""" is to add quotes that match docker.obt extraImages format: "imgName" = []
              val includes = srcImagesConf.stringListOrEmpty(s""""$imgName"""").map(str => Paths.get(str))
              Success(ExtraImageDefinition(imgName, includes))
            }
          }
          .map(_.toSet)
      }
    } else Success(Set.empty)
}
