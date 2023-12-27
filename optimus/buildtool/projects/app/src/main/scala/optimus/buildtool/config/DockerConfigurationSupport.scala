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

import optimus.buildtool.files.Directory
import optimus.buildtool.format.docker.DockerDefaults
import optimus.buildtool.format.docker.DockerStructure
import optimus.buildtool.format.docker.ExtraImageDefinition
import optimus.buildtool.format.docker.ImageDefinition
import optimus.buildtool.format.docker.ImageLocation
import optimus.platform._

@entity private[buildtool] trait DockerConfigurationSupport { self: ScopeConfigurationSourceBase =>

  def dockerStructure: DockerStructure

  @node def parseImages(outputDir: Directory, images: Set[String], tag: String): Set[DockerImage] = {
    val imageDefinitions = dockerStructure.images
    val imageDefs = imageDefinitions.filter(img => images.contains(img.name))
    val undefinedImages = images.diff(imageDefs.map(_.name))
    if (undefinedImages.nonEmpty) {
      val validNames = imageDefinitions.map(_.name)
      val msg =
        s"The images '${undefinedImages.mkString(", ")}' are invalid, choose from '${validNames.mkString(", ")}'"
      throw new IllegalArgumentException(msg)
    }
    imageDefs.apar.map { imageDef =>
      val scopeIds = dockerRelevantScopeIds(imageDef.scopes.apar.flatMap(resolveScopes).toSet)
      DockerImage(outputDir, tag, dockerStructure.configuration.defaults, imageDef, scopeIds)
    }
  }

  @node def dockerRelevantScopeIds(scopeIds: Set[ScopeId]): Set[ScopeId] =
    scopeIds.apar.flatMap(relevantInternalIds)

  @node private def relevantInternalIds(scopeId: ScopeId): Set[ScopeId] = {
    if (compilationScopeIds.contains(scopeId)) {
      val config = scopeConfiguration(scopeId)
      val directDeps = (config.internalCompileDependencies ++ config.internalRuntimeDependencies).toSet
      Set(scopeId) ++ directDeps.apar.flatMap(relevantInternalIds)
    } else Set.empty
  }

}

/**
 * State of a docker image, which be resolved from image definition in 'docker.obt'
 * @param location
 *   resolved location for this docker image name string, for example: optimus/dist-infra
 * @param scopeIds
 *   resolved docker relevant ScopeIds
 * @param extraImages
 *   external images that we are going to pull and include as part of our tar
 */
final case class DockerImage(
    location: ImageLocation,
    scopeIds: Set[ScopeId],
    extraImages: Set[ExtraImageDefinition],
    baseImage: Option[ImageLocation])

object DockerImage {
  def apply(
      outputDir: Directory,
      tag: String,
      defaults: DockerDefaults,
      imgDef: ImageDefinition,
      scopeIds: Set[ScopeId]): DockerImage = {
    val imageLocation =
      ImageLocation.File(outputDir.resolveFile(s"${imgDef.name}.tar").path, s"${imgDef.name}:$tag")
    val baseImage = imgDef.baseImage.orElse(defaults.baseImage).map(ImageLocation.parse(_, defaults.registry))
    DockerImage(imageLocation, scopeIds, imgDef.extraImages, baseImage)
  }
}
