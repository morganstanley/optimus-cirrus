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
package optimus.buildtool.format

import ConfigUtils._
import com.typesafe.config.Config

final case class MavenDefinition(
    useMavenLibs: Boolean,
    includeConditionals: Map[String, String],
    excludeConditionals: Map[String, String]) {
  def isEmpty: Boolean = includeConditionals.isEmpty && excludeConditionals.isEmpty
}

object MavenDefinition {
  private val ReleaseKey = "release-"

  def empty: MavenDefinition = MavenDefinition(useMavenLibs = false, Map.empty, Map.empty)

  def loadMavenDefinition(depCfg: Config, isMavenCfg: Boolean, useMavenLibs: Boolean): Result[MavenDefinition] = if (
    isMavenCfg
  ) {
    def loadConditionals(key: String): Result[Map[String, String]] =
      if (!depCfg.hasPath(key)) Success(Map.empty)
      else {
        val keyConfig = depCfg.getConfig(key).resolve()
        Result
          .tryWith(MavenDependenciesConfig, keyConfig) {
            Success(
              keyConfig.keySet.flatten { tpe =>
                val tpeConfig = keyConfig.getConfig(tpe)
                val tpeStr =
                  if (tpe.contains(ReleaseKey) && useMavenLibs) tpe.replace(ReleaseKey, "")
                  else tpe // "release-*" would be ignored for non-maven-release build(useMavenLibs != true)
                tpeConfig.stringListOrEmpty("conditionals").map(_ -> tpeStr)
              }.toMap
            )
          }
          .withProblems(keyConfig.checkExtraProperties(MavenDependenciesConfig, Keys.mavenDefinition))
      }

    for {
      includesOld <- loadConditionals("mavenIncludes")
      includes <- loadConditionals("mavenIncludes")
      excludesOld <- loadConditionals("mavenExcludes")
      excludes <- loadConditionals("mavenExcludes")
    } yield MavenDefinition(useMavenLibs, includes ++ includesOld, excludes ++ excludesOld)
  } else Success(MavenDefinition.empty)
}
