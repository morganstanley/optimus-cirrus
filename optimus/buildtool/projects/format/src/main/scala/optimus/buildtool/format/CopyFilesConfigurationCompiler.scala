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

import java.nio.file.Path

import com.typesafe.config.Config
import optimus.buildtool.config.CopyFileTask
import optimus.buildtool.config.CopyFilesConfiguration
import optimus.buildtool.config.Replacement
import optimus.buildtool.config.TokenFilter
import optimus.buildtool.files.RelativePath
import optimus.buildtool.format.ConfigUtils._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.compat._

object CopyFilesConfigurationCompiler {

  private val copyFiles = "copyFiles"

  def load(config: Config, origin: ObtFile): Result[Option[CopyFilesConfiguration]] =
    Result.tryWith(origin, config) {
      Result.optional(config.hasPath(copyFiles)) {
        Result
          .sequence {
            config
              .getConfigList(copyFiles)
              .asScala
              .map { taskConfig =>
                loadTask(taskConfig, origin)
              }
              .to(Seq)
          }
          .map(ts => CopyFilesConfiguration(ts.toSet))
      }
    }

  private def loadTask(config: Config, origin: ObtFile): Result[CopyFileTask] =
    Result
      .tryWith(origin, config) {
        for {
          from <- loadSourcePath(config, origin)
          into <- config.relativePath("into", origin)
          fileMode <- Result.optional(config.hasPath("fileMode"))(config.loadOctal(origin, "fileMode"))
          dirMode <- Result.optional(config.hasPath("dirMode"))(config.loadOctal(origin, "dirMode"))
          filters <- loadFilters(config, origin)
        } yield CopyFileTask(
          id = s"${origin.path}:${config.origin.lineNumber()}",
          from = from,
          into = into,
          skipIfMissing = config.booleanOrDefault("skipIfMissing", default = false),
          fileMode = fileMode,
          dirMode = dirMode,
          includes = config.optionalStringList("includes"),
          excludes = config.optionalStringList("excludes"),
          targetBundle = config.optionalString("targetBundle"),
          compressAs = config.optionalString("compressAs"),
          filters = filters
        )
      }
      .withProblems(config.checkExtraProperties(origin, Keys.copyFileTask))

  private def loadSourcePath(config: Config, origin: ObtFile): Result[Path] =
    Result
      .tryWith(origin, config) {
        if (config.hasPath("fromExternal")) config.directory("fromExternal", origin).map(_.path)
        else if (config.hasPath("fromResources")) {
          // adding the relative path of the obt file + "src/main/resources"
          config.relativePath("fromResources", origin).map(RelativePath("resources").resolvePath).map(_.path)
        } else {
          // adding the relative path of the obt file
          config.relativePath("from", origin).map(_.path)
        }
      }
      .withProblems(config.checkExclusiveProperties(origin, Keys.copyFileTaskFrom))

  private def loadFilters(config: Config, origin: ObtFile): Result[Seq[TokenFilter]] =
    config.stringMapOrEmpty("filters", origin).map { tokenFilters =>
      tokenFilters.to(Seq).map { case (token, text) =>
        TokenFilter(token = token, replacement = Replacement(text))
      }.sortBy(_.token)
    }
}
