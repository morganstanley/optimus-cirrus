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
package optimus.buildtool.dependencies

import optimus.buildtool.files.Directory
import optimus.buildtool.format.JdkDependenciesConfig
import optimus.buildtool.format.ObtFile
import optimus.buildtool.format.Result
import optimus.buildtool.format.Error
import optimus.buildtool.format.JvmDependenciesConfig

import java.nio.file.Paths
import scala.jdk.CollectionConverters._

final case class JdkDependencies(featureVersionToJdkHome: Map[Int, Directory])

object JdkDependencies {
  val empty: JdkDependencies = JdkDependencies(Map.empty)
  val inferred: Result[JdkDependencies] = {
    def missingError(key: String): Error = Error(s"$key is missing from system properties", JvmDependenciesConfig)
    val res = for {
      javaHome <- Result.fromOption(Option(System.getProperty("java.home")), List(missingError("java.home")))
      javaReleaseString <- Result
        .fromOption(Option(System.getProperty("java.version")), List(missingError("java.version")))
      javaReleaseVersion <- Result.fromOption(
        {
          if (javaReleaseString.startsWith("1.")) javaReleaseString.substring(2, 3).toIntOption
          else javaReleaseString.toIntOption
        },
        List(Error(s"Could not parse java.version = `$javaReleaseString`", JvmDependenciesConfig))
      )
    } yield JdkDependencies(Map(javaReleaseVersion -> Directory(Paths.get(javaHome))))
    res.withProblems(
      _.featureVersionToJdkHome.values.toList
        .filter(!_.exists)
        .map(dir => Error(s"$dir does not exist", JvmDependenciesConfig)))
  }
}

object JdkDependenciesLoader {
  def load(loader: ObtFile.Loader): Result[JdkDependencies] = {
    if (loader.exists(JdkDependenciesConfig)) {
      loader(JdkDependenciesConfig).map { config =>
        val jdkHomes = if (config.hasPath("jdks")) {
          val jdks = config.getObject("jdks")
          val jdkVersions = jdks.unwrapped().keySet()
          jdkVersions.asScala.map(v => v.toInt -> Directory(Paths.get(jdks.toConfig.getString(s"$v.home")))).toMap
        } else Map.empty[Int, Directory]
        JdkDependencies(jdkHomes)
      }
    } else {
      JdkDependencies.inferred
    }
  }
}
