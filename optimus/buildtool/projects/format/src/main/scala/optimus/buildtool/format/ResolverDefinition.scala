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

import java.util.{List => JList}

import com.typesafe.config.Config
import com.typesafe.config.ConfigObject
import optimus.buildtool.format.ConfigUtils._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.compat._

final case class ResolverDefinition(
    name: String,
    ivyPatterns: List[String],
    artifactPatterns: List[String],
    line: Int) {
  def ivyPatternsJava: JList[String] = ivyPatterns.asJava
  def artifactPatternsJava: JList[String] = artifactPatterns.asJava
}

object ResolverDefinition {
  private def parseResolver(config: Config): Result[ResolverDefinition] = Result.tryWith(ResolverConfig, config) {
    def applyResolverRoot(list: java.util.List[String]): Seq[String] =
      if (config.hasPath("root")) {
        val root = config.getString("root")
        list.asScala.to(Seq).map(path => s"$root/$path")
      } else list.asScala.to(Seq)

    val ivyPatterns = applyResolverRoot(config.getStringList("ivys")).toList
    val artifactPattern = applyResolverRoot(config.getStringList("artifacts")).toList
    val line = config.root().origin().lineNumber()
    Success(
      ResolverDefinition(config.getString("name"), ivyPatterns, artifactPattern, line),
      config.checkExtraProperties(ResolverConfig, Keys.resolversFile)
    )
  }

  private def parseResolvers(config: Config): Result[Seq[ResolverDefinition]] = Result.tryWith(ResolverConfig, config) {
    Result
      .traverse(config.getList("resolvers").asScala.to(Seq)) { cfgValue =>
        parseResolver(cfgValue.asInstanceOf[ConfigObject].toConfig)
      }
      .map(_.to(Seq).filter(res => res.artifactPatterns.nonEmpty && res.ivyPatterns.nonEmpty))
  }

  def load(loader: ObtFile.Loader): Result[Seq[ResolverDefinition]] =
    ResolverConfig.tryWith {
      val resolvers = for {
        config <- loader(ResolverConfig)
        resolver <- parseResolvers(config)
      } yield resolver

      resolvers.withProblems { results =>
        results
          .groupBy(_.name)
          .filter(_._2.size > 1)
          .flatMap { case (name, configs) =>
            val msg = s"Resolver name must be unique, '$name' is used at ${configs.map(_.line)}"
            configs.map(conf => Error(msg, ResolverConfig, conf.line))
          }
          .to(Seq)
      }
    }
}
