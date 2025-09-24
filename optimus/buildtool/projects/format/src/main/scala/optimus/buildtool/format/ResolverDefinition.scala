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

import com.typesafe.config.Config
import com.typesafe.config.ConfigObject
import optimus.buildtool.format.ConfigUtils._
import optimus.buildtool.format.ResolverDefinition.ResolverType

import scala.jdk.CollectionConverters._
import scala.collection.compat._
import scala.util.control.NonFatal

final case class ResolverDefinition(
    name: String,
    tpe: ResolverType,
    metadataPatterns: List[String],
    artifactPatterns: List[String],
    line: Int)

object ResolverDefinition {
  sealed trait ResolverType
  object ResolverType {
    case object Maven extends ResolverType
    case object Ivy extends ResolverType
  }

  private[buildtool] val Artifacts = "artifacts"
  private[buildtool] val Name = "name"
  private[buildtool] val Poms = "poms"
  private[buildtool] val Ivys = "ivys"
  private[buildtool] val Root = "root"
  private[buildtool] val IvyResolvers = "ivy-resolvers"
  private[buildtool] val MavenResolvers = "maven-resolvers"

  private[buildtool] val noMetadataMsg = s"no $Ivys or $Poms pattern predefined!"
  private[buildtool] val noArtifactsMsg = s"no $Artifacts pattern predefined!"

  private def parseResolver(config: Config, metadataTpe: ResolverType): Result[ResolverDefinition] =
    Result.tryWith(ResolverConfig, config) {
      def applyResolverRoot(list: java.util.List[String]): Seq[String] =
        if (config.hasPath(Root)) {
          val root = config.getString(Root)
          list.asScala.to(Seq).map(path => s"$root/$path")
        } else list.asScala.to(Seq)

      def loadListWithRoot(key: String): List[String] =
        if (config.hasPath(key)) applyResolverRoot(config.getStringList(key)).toList else List.empty

      def configWarning(msg: String, condition: Boolean, line: Int): Option[Warning] =
        if (condition) Some(Warning(msg, ResolverConfig, line)) else None

      val metadataPatterns = {
        val loadedIvy = loadListWithRoot(Ivys)
        if (loadedIvy.isEmpty) loadListWithRoot(Poms) else loadedIvy
      }
      val artifactPattern = loadListWithRoot(Artifacts)
      val line = config.root().origin().lineNumber()

      Success(
        ResolverDefinition(config.getString(Name), metadataTpe, metadataPatterns, artifactPattern, line),
        config.checkExtraProperties(ResolverConfig, Keys.resolversFile)
      )
    }

  private def parseResolvers(config: Config): Result[ResolverDefinitions] = Result.tryWith(ResolverConfig, config) {

    def loadResolversList(key: String, allResolvers: Seq[ResolverDefinition]): Seq[ResolverDefinition] = {
      val nameList: Seq[String] =
        if (config.hasPath(key))
          config.getList(key).asScala.map(_.asInstanceOf[ConfigObject].toConfig.getString("name")).toIndexedSeq
        else Nil

      nameList.flatMap(n => allResolvers.find(_.name == n))
    }

    def loadAllPredefinedResolvers: Result[Seq[ResolverDefinition]] =
      Result
        .traverse(
          config.keySet
            .to(Seq)
            .filter { k =>
              try {
                config.getConfig(k).hasPath("name")
              } catch {
                case NonFatal(e) => false
              }
            }
        ) { onDemandRepoName =>
          val repoConfig = config.getConfig(onDemandRepoName)
          parseResolver(repoConfig, if (repoConfig.hasPath(Ivys)) ResolverType.Ivy else ResolverType.Maven)
        }
        .map(_.to(Seq).filter(res => res.artifactPatterns.nonEmpty && res.metadataPatterns.nonEmpty))
        .withProblems { results =>
          results
            .groupBy(_.name)
            .filter(_._2.size > 1)
            .flatMap { case (name, configs) =>
              val msg = s"Resolver name must be unique, '$name' is used at ${configs.map(_.line)}"
              configs.map(conf => Error(msg, ResolverConfig, conf.line))
            }
            .to(Seq)
        }

    for {
      allResolvers <- loadAllPredefinedResolvers
      ivyResolvers = loadResolversList(IvyResolvers, allResolvers)
      mavenResolvers = loadResolversList(MavenResolvers, allResolvers)
    } yield ResolverDefinitions(ivyResolvers, mavenResolvers, allResolvers.diff(ivyResolvers ++ mavenResolvers))
  }

  def load(loader: ObtFile.Loader): Result[ResolverDefinitions] =
    ResolverConfig.tryWith {
      for {
        config <- loader(ResolverConfig)
        resolver <- parseResolvers(config)
      } yield resolver
    }
}

final case class ResolverDefinitions(
    defaultIvyResolvers: Seq[ResolverDefinition],
    defaultMavenResolvers: Seq[ResolverDefinition],
    onDemandResolvers: Seq[ResolverDefinition]
) { def allResolvers: Seq[ResolverDefinition] = defaultIvyResolvers ++ defaultMavenResolvers ++ onDemandResolvers }

object ResolverDefinitions {
  val empty: ResolverDefinitions = ResolverDefinitions(Nil, Nil, Nil)
}
