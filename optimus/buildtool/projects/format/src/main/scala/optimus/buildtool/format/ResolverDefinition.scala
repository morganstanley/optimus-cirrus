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
import scala.collection.immutable.Seq
import scala.collection.compat._

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
    case object Other extends ResolverType
  }

  private[buildtool] val Artifacts = "artifacts"
  private[buildtool] val Name = "name"
  private[buildtool] val Poms = "poms"
  private[buildtool] val Ivys = "ivys"
  private[buildtool] val Root = "root"
  private[buildtool] val IvyResolvers = "ivy-resolvers"
  private[buildtool] val MavenResolvers = "maven-resolvers"
  private[buildtool] val MavenUnzipResolvers = "maven-unzip-resolvers"
  private[buildtool] val OtherResolvers = "other-resolvers"

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

      // allow user put variables in artifacts and metadata, should be warnings
      val warnings =
        Seq(
          configWarning(noArtifactsMsg, artifactPattern.isEmpty, line),
          configWarning(noMetadataMsg, metadataPatterns.isEmpty, line)
        ).flatten

      Success(
        ResolverDefinition(config.getString(Name), metadataTpe, metadataPatterns, artifactPattern, line),
        config.checkExtraProperties(ResolverConfig, Keys.resolversFile)
      ).withProblems(warnings)
    }

  private def parseResolvers(config: Config): Result[Seq[ResolverDefinition]] = Result.tryWith(ResolverConfig, config) {

    def loadResolversList(key: String, tpe: ResolverType): Result[Seq[ResolverDefinition]] =
      Result
        .traverse(if (config.hasPath(key)) config.getList(key).asScala.to(Seq) else Nil) { cfgValue =>
          parseResolver(cfgValue.asInstanceOf[ConfigObject].toConfig, tpe)
        }
        .map(_.to(Seq).filter(res => res.artifactPatterns.nonEmpty && res.metadataPatterns.nonEmpty))

    for {
      ivyResolvers <- loadResolversList(IvyResolvers, ResolverType.Ivy)
      mavenResolvers <- loadResolversList(MavenResolvers, ResolverType.Maven)
      // TODO (OPTIMUS-58907): remove MavenUnzipResolvers after next obt release
      mavenUnzipResolvers <- loadResolversList(MavenUnzipResolvers, ResolverType.Other)
      otherResolvers <- loadResolversList(OtherResolvers, ResolverType.Other)
    } yield ivyResolvers ++ mavenResolvers ++ mavenUnzipResolvers ++ otherResolvers
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
