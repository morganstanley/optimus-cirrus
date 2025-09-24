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
package optimus.stratosphere.utils

import com.typesafe.config.Config
import optimus.stratosphere.filesanddirs.PathsOpts.pathToPathsOpts

import java.nio.file.Path
import java.nio.file.Paths
import scala.jdk.CollectionConverters._
import scala.xml.Elem
import scala.xml.Node

final case class Conf(name: String, extend: Seq[String]) {
  def toObtEntry(artifacts: Seq[Artifact]): String = {
    if (name == "runtime")
      // runtime is a special case, we do not declare the path in the ivyConfigurations for it
      if (extend.isEmpty) ""
      else s"$name.extends = [${extend.sorted.mkString(", ")}]"
    else
      artifacts.find(_.conf == name) match {
        case Some(artifact) if extend.nonEmpty =>
          s"""$name {
             |  extends = [${extend.sorted.mkString(", ")}]
             |  maven.${artifact.mavenCoord.toObtEntry}
             |}""".stripMargin
        case Some(artifact)          => s"$name.maven.${artifact.mavenCoord.toObtEntry}"
        case None if extend.nonEmpty => s"$name.extends = [${extend.sorted.mkString(", ")}]"
        case None                    => ""
      }
  }
}

final case class Artifact(tpe: String, conf: String, mavenCoord: Release)

final case class Release(org: String, name: String, version: String) {
  def toObtEntry: String = {
    Seq(
      if (org.contains(".")) Text.doubleQuote(org) else org,
      if (name.contains(".")) Text.doubleQuote(name) else name,
      s"version = $version"
    ).mkString(".")
  }
}
object Release {
  def parse(from: String): Release = from.split(":") match {
    case Array(org, name, version) => Release(org, name, version)
    case _                         => throw new IllegalArgumentException(s"Cannot parse $from as version")
  }
}

final class IvyMapper private (ivyFile: Elem) {

  import IvyMapper._

  def generateIvyConfigurations: String = {
    val configurations = (ivyFile \ "configurations" \ "conf")
      .map(node => Conf(node \@ "name", (node \@ "extends").split(",").to(Seq)))
      .sortBy(_.name)

    val artifacts = (ivyFile \ "publications" \ "artifact")
      .flatMap { node =>
        val tpe = node \@ "type"
        val conf = node \@ "conf"
        (node \@@ "extra:mvn").flatMap(release =>
          if (tpe == "jar") Some(Artifact(tpe, conf, Release.parse(release))) else None)
      }

    s"""ivyConfigurations {
       |
       |${configurations.map(_.toObtEntry(artifacts)).mkString("\n\n")}
       |
       |}""".stripMargin
  }
}

object IvyMapper {
  implicit class EasyNode(node: Node) {
    def \@@(attributeName: String): Option[String] = node.attributes.head.asAttrMap.get(attributeName)
  }

  def loadFrom(path: Path): IvyMapper = new IvyMapper(XmlLoader.loadString(path.file.getContent()))

  def loadFrom(resolvers: Config, meta: String, project: String, version: String): Option[IvyMapper] = {
    (loadResolvers(resolvers, "msJava") ++ loadResolvers(resolvers, "afsRegular"))
      .map(pattern =>
        Paths.get(pattern.replace("[organisation]", meta).replace("[module]", project).replace("[revision]", version)))
      .find(ivyPath => ivyPath.file.exists())
      .map(loadFrom)
  }

  private def loadResolvers(resolvers: Config, name: String): Seq[String] =
    resolvers
      .getStringList(s"$name.ivys")
      .asScala
      .map(suffix => resolvers.getString("msJava.root") + "/" + suffix)
      .toSeq

}
