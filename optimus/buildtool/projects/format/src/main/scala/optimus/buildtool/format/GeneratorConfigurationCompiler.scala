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

import java.nio.file.Paths

import ConfigUtils._
import com.typesafe.config.Config
import optimus.buildtool.config.GeneratorConfiguration
import optimus.buildtool.files.Directory
import optimus.buildtool.files.RelativePath
import optimus.buildtool.generators.GeneratorType
import spray.json._

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import scala.collection.compat._

object GeneratorConfigurationCompiler {
  def load(config: Config, origin: ObtFile): Result[Seq[(GeneratorType, GeneratorConfiguration)]] = {
    Success {
        if (config.hasPath("generators")) {
          val gens = config.getConfig("generators")
          gens.root.keySet.asScala
            .map { name =>
              val nameCfg = gens.getConfig(name)
              val tpe = nameCfg.stringOrDefault("type", name)
              val templateRoots = nameCfg.getStringList("templates").asScala.map(Paths.get(_)).to(Seq)
              val (externalRootPaths, internalRootPaths) = templateRoots.partition(_.isAbsolute)
              val internalRoots = internalRootPaths.map(RelativePath(_))
              val externalRoots = externalRootPaths.map(Directory(_))
              val cfg = if (nameCfg.hasPath("configuration")) {
                nameCfg
                  .getConfig("configuration")
                  .entrySet()
                  .asScala
                  .map { e =>
                    e.getKey -> e.getValue.unwrapped.toString
                  }
                  .toMap
              } else Map.empty[String, String]
              val files = nameCfg.stringListOrEmpty("files").map(RelativePath(_))
              val includes = nameCfg.optionalString("includes")
              val excludes = nameCfg.optionalString("excludes")
              GeneratorType(tpe) -> GeneratorConfiguration(
                name,
                internalRoots,
                externalRoots,
                files,
                includes,
                excludes,
                cfg)
            }
            .to(Seq)
            .sortBy(_._2.name)
        } else Nil
      }
      .withProblems(
        if (config.hasPath("generators")) {
          val gens = config.getConfig("generators")
          gens.root.keySet.asScala
            .flatMap { tpe =>
              gens.getConfig(tpe).checkExtraProperties(origin, Keys.generator)
            }
            .to(Seq)
        } else Nil
      )
  }

  def asJson(generators: Seq[(GeneratorType, GeneratorConfiguration)]): JsObject = {
    JsObject(generators.flatMap { case (t, gc) =>
      val typeField = if (gc.name == t.name) Nil else Seq("type" -> JsString(t.name))
      val filesField =
        if (gc.files.nonEmpty) Some("files" -> JsArray(gc.files.map(f => JsString(f.pathString)): _*)) else None
      val includesField = gc.includes.map(i => "includes" -> JsString(i))
      val excludesField = gc.excludes.map(e => "excludes" -> JsString(e))
      val templates = gc.internalTemplateFolders.map(_.pathString) ++ gc.externalTemplateFolders.map(_.pathString)
      Map(
        gc.name -> JsObject(
          typeField ++ Seq(
            "templates" -> JsArray(templates.map(t => JsString(t)): _*),
            "configuration" -> JsObject(gc.configuration.map { case (k, v) => k -> JsString(v) })
          ) ++ filesField ++ includesField ++ excludesField: _*
        )
      )
    }: _*)
  }

}
