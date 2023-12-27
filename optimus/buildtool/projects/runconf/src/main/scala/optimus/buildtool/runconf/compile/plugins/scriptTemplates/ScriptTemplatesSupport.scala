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
package optimus.buildtool.runconf.compile.plugins.scriptTemplates

import java.util.{Map => JMap}

import optimus.buildtool.runconf
import optimus.buildtool.runconf.compile._
import optimus.buildtool.runconf.compile.plugins.scriptTemplates.ScriptTemplatesSupport._
import optimus.buildtool.runconf.plugins.ScriptTemplates

import scala.jdk.CollectionConverters._
import optimus.scalacompat.collection._

object ScriptTemplatesSupport {

  object names {
    val scriptTemplates = "scriptTemplates"
    val templates = "templates"
    val customVariables = "customVariables"
    val baseDir = "baseDir"
  }

  val expectedProperties: Set[String] = Set(names.scriptTemplates)

}

class ScriptTemplatesSupport(runEnv: RunEnv) {

  private val expectedTypes: Map[String, Type] = Map(
    names.templates -> T.Object(T.String),
    names.customVariables -> T.Object(T.String),
    names.baseDir -> T.String
  )

  val expectedProperties: Set[String] = expectedTypes.keySet

  private type RawScriptTemplates = JMap[String, Any]

  def typecheck(properties: RawProperties, reporter: Reporter): Unit = {
    Typecheck.forProperties(reporter, Map(names.scriptTemplates -> T.Object(T.Any)), properties)
    extractRawScripts(properties).foreach { value =>
      Typecheck.forProperties(reporter.scoped(KeyPath(Seq(names.scriptTemplates))), expectedTypes, value)
    }
  }

  def reportUnknownProperties(conf: RunConfCompilingState): Unit = {
    extractRawScripts(conf.untypedProperties).foreach { scripts =>
      UnknownProperties.report(
        conf.reportError.scoped(KeyPath(Seq(names.scriptTemplates))),
        expectedProperties,
        scripts
      )
    }
  }

  def extractTyped(properties: RawProperties): ScriptTemplates = {
    extractRawScripts(properties)
      .map { rawTemplates =>
        val extractor = new PropertyExtractor(rawTemplates)
        ScriptTemplates(
          extractor.extractMap(names.templates).mapValuesNow(_.toString),
          extractor.extractMap(names.customVariables).mapValuesNow(_.toString),
          extractor.extractString(names.baseDir)
        )
      }
      .getOrElse(ScriptTemplates())
  }

  def merge(target: ScriptTemplates, source: ScriptTemplates): ScriptTemplates = {
    val merger = new Merger(target, source)

    def mergeMaps(get: ScriptTemplates => Map[String, String]): Map[String, String] =
      Maps.merge(get(target), get(source)) { case (_, source) =>
        source
      }

    runconf.plugins.ScriptTemplates(
      mergeMaps(_.templates),
      mergeMaps(_.customVariables),
      merger.merge(_.baseDir)
    )
  }

  private def extractRawScripts(properties: RawProperties): Option[RawProperties] = {
    properties.get(names.scriptTemplates).collect { case rawScripts: JMap[String, Any] @unchecked =>
      rawScripts.asScala.toMap
    }
  }
}
