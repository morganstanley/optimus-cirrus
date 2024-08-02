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
package optimus.buildtool.runconf.compile.plugins

import java.util.{Map => JMap}

import optimus.buildtool.runconf.compile._
import optimus.buildtool.runconf.compile.plugins.StrictRuntimeSupport._
import optimus.buildtool.runconf.plugins.StrictRuntime

import scala.jdk.CollectionConverters._

object StrictRuntimeSupport {
  object names {
    val strictRuntime = "strictRuntime"
    val enabled = "enabled"
    val envNames = "envNames"
    val sysPropNames = "sysPropNames"
    val javaOptNames = "javaOptNames"
  }

  val expectedProperties: Set[String] = Set(names.strictRuntime)
}

class StrictRuntimeSupport() {
  private def expectedTypes: Map[String, Type] = Map(
    names.enabled -> T.Boolean,
    names.envNames -> T.Array(T.String),
    names.sysPropNames -> T.Array(T.String),
    names.javaOptNames -> T.Array(T.String),
  )

  val expectedProperties: Set[String] = expectedTypes.keySet

  def typecheck(properties: RawProperties, reporter: Reporter): Unit = {
    Typecheck.forProperties(reporter, Map(names.strictRuntime -> T.Object(T.Any)), properties)
    extractRawStrictRuntime(properties).foreach { value =>
      Typecheck.forProperties(reporter.scoped(KeyPath(Seq(names.strictRuntime))), expectedTypes, value)
    }
  }

  def reportUnknownProperties(conf: RunConfCompilingState): Unit = {
    extractRawStrictRuntime(conf.untypedProperties).foreach { scripts =>
      UnknownProperties.report(
        conf.reportError.scoped(KeyPath(Seq(names.strictRuntime))),
        expectedProperties,
        scripts
      )
    }
  }

  def extractTyped(properties: RawProperties): StrictRuntime = {
    extractRawStrictRuntime(properties)
      .map { rawTemplates =>
        val extractor = new PropertyExtractor(rawTemplates)
        StrictRuntime(
          extractor.extractBoolean(names.enabled),
          extractor.extractSeq(names.envNames),
          extractor.extractSeq(names.sysPropNames),
          extractor.extractSeq(names.javaOptNames),
        )
      }
      .getOrElse(StrictRuntime())
  }

  def merge(target: StrictRuntime, source: StrictRuntime): StrictRuntime = {
    val merger = new Merger(target, source)
    StrictRuntime(
      merger.merge(_.enabled),
      merger.mergeDistinct(_.envNames),
      merger.mergeDistinct(_.sysPropNames),
      merger.mergeDistinct(_.javaOptNames)
    )
  }

  private def extractRawStrictRuntime(properties: RawProperties): Option[RawProperties] = {
    properties.get(names.strictRuntime).collect { case rawScripts: JMap[String, Any] @unchecked =>
      rawScripts.asScala.toMap
    }
  }
}
