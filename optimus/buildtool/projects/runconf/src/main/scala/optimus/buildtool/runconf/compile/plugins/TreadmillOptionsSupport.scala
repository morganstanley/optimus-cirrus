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

import optimus.buildtool.runconf
import optimus.buildtool.runconf.compile._
import optimus.buildtool.runconf.plugins.TreadmillOpts

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

object TreadmillOptionsSupport {
  private val expectedTypes: Map[String, Type] = Map(
    RunConfSupport.names.treadmillOptions.mem -> T.String,
    RunConfSupport.names.treadmillOptions.app -> T.String,
    RunConfSupport.names.treadmillOptions.cell -> T.String,
    RunConfSupport.names.treadmillOptions.cpu -> T.String,
    RunConfSupport.names.treadmillOptions.disk -> T.String,
    RunConfSupport.names.treadmillOptions.kuu -> T.String,
    RunConfSupport.names.treadmillOptions.manifest -> T.String,
    RunConfSupport.names.treadmillOptions.priority -> T.Integer
  )

  private val expectedProperties: Set[String] = expectedTypes.keySet

  private type RawTreadmillOpts = JMap[String, String]

  def typeCheckTreadmillOpts(properties: RawProperties, reporter: Reporter, partialKeyPath: KeyPath): Unit = {
    extractRawTreadmillOptions(properties).foreach { values =>
      Typecheck.forProperties(
        reporter.scoped(KeyPath(partialKeyPath.components ++ Seq(RunConfSupport.names.treadmillOpts))),
        expectedTypes,
        values.asScala.toMap
      )
    }
  }

  def reportUnknownProperties(
      suiteConfig: Map[String, Any],
      conf: RunConfCompilingState,
      partialKeyPath: KeyPath): Unit = {
    extractRawTreadmillOptions(suiteConfig).foreach { values =>
      UnknownProperties.report(
        conf.reportError.scoped(KeyPath(partialKeyPath.components ++ Seq(RunConfSupport.names.treadmillOpts))),
        TreadmillOptionsSupport.expectedProperties,
        values.asScala.toMap
      )
    }
  }

  def extract(properties: RawProperties): TreadmillOpts = {
    val extractor = new PropertyExtractor(properties)
    TreadmillOpts(
      mem = extractor.extractString(RunConfSupport.names.treadmillOptions.mem),
      disk = extractor.extractString(RunConfSupport.names.treadmillOptions.disk),
      cpu = extractor.extractString(RunConfSupport.names.treadmillOptions.cpu),
      manifest = extractor.extractString(RunConfSupport.names.treadmillOptions.manifest),
      cell = extractor.extractString(RunConfSupport.names.treadmillOptions.cell),
      app = extractor.extractString(RunConfSupport.names.treadmillOptions.app),
      kuu = extractor.extractString(RunConfSupport.names.treadmillOptions.kuu),
      priority = extractor.extractInt(RunConfSupport.names.treadmillOptions.priority)
    )
  }

  def mergeTreadmillOptions(target: Option[TreadmillOpts], source: Option[TreadmillOpts]): Option[TreadmillOpts] =
    (source, target) match {
      case (Some(s), Some(t)) =>
        val merger = new Merger(t, s)
        Some(
          runconf.plugins.TreadmillOpts(
            mem = merger.merge(_.mem),
            disk = merger.merge(_.disk),
            cpu = merger.merge(_.cpu),
            manifest = merger.merge(_.manifest),
            cell = merger.merge(_.cell),
            app = merger.merge(_.app),
            kuu = merger.merge(_.kuu),
            priority = merger.merge(_.priority)
          ))
      case (Some(s), None) => Some(s)
      case (None, Some(t)) => Some(t)
      case (None, None)    => None
    }

  private def extractRawTreadmillOptions(properties: RawProperties): Option[RawTreadmillOpts] = {
    properties.get(RunConfSupport.names.treadmillOpts).collect { case rawTreadmillOpts: RawTreadmillOpts @unchecked =>
      rawTreadmillOpts
    }
  }
}
