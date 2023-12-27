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
import optimus.buildtool.runconf.plugins.ExtraExecOpts

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

object ExtraExecOptionsSupport {
  private val expectedTypes: Map[String, Type] = Map(
    RunConfSupport.names.extraExecOptions.preBas -> T.String,
    RunConfSupport.names.extraExecOptions.preReg -> T.String,
    RunConfSupport.names.extraExecOptions.preDiff -> T.String,
    RunConfSupport.names.extraExecOptions.postBas -> T.String,
    RunConfSupport.names.extraExecOptions.postReg -> T.String,
    RunConfSupport.names.extraExecOptions.postDiff -> T.String
  )

  private val expectedProperties: Set[String] = expectedTypes.keySet

  private type RawExtraExecOpts = JMap[String, String]

  def typeCheckExtraExecOpts(properties: RawProperties, reporter: Reporter, partialKeyPath: KeyPath): Unit = {
    extractRawExtraExecOptions(properties).foreach { values =>
      Typecheck.forProperties(
        reporter.scoped(KeyPath(partialKeyPath.components ++ Seq(RunConfSupport.names.extraExecOpts))),
        expectedTypes,
        values.asScala.toMap
      )
    }
  }

  def reportUnknownProperties(
      suiteConfig: Map[String, Any],
      conf: RunConfCompilingState,
      partialKeyPath: KeyPath
  ): Unit = {
    extractRawExtraExecOptions(suiteConfig).foreach { values =>
      UnknownProperties.report(
        conf.reportError.scoped(KeyPath(partialKeyPath.components ++ Seq(RunConfSupport.names.extraExecOpts))),
        ExtraExecOptionsSupport.expectedProperties,
        values.asScala.toMap
      )
    }
  }

  def extract(properties: RawProperties): ExtraExecOpts = {
    val extractor = new PropertyExtractor(properties)
    ExtraExecOpts(
      preBas = extractor.extractString(RunConfSupport.names.extraExecOptions.preBas),
      preReg = extractor.extractString(RunConfSupport.names.extraExecOptions.preReg),
      preDiff = extractor.extractString(RunConfSupport.names.extraExecOptions.preDiff),
      postBas = extractor.extractString(RunConfSupport.names.extraExecOptions.postBas),
      postReg = extractor.extractString(RunConfSupport.names.extraExecOptions.postReg),
      postDiff = extractor.extractString(RunConfSupport.names.extraExecOptions.postDiff)
    )
  }

  def mergeExtraExecOpts(target: Option[ExtraExecOpts], source: Option[ExtraExecOpts]): Option[ExtraExecOpts] =
    (source, target) match {
      case (Some(s), Some(t)) =>
        val merger = new Merger(t, s)
        Some(
          runconf.plugins.ExtraExecOpts(
            preBas = merger.merge(_.preBas),
            preReg = merger.merge(_.preReg),
            preDiff = merger.merge(_.preDiff),
            postBas = merger.merge(_.postBas),
            postReg = merger.merge(_.postReg),
            postDiff = merger.merge(_.postDiff)
          )
        )
      case (Some(s), None) => Some(s)
      case (None, Some(t)) => Some(t)
      case (None, None)    => None
    }

  private def extractRawExtraExecOptions(properties: RawProperties): Option[RawExtraExecOpts] = {
    properties.get(RunConfSupport.names.extraExecOpts).collect { case rawExtraExecOpts: RawExtraExecOpts @unchecked =>
      rawExtraExecOpts
    }
  }
}
