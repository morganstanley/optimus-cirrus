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
import optimus.buildtool.runconf.plugins.JacocoOpts

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

object JacocoOptionsSupport {
  object ValidationCriteria extends Enumeration {
    type ValidationCriteria = Value
    val minCoveragePct = Value("Must be between 0 and 100")
  }

  private val expectedTypes: Map[String, Type] = Map(
    RunConfSupport.names.jacocoOptions.minCoveragePct -> T.Integer,
    RunConfSupport.names.jacocoOptions.includes -> T.Array(T.String),
    RunConfSupport.names.jacocoOptions.excludes -> T.Array(T.String)
  )

  private val expectedProperties: Set[String] = expectedTypes.keySet

  private val validationMethods: Seq[RawProperties => ValidationResult] = Seq(
    validateJacocoMinCoveragePct
  )

  private type RawJacocoOpts = JMap[String, String]

  def typecheck(properties: RawProperties, reporter: Reporter): Unit = {
    Typecheck.forProperties(reporter, Map(RunConfSupport.names.jacocoOpts -> T.Object(T.Any)), properties)
    extractRaw(properties).foreach { value =>
      Typecheck.forProperties(reporter.scoped(KeyPath(Seq(RunConfSupport.names.jacocoOpts))), expectedTypes, value)
    }
  }

  def reportUnknownProperties(conf: RunConfCompilingState): Unit = {
    extractRaw(conf.untypedProperties).foreach { values =>
      UnknownProperties.report(
        conf.reportError.scoped(KeyPath(Seq(RunConfSupport.names.jacocoOpts))),
        expectedProperties,
        values)
    }
  }

  def reportInvalidProperties(conf: RunConfCompilingState): Unit = {
    extractRaw(conf.untypedProperties) match {
      case Some(props) =>
        validationMethods.foreach { method =>
          InvalidProperties.report(
            conf.reportError.scoped(KeyPath(Seq(RunConfSupport.names.jacocoOpts))),
            props,
            method
          )
        }
      case None =>
    }
  }

  def mergeJacocoOpts(target: Option[JacocoOpts], source: Option[JacocoOpts]): Option[JacocoOpts] =
    (source, target) match {
      case (Some(s), Some(t)) =>
        val merger = new Merger(t, s)
        Some(
          runconf.plugins.JacocoOpts(
            minCoveragePct = merger.merge(_.minCoveragePct),
            includes = merger.mergeDistinct(_.includes),
            excludes = merger.mergeDistinct(_.excludes)
          )
        )
      case (Some(s), None) => Some(s)
      case (None, Some(t)) => Some(t)
      case (None, None)    => None
    }

  def extract(properties: RawProperties): JacocoOpts = {
    val extractor = new PropertyExtractor(properties)
    val extractedMinCoveragePctValue: Option[Int] =
      extractor.extractInt(RunConfSupport.names.jacocoOptions.minCoveragePct) match {
        case a: Some[Int] => a
        case None         => None
      }
    def extractArray(runconfName: String): Seq[String] = extractor.extractSeq(runconfName)
    JacocoOpts(
      minCoveragePct = extractedMinCoveragePctValue,
      includes = extractArray(RunConfSupport.names.jacocoOptions.includes),
      excludes = extractArray(RunConfSupport.names.jacocoOptions.excludes),
    )
  }

  private def extractRaw(properties: RawProperties): Option[RawProperties] = {
    properties.get(RunConfSupport.names.jacocoOpts).collect { case rawLibraries: JMap[String, Any] @unchecked =>
      rawLibraries.asScala.toMap
    }
  }

  private def extractRawJacocoOptions(properties: RawProperties): Option[RawJacocoOpts] = {
    properties.get(RunConfSupport.names.jacocoOpts).collect { case rawJacocoOpts: RawJacocoOpts @unchecked =>
      rawJacocoOpts
    }
  }

  private def validateJacocoMinCoveragePct(properties: RawProperties): ValidationResult = {
    extract(properties).minCoveragePct match {
      case Some(min) if 0 to 100 contains min =>
        ValidationResult(true)
      case Some(min) if !(0 to 100 contains min) =>
        ValidationResult(
          false,
          Some(RunConfSupport.names.jacocoOptions.minCoveragePct),
          Some(ValidationCriteria.minCoveragePct.toString),
          Some(min.toString))
      case None =>
        ValidationResult(
          false,
          Some(RunConfSupport.names.jacocoOptions.minCoveragePct),
          Some(ValidationCriteria.minCoveragePct.toString),
          None
        )
    }
  }

}
