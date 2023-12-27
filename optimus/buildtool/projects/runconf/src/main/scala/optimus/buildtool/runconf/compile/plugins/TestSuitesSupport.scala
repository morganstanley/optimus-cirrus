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

import java.util.Properties
import java.util.{Map => JMap}

import optimus.buildtool.runconf
import optimus.buildtool.runconf.TestRunConf
import optimus.buildtool.runconf.compile._
import optimus.buildtool.runconf.compile.plugins.TestSuitesSupport._
import optimus.buildtool.runconf.compile.plugins.TreadmillOptionsSupport._
import optimus.buildtool.runconf.compile.plugins.ExtraExecOptionsSupport._
import optimus.buildtool.runconf.plugins.EnvInternal
import optimus.buildtool.runconf.plugins.SuiteConfig

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import optimus.scalacompat.collection._

object TestSuitesSupport {
  val expectedProperties: Set[String] = Set(names.suites)

  object names {
    val suiteProperty = "suite"
    val default = "default"
    val suites = "suites"
  }
}

class TestSuitesSupport(runEnv: RunEnv, sysProps: Properties) {
  private lazy val suite: String = {
    Some(sysProps.asScala)
      .flatMap(_.get(names.suiteProperty))
      .getOrElse(names.default)
  }

  def apply(conf: ResolvedRunConfCompilingState): Unit = {
    def mergeEnvs(target: Map[String, String], source: EnvInternal): Map[String, String] = {
      val targetInternal: EnvInternal = target.mapValuesNow(Left(_))
      envInternalToExternal(Merger.merge(targetInternal, source))
    }

    conf.runConf.suites.get(suite).foreach { suiteConfig =>
      conf.transformTemplate { template =>
        template.copy(
          includes = Merger.merge(template.includes, suiteConfig.includes),
          excludes = Merger.merge(template.excludes, suiteConfig.excludes),
          javaOpts = Merger.merge(template.javaOpts, suiteConfig.javaOpts),
          env = mergeEnvs(template.env, suiteConfig.env)
        )
      }
      conf.transformRunConf { case testRunConf: TestRunConf =>
        testRunConf.copy(
          includes = Merger.merge(testRunConf.includes, suiteConfig.includes),
          excludes = Merger.merge(testRunConf.excludes, suiteConfig.excludes),
          javaOpts = Merger.merge(testRunConf.javaOpts, suiteConfig.javaOpts),
          env = mergeEnvs(testRunConf.env, suiteConfig.env)
        )
      }
    }
  }

  private val expectedTypes: Map[String, Type] = Map(
    RunConfSupport.names.includes -> T.Array(T.String),
    RunConfSupport.names.excludes -> T.Array(T.String),
    RunConfSupport.names.javaOpts -> T.Array(T.String),
    RunConfSupport.names.env -> T.Object(T.String),
    RunConfSupport.names.treadmillOpts -> T.Object(T.Union(Set(T.String, T.Integer))),
    RunConfSupport.names.extraExecOpts -> T.Object(T.Union(Set(T.String, T.String))),
    RunConfSupport.names.category -> T.String,
    RunConfSupport.names.owner -> T.String,
    RunConfSupport.names.flags -> T.Object(T.Union(Set(T.String, T.String)))
  )

  private val expectedProperties = expectedTypes.keySet

  private type RawSuites = JMap[String, JMap[String, Any]]

  def typecheck(properties: RawProperties, reporter: Reporter): Unit = {
    Typecheck.forProperties(reporter, Map(names.suites -> T.Object(T.Object(T.Any))), properties)
    extractRawSuites(properties).foreach { value =>
      value.asScala.foreach { case (suiteName, suiteProperties) =>
        val suiteProps = suiteProperties.asScala.toMap
        val keyPath = KeyPath(Seq(names.suites, suiteName))
        Typecheck.forProperties(reporter.scoped(keyPath), expectedTypes, suiteProps)
        typeCheckTreadmillOpts(suiteProps, reporter, keyPath)
      }
    }
  }

  def reportUnknownProperties(conf: RunConfCompilingState): Unit = {
    extractRawSuites(conf.untypedProperties).foreach { suites =>
      suites.asScala.foreach { case (suiteName, suiteConfig) =>
        val suiteProps = suiteConfig.asScala.toMap
        val keyPath = KeyPath(Seq(names.suites, suiteName))
        UnknownProperties.report(
          conf.reportError.scoped(keyPath),
          expectedProperties,
          suiteProps
        )
        TreadmillOptionsSupport.reportUnknownProperties(suiteProps, conf, keyPath)
      }
    }
  }

  def extractTyped(properties: RawProperties): Map[String, SuiteConfig] = {
    extractRawSuites(properties)
      .map { rawSuites =>
        rawSuites.asScala.map { case (suiteName, suiteConfig) =>
          val extractor = new PropertyExtractor(suiteConfig.asScala.toMap)
          suiteName -> runconf.plugins.SuiteConfig(
            includes = extractor.extractSeq(RunConfSupport.names.includes),
            excludes = extractor.extractSeq(RunConfSupport.names.excludes),
            javaOpts = extractor.extractSeq(RunConfSupport.names.javaOpts),
            env = extractor.extractEnvMap(RunConfSupport.names.env),
            treadmillOpts = extractor.extractTmOpts(RunConfSupport.names.treadmillOpts),
            extraExecOpts = extractor.extractExtraExecOpts(RunConfSupport.names.extraExecOpts),
            category = extractor.extractString(RunConfSupport.names.category),
            groups = extractor.extractString(RunConfSupport.names.groups).toSet,
            owner = extractor.extractString(RunConfSupport.names.owner),
            flags = extractor.extractMap(RunConfSupport.names.flags).mapValuesNow(_.toString)
          )
        }.toMap
      }
      .getOrElse(Map.empty)
  }

  def merge(target: Map[String, SuiteConfig], source: Map[String, SuiteConfig]): Map[String, SuiteConfig] = {
    Maps.merge(target, source) { (target, source) =>
      val merger = new Merger(target, source)
      runconf.plugins.SuiteConfig(
        merger.mergeDistinct(_.includes),
        merger.mergeDistinct(_.excludes),
        merger.merge(_.javaOpts),
        merger.merge(_.env),
        mergeTreadmillOptions(target.treadmillOpts, source.treadmillOpts),
        mergeExtraExecOpts(target.extraExecOpts, source.extraExecOpts),
        merger.merge(_.category),
        merger.merge(_.groups.toIndexedSeq).toSet,
        merger.merge(_.owner),
        merger.mergeMaps(_.flags)
      )
    }
  }

  private def extractRawSuites(properties: RawProperties): Option[RawSuites] = {
    properties.get(names.suites).collect { case rawSuites: RawSuites @unchecked =>
      rawSuites
    }
  }
}
