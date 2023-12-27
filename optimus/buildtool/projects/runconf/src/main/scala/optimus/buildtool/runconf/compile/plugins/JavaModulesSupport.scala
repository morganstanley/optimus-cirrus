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

import optimus.buildtool.runconf.compile.RawProperties
import optimus.buildtool.runconf.compile.Reporter
import optimus.buildtool.runconf.compile.RunEnv
import optimus.buildtool.runconf.compile.T
import optimus.buildtool.runconf.compile.Type
import optimus.buildtool.runconf.compile.Typecheck
import JavaModulesSupport.names
import optimus.buildtool.runconf.compile.KeyPath
import optimus.buildtool.runconf.compile.Merger
import optimus.buildtool.runconf.compile.PropertyExtractor
import optimus.buildtool.runconf.compile.RunConfCompilingState
import optimus.buildtool.runconf.compile.UnknownProperties
import optimus.buildtool.runconf.plugins
import optimus.buildtool.runconf.plugins.JavaModule

import scala.jdk.CollectionConverters._

object JavaModulesSupport {

  object names {
    val javaModule = "javaModule"
    val meta = "meta"
    val project = "project"
    val version = "version"
    val homeOverride = "homeOverride"
  }

  val expectedProperties: Set[String] = Set(names.javaModule)

}

class JavaModulesSupport(runEnv: RunEnv) {
  private val expectedTypes: Map[String, Type] = Map(
    names.meta -> T.String,
    names.project -> T.String,
    names.version -> T.String,
    names.homeOverride -> T.String
  )

  private val expectedProperties = expectedTypes.keySet

  def typecheck(properties: RawProperties, reporter: Reporter): Unit = {
    Typecheck.forProperties(reporter, Map(names.javaModule -> T.Object(T.Any)), properties)

    extractRaw(properties).foreach { value =>
      Typecheck.forProperties(reporter.scoped(keyPath), expectedTypes, value)
    }
  }

  private def extractRaw(properties: RawProperties): Option[RawProperties] = {
    properties.get(names.javaModule).collect { case rawJavaModules: JMap[String, Any] @unchecked =>
      rawJavaModules.asScala.toMap
    }
  }

  def reportUnknownProperties(conf: RunConfCompilingState): Unit = {
    extractRaw(conf.untypedProperties).foreach { libraries =>
      UnknownProperties.report(conf.reportError.scoped(keyPath), expectedProperties, libraries)
    }
  }

  private def keyPath = KeyPath(Seq(names.javaModule))

  def extractTyped(properties: RawProperties): JavaModule = {
    extractRaw(properties)
      .map { raw =>
        val extractor = new PropertyExtractor(raw)
        JavaModule(
          meta = extractor.extractString(JavaModulesSupport.names.meta),
          project = extractor.extractString(JavaModulesSupport.names.project),
          version = extractor.extractString(JavaModulesSupport.names.version),
          homeOverride = extractor.extractString(JavaModulesSupport.names.homeOverride)
        )
      }
      .getOrElse(JavaModule())
  }

  def merge(target: JavaModule, source: JavaModule): JavaModule = {
    val merger = new Merger(target, source)

    plugins.JavaModule(
      merger.merge(_.meta),
      merger.merge(_.project),
      merger.merge(_.version),
      merger.merge(_.homeOverride)
    )
  }
}
