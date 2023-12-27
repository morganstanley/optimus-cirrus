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
package optimus.buildtool.runconf.compile.plugins.native

import java.util.{Map => JMap}

import optimus.buildtool.runconf
import optimus.buildtool.runconf.compile._
import optimus.buildtool.runconf.plugins.NativeLibraries
import optimus.buildtool.runconf.plugins.ReorderSpec

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq

object NativeLibrariesSupport {

  object names {
    val nativeLibraries = "nativeLibraries"
    val reorder = "reorder"
  }

  val expectedProperties: Set[String] = Set(names.nativeLibraries)

  private val expectedTypes: Map[String, Type] = Map(
    RunConfSupport.names.includes -> T.Array(T.String),
    RunConfSupport.names.excludes -> T.Array(T.String),
    names.reorder -> T.Array(T.SizedArray(T.String, 2))
  )

  private val allExpectedProperties = expectedTypes.keySet

  def typecheck(properties: RawProperties, reporter: Reporter): Unit = {
    Typecheck.forProperties(reporter, Map(names.nativeLibraries -> T.Object(T.Any)), properties)
    extractRaw(properties).foreach { value =>
      Typecheck.forProperties(reporter.scoped(keyPath), expectedTypes, value)
    }
  }

  def reportUnknownProperties(conf: RunConfCompilingState): Unit = {
    extractRaw(conf.untypedProperties).foreach { libraries =>
      UnknownProperties.report(conf.reportError.scoped(keyPath), allExpectedProperties, libraries)
    }
  }

  private def keyPath = KeyPath(Seq(names.nativeLibraries))

  def extractTyped(properties: RawProperties): NativeLibraries = {
    extractRaw(properties)
      .map { rawLibraries =>
        val extractor = new PropertyExtractor(rawLibraries)
        runconf.plugins.NativeLibraries(
          includes = extractor.extractSeq(RunConfSupport.names.includes),
          excludes = extractor.extractSeq(RunConfSupport.names.excludes),
          reorder = extractor.extractSeqOfSeq(names.reorder).collect { case Seq(a, b) => ReorderSpec(a, b) }
        )
      }
      .getOrElse(NativeLibraries())
  }

  def merge(target: NativeLibraries, source: NativeLibraries): NativeLibraries = {
    val merger = new Merger(target, source)
    NativeLibraries(
      source.includes,
      merger.mergeDistinct(_.excludes),
      merger.mergeDistinct(_.reorder),
      target.defaults ++ target.includes ++ source.defaults
    )
  }

  private def extractRaw(properties: RawProperties): Option[RawProperties] = {
    properties.get(names.nativeLibraries).collect { case rawLibraries: JMap[String, Any] @unchecked =>
      rawLibraries.asScala.toMap
    }
  }

}
