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
package optimus.buildtool.runconf.compile

import optimus.buildtool.runconf.compile.plugins.ExtraExecOptionsSupport

import java.lang.{Boolean => JBoolean}
import java.util.{List => JList}
import java.util.{Map => JMap}
import optimus.buildtool.runconf.compile.plugins.TreadmillOptionsSupport
import optimus.buildtool.runconf.plugins.EnvInternal
import optimus.buildtool.runconf.plugins.ExtraExecOpts
import optimus.buildtool.runconf.plugins.TreadmillOpts

import scala.jdk.CollectionConverters._
import scala.collection.immutable.Seq
import optimus.scalacompat.collection._

private[compile] class PropertyExtractor(val properties: RawProperties) extends AnyVal {
  def extractAny(key: String): Option[Any] = properties.get(key)

  def extractInt(key: String): Option[Int] = extractAny(key).map(_.asInstanceOf[java.lang.Integer].intValue)

  def extractString(key: String): Option[String] =
    extractAny(key).map(_.asInstanceOf[String])

  def extractBoolean(key: String): Option[Boolean] =
    extractAny(key).map(_.asInstanceOf[JBoolean].booleanValue)

  def extractSeq(key: String): Seq[String] =
    extractAny(key).map(_.asInstanceOf[JList[String]].asScala.toList).getOrElse(Nil)

  def extractSeqOfSeq(key: String): Seq[Seq[String]] =
    extractAny(key).map(_.asInstanceOf[JList[JList[String]]].asScala.toList.map(_.asScala.toList)).getOrElse(Nil)

  def extractMap(key: String): Map[String, Any] =
    extractAny(key).map(_.asInstanceOf[JMap[String, Any]].asScala.toMap).getOrElse(Map.empty)

  def extractEnvMap(key: String): EnvInternal =
    extractMap(key).mapValuesNow {
      case list: java.util.List[_] => Right(list.asInstanceOf[java.util.List[String]].asScala.toVector)
      case str: String             => Left(str)
    }.toMap

  def extractTmOpts(key: String): Option[TreadmillOpts] = {
    val tmOptsMap = extractMap(key)
    if (tmOptsMap.nonEmpty) {
      Some(TreadmillOptionsSupport.extract(tmOptsMap))
    } else {
      None
    }
  }

  def extractExtraExecOpts(key: String): Option[ExtraExecOpts] = {
    val extraExecOptsMap = extractMap(key)
    if (extraExecOptsMap.nonEmpty) {
      Some(ExtraExecOptionsSupport.extract(extraExecOptsMap))
    } else {
      None
    }
  }
}

object PropertyExtractor {
  /*
   * Removes double and single quotes and unescape quotes.
   * The single quote semantics cannot apply with process builder.
   * Also the ParameterExpansionParser does not honor single quotes semantics.
   */
  def unquote(input: String): String =
    if (
      input.length > 1 &&
      ((input.startsWith("\"") && input.endsWith("\"")) || (input.startsWith("'") && input.endsWith("'")))
    ) {
      input.drop(1).dropRight(1)
    } else input

  def doubleQuoteIfNecessary(input: String): String =
    if (input.exists(c => c == ' ' || c == '\t' || c == '"')) {
      // Unescape then re-escape to avoid side effects
      s""""${input.replace("\\\"", "\"").replace("\"", "\\\"")}""""
    } else input
}
