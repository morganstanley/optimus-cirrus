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
package optimus.config

import optimus.config.spray.json.{JsObject, JsString, JsValue, JsonParser, ParserInput}
import optimus.config.spray.json.JsonParser.ParsingException

import scala.collection.mutable

object OptimusExtensionType extends Enumeration {
  val Reference: OptimusExtensionType.Value = Value
}

object OptimusConfigParser {
  val extensionTypeKey = "optimus$extensionType"
  val sectionKey = "section"
  val keyKey = "key"
  val origin = "origin"

  def parse(input: ParserInput): JsValue = new OptimusConfigParser(input).parseJsValue()
  def cleanConfig(confContent: String): String = if (confContent.contains("//"))
    confContent.split(Array('\r', '\n')).map(_.split("//")(0)).mkString("\n")
  else confContent

  val failOnDotHint = "Shared Config or Cache names that can be referenced elsewhere should not contain '.'"
}

final class OptimusConfigParser(input: ParserInput) extends JsonParser(input) {
  import OptimusConfigParser._

  protected override def valueExtension(): JsValue = {
    require('$')
    require('{')
    ws()

    val sb = new StringBuilder
    var dotCount = 0
    while (cursorChar != '}') {
      if (cursorChar == '.') {
        dotCount += 1
        if (dotCount > 1)
          fail(s"$cursorChar", hint = failOnDotHint)
      }
      sb.append(cursorChar)
      advance()
    }
    val fullPath = sb.toString
    val res = if (fullPath.length > 0) {
      if (dotCount == 0)
        fail(s"$cursorChar")

      // [SEE_DOT_PARSING] TODO (OPTIMUS-46190): parser uses dots to reference nested config sections (arguably not needed)
      val paths = sb.toString.trim.split("\\.")
      val section = paths.head
      val key = paths.tail.head
      val origin = {
        s"pos: ${input.cursor}"
      }

      val map = Map(
        OptimusConfigParser.extensionTypeKey -> JsString(OptimusExtensionType.Reference.toString),
        OptimusConfigParser.sectionKey -> JsString(section),
        OptimusConfigParser.keyKey -> JsString(key),
        OptimusConfigParser.origin -> JsString(origin)
      )
      JsObject(map)
    } else {
      fail(s"$cursorChar")
    }
    require('}')
    ws()
    res
  }

  private[this] var currentPath: List[String] = Nil
  override protected def enteringField(key: String): Unit = currentPath ::= key
  override protected def exitingField(): Unit = currentPath = currentPath.tail

  protected override def `object`(): Unit = {
    if (currentPath != "Classes" :: "Cache" :: Nil) super.`object`()
    else {
      // special handling for the Classes section: preserve the ordering and duplicate keys
      ws()
      jsValue = if (cursorChar != '}') {
        val entries = mutable.ListBuffer[(String, JsValue)]()
        do {
          val key = keyValue()
          entries += key -> jsValue
        } while (ws(','))

        require('}')
        JsOrderedObject(entries.result())
      } else {
        advance()
        JsOrderedObject.empty
      }
      ws()
    }
  }

  // left for debugging only, - this method is expensive.
  def currentPosition: ParserInput.Line = {
    input.getLine(input.cursor)
  }
}

class OptimusConfigParsingException(override val summary: String) extends ParsingException(summary)
final class ConstructorConfigParsingException
    extends OptimusConfigParsingException(
      "Can't set anything other than DontCache policy on a constructor node through optconf!")

/** like JsObject but preserves ordering and allows duplicated keys */
final case class JsOrderedObject(fields: Seq[(String, JsValue)]) extends JsValue
object JsOrderedObject {
  val empty = JsOrderedObject(Nil)
}
