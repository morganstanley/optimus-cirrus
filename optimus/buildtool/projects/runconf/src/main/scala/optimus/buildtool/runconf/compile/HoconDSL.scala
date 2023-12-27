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

import com.typesafe.config.ConfigUtil

object HoconDSL {
  private val maxArrayLength = 80

  class ObjBuilder(key: String) {
    def apply(entries: Seq[String]): String = apply(entries.mkString("\n"))

    def apply(content: String): String = multilineBlock(key, content, "{", "}")
  }

  def obj(name: String): ObjBuilder = new ObjBuilder(name)

  def entry(property: String, value: String): String = {
    s"$property = $value"
  }

  def entry(property: String, value: Int): String = {
    entry(property, value.toString)
  }

  def objEntry(property: String, value: String): String = {
    s"$property $value"
  }

  def arrayAppend(property: String, value: String): String = {
    s"$property += $value"
  }

  def string(value: String): String = {
    quote(value)
  }

  def smartArray(property: String, elements: Seq[String]): String = {
    elements match {
      case Seq(single) => arrayAppend(property, single)
      case _ =>
        val length = elements.map(_.length).sum
        if (length <= maxArrayLength) {
          entry(property, elements.mkString("[", ", ", "]"))
        } else {
          val content = (elements.init.map(_ + ",") :+ elements.last).mkString("\n")
          multilineBlock(property, content, "= [", "]")
        }
    }
  }

  private def inlineBlock(content: String): String = s"{ $content }"

  private def quote(s: String): String = {
    def wrap(quotes: String) = quotes + s + quotes

    val doubleQuote = '"'.toString
    if (s.contains("\\")) wrap(doubleQuote * 3) else wrap(doubleQuote)
  }

  private def multilineBlock(key: String, content: String, open: String, close: String) = {
    val escapedKey = ConfigUtil.joinPath(key)
    if (content.trim.isEmpty) {
      s"$escapedKey $open$close"
    } else {
      s"""$escapedKey $open
         |${indent(content)}
         |$close""".stripMargin
    }
  }

  private def indent(s: String): String = {
    s.linesIterator
      .map { line =>
        if (line.isEmpty) line else "  " + line
      }
      .mkString("\n")
  }

}
