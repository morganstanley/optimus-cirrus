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
package optimus.platform.util

import scala.util.parsing.combinator._

/**
 * @param keepWhiteSpace
 *   if true, keep the head and tail whitespace of unquoted fields, for the quoted field, we will keep what it is
 *   regardless of this parameter.
 */
final case class CsvParser(keepWhiteSpace: Boolean = false) {

  // backward compatibility
  def parseLine(s: String) = {
    CsvParser.parseLine(s, keepWhiteSpace)
  }

}

object CsvParser extends RegexParsers {
  // here means when Parser begin to apply regex to input string, will head and tail whitespace be overlooked
  // e.g.""" "c1,1" ," ", """. we will handle it by ourselves, so it is false.
  override def skipWhitespace = false

  def parseLine(s: String): List[String] = parseLine(s, keepWhiteSpace = false)
  def parseLine(s: String, keepWhiteSpace: Boolean): List[String] = {
    val result =
      if (keepWhiteSpace) parseAll(recordWithWhiteSpace, s)
      else parseAll(record, s)
    result match {
      case Success(res, _) => res
      case e               => throw new RuntimeException("Cannot parse to csv: " + e.toString)
    }
  }

  def whitespace: Parser[String] = """[ \t]*""".r

  def record: Parser[List[String]] = repsep(quotedFieldWithSpace | unQuotedFieldWithSpace, ",")

  def recordWithWhiteSpace: Parser[List[String]] = repsep(quotedFieldWithSpace | unQuotedFieldWithWhiteSpace, ",")

  def unQuotedFieldWithWhiteSpace: Parser[String] = simpleFields

  def unQuotedFieldWithSpace: Parser[String] = opt(whitespace) ~> compoundFields <~ opt(whitespace)

  def quotedFieldWithSpace: Parser[String] = opt(whitespace) ~> quotedField <~ opt(whitespace)

  def simpleFields: Parser[String] = """[^\n\r,"]*""".r

  def compoundFields: Parser[String] = """[^\n\r\t ,"]*[[ \t]+[^\n\r\t ,"]+]*""".r ^^ (_.replaceFirst("\\s*$", ""))

  def quotedField: Parser[String] = "\"" ~> escapedField <~ "\""

  def escapedField: Parser[String] = repsep("""[^"]*""".r, "\"\"") ^^ (_.mkString("\"")) // match and replace "" =>"
}
