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
package optimus.utils

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import scala.util.Try

trait OptimusStringUtils {

  implicit class ExtraStringOps(underlying: String) {
    def emptyOrSome: Option[String] =
      if (underlying.isEmpty) None else Some(underlying)

    def getOrElse(default: => String): String =
      if (underlying.isEmpty) default else underlying

    def isNullOrEmpty: Boolean = (underlying eq null) || (underlying.length == 0)

    def abbrev(n: Int, ellipsis: String = "..."): String = {
      val s = underlying.replaceAllLiterally("\n", "\\n")
      if (s.size <= n) s else s.substring(0, n - 1) + ellipsis
    }
    def abbrev: String = abbrev(80)
  }

  object IntParsable {
    def unapply(candidate: String): Option[Int] = Try { candidate.toInt } toOption
  }

}

object OptimusStringUtils extends OptimusStringUtils {
  def detectCharset(a: Array[Byte]): Charset = {
    if (a.length < 4)
      throw new IllegalArgumentException("Cannot detect encoding with < 4 octets")
    // JSON is Unicode-encoded, so we can detect the charset by looking at
    // the first four octets (see RFC4627, Section 3)
    if (a(0) == 0) {
      if (a(1) == 0) Charset.forName("UTF-32BE")
      else StandardCharsets.UTF_16BE
    } else {
      if (a(1) != 0) StandardCharsets.UTF_8
      else {
        if (a(2) == 0) Charset.forName("UTF-32LE")
        else StandardCharsets.UTF_16LE
      }
    }
  }

  def charsetAwareToString(a: Array[Byte]): String = new String(a, detectCharset(a))
  def charsetAwareToString(a: Array[Byte], allowDefault: Boolean): String =
    if (allowDefault && a.length < 4) new String(a)
    else new String(a, detectCharset(a))

}
