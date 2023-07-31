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

import scala.util.Try

trait OptimusStringUtils {

  implicit class ExtraStringOps(underlying: String) {
    def emptyOrSome: Option[String] =
      if (underlying.isEmpty) None else Some(underlying)

    def isNullOrEmpty: Boolean = (underlying eq null) || (underlying.length == 0)

    def abbrev(n: Int, ellipsis: String = "..."): String = if(underlying.size <= n) underlying else underlying.substring(0, n-1) + ellipsis
    def abbrev: String = abbrev(80)
  }

  object IntParsable {
    def unapply(candidate: String): Option[Int] = Try { candidate.toInt } toOption
  }

}
