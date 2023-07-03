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
package optimus.rest.bitbucket

import scala.util.matching.Regex

object MergeCommitUtils {
  val stagingPrMessagePattern: Regex = """.*[Pp]ull request #(\d+)(?s).*from .+ to .*staging(?s).*""".r

  def prIdOf(message: String): Option[Int] = message match {
    case stagingPrMessagePattern(id) =>
      Some(id.toInt)
    case _ =>
      None
  }
}
