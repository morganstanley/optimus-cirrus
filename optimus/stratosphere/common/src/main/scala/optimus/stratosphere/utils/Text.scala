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
package optimus.stratosphere.utils

object Text {
  def quote(str: String): String = "'" + str + "'"

  def doubleQuote(str: String): String = "\"" + str + "\""

  def bind(str: String, replacements: Map[String, String]): String =
    replacements.foldLeft(str) { case (str, (key, value)) => str.replace(key, value) }

  def pluralize(i: Int, noun: String): String =
    if (i == 1) noun else s"${noun}s"
}
