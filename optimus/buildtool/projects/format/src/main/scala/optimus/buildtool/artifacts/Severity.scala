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
package optimus.buildtool.artifacts

sealed trait Severity {
  override def toString: String = getClass.getSimpleName.stripSuffix("$").toUpperCase
}
object Severity {
  val values = Seq(Error, Warning, Info)

  def safelyParse(s: String): Option[Severity] = s.toUpperCase match {
    case "ERROR"                 => Some(Error)
    case "WARNING"               => Some(Warning)
    case "WARN"                  => Some(Warning) // compatibility with older serializations
    case "INFO"                  => Some(Info)
    case w if w.contains("WARN") => Some(Warning)
    case _                       => None
  }

  def parse(s: String): Severity = safelyParse(s).getOrElse(Severity.Error)

  case object Error extends Severity
  case object Warning extends Severity
  case object Info extends Severity
}
