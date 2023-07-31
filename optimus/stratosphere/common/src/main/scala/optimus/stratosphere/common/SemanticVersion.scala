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
package optimus.stratosphere.common

final case class SemanticVersion(major: Int, minor: Int, patch: Option[Int], suffix: Option[String] = None) {

  def isAtLeast(major: Int, minor: Int, patch: Int): Boolean =
    this.major > major ||
      (this.major == major && this.minor > minor) ||
      (this.major == major && this.minor == minor && this.patch.getOrElse(0) >= patch)

  def isAtLeast(that: SemanticVersion): Boolean = isAtLeast(that.major, that.minor, that.patch.getOrElse(0))

  override def toString: String =
    s"$major.$minor" + patch.map(s => s".$s").getOrElse("") + suffix.map(s => s".$s").getOrElse("")
}

object SemanticVersion {
  def parse(version: String): SemanticVersion = {
    version.split("""[\.-]""", 4) match {
      case Array(major, minor, patch, suffix) => SemanticVersion(major.toInt, minor.toInt, patch.toInt, suffix)
      case Array(major, minor, patch)         => SemanticVersion(major.toInt, minor.toInt, patch.toInt)
      case Array(major, minor)                => SemanticVersion(major.toInt, minor.toInt, 0)
    }
  }

  def apply(major: Int, minor: Int, patch: Int): SemanticVersion =
    apply(major, minor, Some(patch), None)

  def apply(major: Int, minor: Int, patch: Int, suffix: String): SemanticVersion =
    apply(major, minor, Some(patch), Some(suffix))
}
