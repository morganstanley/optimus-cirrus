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

import java.io.File
import java.nio.file.Path

import com.typesafe.config.ConfigUtil

final case class Problem(message: String, location: Location, level: Level) {
  override def toString: String = {
    val locationStr = location.toString
    val locationSuffix = if (locationStr.isEmpty) "" else s" ($location)"
    s"[$level] $message" + locationSuffix
  }
}

sealed abstract class Level(override val toString: String)

object Level {
  object Warn extends Level("WARN")
  object Error extends Level("ERROR")

  def fromString(s: String): Option[Level] = {
    s match {
      case "WARN"  => Some(Warn)
      case "ERROR" => Some(Error)
      case _       => None
    }
  }
}

sealed trait Location

sealed trait Global extends Location

object Unknown extends Global {
  override def toString: Block = ""
}

sealed trait AtLine extends Location {
  def line: Int // 1-based
  override def toString: String = s"at line $line"
}

sealed trait AtLocation extends AtLine {
  def filePath: Path
  def fileLocation: String = filePath.toString.replace(File.separatorChar, '/')
  override def toString: String = s"$fileLocation:$line"
}

final case class AtFile(line: Int, filePath: Path) extends AtLocation with Global

final case class AtSubstitution(line: Int, path: String) extends AtLine with Global

sealed trait AtRunConf extends AtLocation {
  def runConf: String
  override def toString: String = s"${super.toString} at run configuration $runConf"
}

final case class AtName(runConf: String, line: Int, filePath: Path) extends AtRunConf

final case class KeyPath(components: Seq[String]) extends Iterable[String] {
  override def iterator: Iterator[String] = components.iterator
  override def tail: KeyPath = KeyPath(components.tail)
  def resolve(key: String): KeyPath = KeyPath(components :+ key)
  def resolve(relPath: KeyPath): KeyPath = KeyPath(components ++ relPath.components)
  override def toString: Block = ConfigUtil.joinPath(components.toSeq: _*)
}

object KeyPath {
  val empty = KeyPath(Nil)
}

sealed trait AtProperty extends AtRunConf {
  def path: KeyPath
  override def toString: String = s"${super.toString} at property $path"
}

final case class AtKey(runConf: String, line: Int, path: KeyPath, filePath: Path) extends AtProperty {
  def unnest: AtKey = copy(path = path.tail)
  override def toString: String = s"${super.toString} at key"
}

final case class AtValue(runConf: String, line: Int, path: KeyPath, filePath: Path) extends AtProperty {
  def unnest: AtValue = copy(path = path.tail)
  override def toString: String = s"${super.toString} at value"
}

final case class AtParent(runConf: String, parent: String, line: Int, filePath: Path) extends AtProperty {
  def path = KeyPath(Seq(RunConfSupport.names.parents))
  override def toString: String = s"${super.toString} at parent $parent"
}
