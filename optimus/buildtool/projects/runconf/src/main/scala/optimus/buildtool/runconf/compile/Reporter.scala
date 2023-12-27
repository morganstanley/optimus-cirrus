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

import java.nio.file.Path

import com.typesafe.config.Config
import optimus.buildtool.config.ParentId
import optimus.buildtool.config.WorkspaceId
import optimus.buildtool.runconf.ScopedName

import scala.collection.immutable.Seq
import scala.collection.mutable
import scala.util.Try

private[compile] class GlobalReporter {
  private val buffer = mutable.Buffer[Problem]()
  def atFile(path: Path, line: Int, message: String): Unit = buffer += Problem(message, AtFile(line, path), Level.Error)
  def warnAtFile(path: Path, line: Int, message: String): Unit =
    buffer += Problem(message, AtFile(line, path), Level.Warn)
  def problems: Seq[Problem] = buffer.distinct.toVector
}

private[compile] class Reporter private (
    block: String = "main",
    id: ParentId = WorkspaceId,
    name: String,
    buffer: mutable.Buffer[Problem],
    path: KeyPath,
    level: Level,
    config: Config,
    workspaceRoot: Option[Path],
    file: Path // relative to workspace source root
) {

  val scopedName: String = if (id == WorkspaceId) name else s"${id.properPath}.$name"

  def atName(message: String): Unit = {
    report(message, AtName(scopedName, lineAnnotation(), file))
  }

  def atKey(key: String, message: String, customKeyPath: String = ""): Unit = {
    val keyLocation = if (customKeyPath.nonEmpty) customKeyPath else key
    val line = lineAnnotation(s"$reportPath.$keyLocation")
    report(message, AtKey(name, line, path.resolve(key), file))
  }

  def atValue(key: String, message: String): Unit = {
    val line = lineAnnotation(s"$reportPath.$key")
    report(message, AtValue(scopedName, line: Int, path.resolve(key), file))
  }

  def atParent(parent: String, message: String): Unit = {
    report(message, AtParent(scopedName, parent, lineAnnotation(".parents"), file))
  }

  private def report(message: String, location: Location): Unit = {
    buffer += Problem(message, location, level)
  }

  def problems: Seq[Problem] = buffer.distinct.toVector

  def hasErrors: Boolean = buffer.exists(_.level == Level.Error)

  def toEither[R](right: R): Either[Seq[Problem], R] = if (buffer.isEmpty) Right(right) else Left(problems)

  def scoped(relPath: KeyPath): Reporter =
    new Reporter(block, id, name, buffer, path.resolve(relPath), level, config, workspaceRoot, file)

  def scoped(key: String): Reporter = scoped(KeyPath(Seq(key)))

  def withLevel(level: Level): Reporter =
    new Reporter(block, id, name, buffer, path, level, config, workspaceRoot, file)

  private def reportPath: String = if (path.isEmpty) "" else path.mkString(".", ".", "")

  private def wrapScope(scope: String): String = {
    val sanitizedScope = scope.replace("\"", "\\\"")
    if (sanitizedScope.contains(".")) s""""$sanitizedScope"""" else sanitizedScope
  }

  private def lineAnnotation(additionalPath: String = ""): Int = {
    val baseResolution = config.getConfig(block)
    Try(baseResolution.resolve.getValue(s"${wrapScope(scopedName)}$additionalPath").origin())
      .orElse(Try(baseResolution.resolve.getValue(s"${wrapScope(name)}$additionalPath").origin()))
      .getOrElse(baseResolution.origin())
      .lineNumber()
  }
}

private[compile] object Reporter {
  def apply(
      block: Block,
      scopedName: ScopedName,
      config: Config,
      file: Path,
      workspaceRoot: Option[Path],
      initial: Seq[Problem] = Nil
  ): Reporter = {
    new Reporter(
      block,
      scopedName.id,
      scopedName.name,
      mutable.Buffer[Problem](initial: _*),
      KeyPath.empty,
      Level.Error,
      config,
      workspaceRoot,
      file)
  }
}
