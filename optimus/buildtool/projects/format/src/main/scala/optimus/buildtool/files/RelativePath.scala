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
package optimus.buildtool.files

import optimus.buildtool.utils.OptimusBuildToolAssertions
import optimus.buildtool.utils.PathUtils

import java.nio.file.Path
import java.nio.file.Paths

final case class RelativePath(path: Path) extends Pathed with PathedOps {
  OptimusBuildToolAssertions.require(!PathUtils.isAbsolute(path), s"Invalid relative path: $path")

  override def toString: String = pathString

  // Note: We resolve based on `pathString` here, since we don't want to throw if p.path
  // is on a different filesystem from `path`
  def resolvePath(other: RelativePath): RelativePath = resolvePath(other.pathString)
  def resolvePath(other: String): RelativePath = RelativePath(path.resolve(other))

  // Path has odd behaviour here - the parent of "a.txt" is null, not the empty path. We want to modify
  // this behaviour so for example you can call `RelativePath("a.txt").parent.resolvePath("b.txt")`
  def parent: RelativePath = {
    if (this == RelativePath.empty) throw new IllegalArgumentException(s"No parent for $this")
    val parent = path.getParent
    if (parent == null) RelativePath.empty
    else RelativePath(path.getParent)
  }

  def parentOption: Option[RelativePath] = {
    if (this == RelativePath.empty) None
    val parent = path.getParent
    if (parent == null) Some(RelativePath.empty)
    else Some(RelativePath(path.getParent))
  }

  def subPath(start: Int): Option[RelativePath] = subPath(start, path.getNameCount)

  def subPath(start: Int, end: Int): Option[RelativePath] =
    try Some(RelativePath(path.subpath(start, end)))
    catch {
      case _: IllegalArgumentException => None
    }

  def first(i: Int): Option[RelativePath] = subPath(0, i)

  def relativize(child: RelativePath): RelativePath =
    if (this == RelativePath.empty) child else RelativePath(Pathed.relativize(path, child.path))
}

object RelativePath {
  val empty = RelativePath(Paths.get(""))

  def apply(s: String): RelativePath = RelativePath(Paths.get(s))

  implicit val ordering: Ordering[RelativePath] = Ordering.by(_.pathString)
}
