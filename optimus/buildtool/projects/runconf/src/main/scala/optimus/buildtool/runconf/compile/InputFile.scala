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

import optimus.buildtool.config.ParentId
import optimus.buildtool.config.WorkspaceId

import scala.jdk.CollectionConverters._

final case class InputFile(
    scope: ParentId,
    content: String,
    isLocal: Boolean = false,
    origin: Path // relative to workspace source root
) {
  val fileName: String =
    origin.getFileName.toString // Early evaluation as 'origin' could be tied to a closed FileSystem

  def append(text: String): InputFile = {
    copy(content = content + "\n" + text)
  }

  def rewrite(transform: String => String): InputFile = {
    copy(content = transform(content))
  }
}

object InputFile {
  private val nonIdDirectories = Set("projects", "appscripts")
  // TODO (OPTIMUS-43576): Use workspace structure instead of path mangling
  // Note: runConfFile is relative to workspace source root
  def idForPath(runConfFile: Path): ParentId = {
    val runConfDir = runConfFile.getParent
    if (runConfDir == null) { // runConfFile is a top-level file
      WorkspaceId
    } else {
      val directoryComponents = runConfDir.asScala.map(_.toString).toList
      val elements = directoryComponents.filterNot(nonIdDirectories.contains)
      ParentId.parse(elements)
    }
  }
}
