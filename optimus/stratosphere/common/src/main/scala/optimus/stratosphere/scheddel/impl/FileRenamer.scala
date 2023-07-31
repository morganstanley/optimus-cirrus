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
package optimus.stratosphere.scheddel.impl

import java.nio.file.Files
import java.nio.file.Path

import optimus.stratosphere.filesanddirs.PathsOpts._
import optimus.stratosphere.utils.DateTimeUtils._

import scala.util.Try

final case class RenameResult(path: Path, renamed: Try[Path])

object FileRenamer {

  def rename(srcPath: Path): Option[RenameResult] = {
    if (!srcPath.exists()) None
    else {
      val renamed = Try(srcPath.rename(getNewName(srcPath)))
      Some(RenameResult(srcPath, renamed))
    }
  }

  private def getNewName(srcPath: Path): String = {

    val dstFileName = s"${srcPath.getFileName}-${nextDateTimeSuffix()}"
    val dstPath = srcPath.resolveSibling(dstFileName)
    if (Files.exists(dstPath)) {
      throw ScheduledDeletionException(s"Could not rename: '$srcPath', to '$dstPath'. Path already exists")
    } else {
      dstPath.getFileName.toString
    }
  }

}
