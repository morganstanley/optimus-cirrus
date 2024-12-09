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
package optimus.buildtool.oci

import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.util.concurrent._

import scala.annotation.tailrec

object SymLinkSearch {
  private val analysedPath: ConcurrentHashMap[Path, Option[String]] = new ConcurrentHashMap()
  private val granularityInDays: List[Int] =
    List(1, 2, 7, 10, 20, 30, 365, 2 * 365, 3 * 365, 4 * 365, 5 * 365).sorted
}

trait SymLinkSearch {
  import SymLinkSearch._

  protected val cache: ConcurrentHashMap[Path, Option[String]] = analysedPath
  protected def getLastModifiedTime(path: Path): Instant =
    Files.getLastModifiedTime(path).toInstant
  protected def isSymLink(p: Path): Boolean = Files.isSymbolicLink(p)

  @tailrec
  private def touchesSymLink(p: Path): Boolean =
    p.getParent != null && (isSymLink(p) || touchesSymLink(p.getParent))

  private def timePeriod(p: Path): String = {
    val timeSinceLastModInDays = Duration.between(getLastModifiedTime(p), Instant.now()).toDays.toInt
    val choice = granularityInDays.find(_ >= timeSinceLastModInDays)
    choice match {
      case Some(n) => if (n >= 365) (n / 365).toString + "y" else n.toString + "d"
      case None    => "other"
    }
  }

  def pickSymLinkLayer(p: Path): Option[String] =
    cache.computeIfAbsent(p, { path => if (touchesSymLink(path)) Some(s"<symLink-${timePeriod(path)}>") else None })

}
