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
package optimus.buildtool.utils

import java.nio.file.attribute.PosixFilePermission
import java.util.Collections
import java.{util => ju}

import optimus.buildtool.config.OctalMode

import scala.jdk.CollectionConverters._

object PosixPermissionUtils {
  type Permissions = ju.Set[PosixFilePermission]
  val NoPermissions: Permissions = Collections.emptySet[PosixFilePermission]

  // we compute all the possible combinations here since it is not many of them!
  private lazy val modeToPosix: Map[OctalMode, Permissions] = {
    val allPermissions = PosixFilePermission.values()
    val allPermutations = allPermissions.toSet.subsets()
    allPermutations.map { permutation =>
      var mode = 0
      allPermissions.foreach { action =>
        mode = mode << 1
        mode += (if (permutation.contains(action)) 1 else 0)
      }
      OctalMode.fromMode(mode) -> permutation.asJava
    }.toMap
  }

  private lazy val posixToMode: Map[Permissions, OctalMode] = modeToPosix.map(_.swap)

  def fromModeString(mode: String): Permissions = fromMode(OctalMode.fromModeString(mode))
  def fromMode(mode: OctalMode): Permissions = modeToPosix(mode)
  def toMode(permissions: Permissions): OctalMode = posixToMode(permissions)
}
