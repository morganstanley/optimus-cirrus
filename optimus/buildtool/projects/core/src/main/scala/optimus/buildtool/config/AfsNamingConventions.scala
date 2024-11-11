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
package optimus.buildtool.config

import java.nio.file.Path
import java.nio.file.Paths

object AfsNamingConventions {
  val AfsRootStr: String = StaticConfig.string("afsRoot")
  val AfsRootStrWindows: String = AfsRootStr.replace('/', '\\')

  val AfsDistStr: String = s"${AfsRootStr}dist/"
  private val AfsDistPath: Path = Paths.get(AfsDistStr)
  private val AfsDistPathWindows: Path = Paths.get(s"${AfsRootStrWindows}dist\\")

  val AfsRootMapping: String = StaticConfig.string("afsRootMapping")
  val AfsRootMappingWindows: String = AfsRootMapping.replace('/', '\\')

  def isAfs(path: Path): Boolean = path.startsWith(AfsDistPath) || path.startsWith(AfsDistPathWindows)

}
