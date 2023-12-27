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
package optimus.buildtool.runconf

import java.io.File

final case class NativePaths(jniPath: String, preloadPath: String)

object NativePaths {
  def stringify(paths: Seq[FilePath]): String = {
    paths.map(_.replace("/", File.separator)).mkString(File.pathSeparator)
  }

  // repeats the logic that was there before to ensure that anything that
  // depends on .jnipath files behaves in the same way
  def stringifyBackwardCompatible(paths: Seq[FilePath]): String = {
    "\"" + stringify(paths) + "\""
  }
}
