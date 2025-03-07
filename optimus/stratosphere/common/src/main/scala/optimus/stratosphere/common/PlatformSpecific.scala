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

import optimus.stratosphere.bootstrap.OsSpecific

import java.nio.file.Path

object PlatformSpecific {

  /**
   * If file exists then canonical path on Windows, absolute path on Linux. Original path otherwise
   */
  def fullPath(path: Path): Path = {
    val absolutePath = if (OsSpecific.isWindows) path.toFile.getCanonicalFile else path.toFile.getAbsoluteFile
    /*
      Reason for this check is that absolute path may be expanded to a remote drive identifier on Linux
      and this expansion may not yield a correct local path, so subsequent operations on the returned path will throw FileNotFoundException.
      Hence, when file after expansion is non-existent, we'll fall back to the original path
     */
    if (absolutePath.exists()) absolutePath.toPath else path
  }
}
