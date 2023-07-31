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
package optimus.stratosphere.filesanddirs

import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Paths

object ResourceUtils {

  def fromResourceOrFile(path: String): InputStream = {
    val is = getClass.getResourceAsStream(path)
    if (is != null) is
    else if (Files.exists(Paths.get(path))) new FileInputStream(path)
    else throw new RuntimeException(s"Neither resource nor file found at: $path")
  }
}
