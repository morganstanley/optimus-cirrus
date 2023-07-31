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

import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties

object PropertiesUtils {

  def fromIS(inputStream: InputStream): Properties = {
    val props = new Properties()
    try props.load(inputStream)
    finally inputStream.close()
    props
  }

  def fromFile(fileName: String): Properties = fromIS(new FileInputStream(fileName))

  def fromFile(file: File): Properties = fromPath(file.toPath)

  def fromPath(path: Path) = fromIS(Files.newInputStream(path))

  implicit class PropOpts(properties: Properties) {
    def store(dest: Path, comment: String = ""): Unit = {
      Files.createDirectories(dest.getParent)
      val writer = Files.newBufferedWriter(dest)
      try properties.store(writer, comment)
      finally writer.close()
    }
  }
}

final case class Property(name: String, value: String)
