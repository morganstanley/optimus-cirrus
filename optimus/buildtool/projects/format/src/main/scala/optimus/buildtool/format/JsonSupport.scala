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
package optimus.buildtool.format

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.JavaTypeable
import optimus.buildtool.files.FileAsset

import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

object JsonSupport {

  private val mapper: JsonMapper with ClassTagExtensions = {
    JsonMapper
      .builder()
      .addModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      .build() :: ClassTagExtensions
  }
  private val prettyMapper = mapper.writerWithDefaultPrettyPrinter()

  def readValue[T: JavaTypeable](inputStream: InputStream): T = mapper.readValue[T](inputStream)

  def readValue[T: JavaTypeable](path: Path): T = {
    val inputStream = Files.newInputStream(path)
    try readValue[T](inputStream)
    finally inputStream.close()
  }

  def readValue[T: JavaTypeable](file: FileAsset): T = readValue[T](file.path)

  def writeValueAsString[T: JavaTypeable](value: T): String = mapper.writeValueAsString(value)

  // TODO (OPTIMUS-47169): Eventually replace other json writers in AssetUtils and Asset with this one.
  /**
   * Return a function that writes a value as json to a path.
   *
   * To be used with [[optimus.buildtool.utils.AssetUtils.atomicallyWrite()]].
   */
  def jsonWriter[T: JavaTypeable](value: T, prettyPrint: Boolean = false): Path => Unit = (path: Path) => {
    val out = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW)
    try {
      val bytes = if (prettyPrint) prettyMapper.writeValueAsBytes(value) else mapper.writeValueAsBytes(value)
      out.write(bytes)
    } finally out.close()
  }
}
