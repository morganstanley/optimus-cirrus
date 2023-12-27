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

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.FileAttributeView

/**
 * Gratuitous sugar for select file attributes.
 */
final class FileAttrs(file: Path) {
  def asView[V <: FileAttributeView: reflect.ClassTag]: Option[V] =
    Option(Files.getFileAttributeView(file, reflect.classTag[V].runtimeClass.asInstanceOf[Class[V]]))
}

object FileAttrs {
  implicit def toFileAttrs(file: Path): FileAttrs = new FileAttrs(file)
}
