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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import optimus.buildtool.files.FileAsset
import optimus.buildtool.utils.AssetUtils
import org.slf4j.LoggerFactory.getLogger

import java.io.File
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.collection.compat._
import scala.collection.immutable.Seq
import scala.collection.mutable.Buffer

// Functions related to reading and writing the mapping files (current-mapping.json, classpath-mapping.txt) that maps from the OBT produced jars to the scope IDs.

object OutputMappings {
  private val log = getLogger(this.getClass)
  private val separator = File.pathSeparator

  def readClasspathPlain(path: Path): Try[Map[String, Seq[String]]] = readClasspathPlain(FileAsset(path))

  def readClasspathPlain(file: FileAsset): Try[Map[String, Seq[String]]] = Try {
    val lines =
      try {
        Files
          .readAllLines(file.path)
          .asScala
      } catch {
        // this is the only exception that it makes sense to catch here, as we call this function when we update the
        // (potentially inexistent) classpath mapping file. Other issues (invalid path, permissions etc) are bubbled up.
        case e: NoSuchFileException =>
          log.debug(s"No such file ${file.pathString}")
          Buffer.empty
      }
    lines
      .flatMap(line => {
        line.split("\t") match {
          case Array(a, b) => Some(a -> b.split(separator).to(Seq))
          case _ =>
            log.warn(s"Invalid mapping entry: '$line'")
            None
        }
      })
      .toMap
  }

  def updateClasspathPlain(file: FileAsset, updatedMappings: Map[String, Seq[String]]): Unit = {
    val previousMappings: Map[String, Seq[String]] = readClasspathPlain(file)
      .getOrElse {
        log.warn("Could not read previous plain mapping file")
        Map.empty
      }
    val newMapping: Map[String, Seq[String]] = (previousMappings ++ updatedMappings)
    val content = newMapping
      .map { case (k, vs) =>
        s"$k\t${vs.mkString(separator)}"
      }
      .mkString("\n")
      .getBytes(StandardCharsets.UTF_8)

    Files.createDirectories(file.parent.path)
    AssetUtils.atomicallyWrite(file, replaceIfExists = true) { p =>
      Files.write(p, content)
    }
  }
}
