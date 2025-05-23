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
package optimus.buildtool.compilers.runconfc

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import optimus.buildtool.config.{NamingConventions, ScopeId}
import optimus.buildtool.files.{FileAsset, RelativePath}
import optimus.buildtool.runconf.{AppRunConf, RunConfType}
import optimus.buildtool.utils.ConsistentlyHashedJarOutputStream

import scala.io.Source

final case class RunConfInventoryEntry(
    scopeId: ScopeId,
    name: String,
    tpe: RunConfType,
    relativeSourcePath: Option[String]) {
  import optimus.buildtool.compilers.runconfc.RunConfInventoryEntry.sourceQualifier

  // For RT-ness, we do not include the source file. The source file is only to be used for error messages.
  def serialized: String =
    (Seq(scopeId, name, tpe) ++ relativeSourcePath.map(path => s"$sourceQualifier$path")).mkString("/")
  override def toString: String = serialized
  def isApp: Boolean = tpe == AppRunConf
}

object RunConfInventoryEntry {
  private val sourceQualifier = "sourceFile="

  // backward-compatibility for inventory files written by older OBT versions
  private def upgradeName(possiblyScopedName: String): String =
    possiblyScopedName.split('.').last

  def parseFrom(str: String): Option[RunConfInventoryEntry] =
    PartialFunction.condOpt(str.split("/", 4)) {
      case Array(scopeId, possiblyScopedName, RunConfType(tpe), relativeSourcePath) if !scopeId.startsWith("#") =>
        val name = upgradeName(possiblyScopedName)
        RunConfInventoryEntry(ScopeId.parse(scopeId), name, tpe, Some(relativeSourcePath.stripPrefix(sourceQualifier)))

      case Array(scopeId, possiblyScopedName, RunConfType(tpe)) if !scopeId.startsWith("#") =>
        val name = upgradeName(possiblyScopedName)
        RunConfInventoryEntry(ScopeId.parse(scopeId), name, tpe, None)
    }
}

object RunConfInventory {
  def fromFile(inventoryFile: FileAsset): Seq[RunConfInventoryEntry] =
    if (inventoryFile.exists) {
      val in = Files.newInputStream(inventoryFile.path)
      try {
        Source
          .fromInputStream(in, StandardCharsets.UTF_8.name())
          .getLines()
          .flatMap(RunConfInventoryEntry.parseFrom)
          .toList // Realize before leaving closure
      } finally in.close()
    } else List.empty

  def writeFile(stream: ConsistentlyHashedJarOutputStream, inventory: Seq[RunConfInventoryEntry]): Unit =
    stream.writeFile(
      NamingConventions.runConfInventoryHeader + "\n" +
        inventory
          .map(_.serialized)
          .sorted // Make RT-ish
          .mkString("\n"),
      RelativePath(NamingConventions.runConfInventory)
    )
}
