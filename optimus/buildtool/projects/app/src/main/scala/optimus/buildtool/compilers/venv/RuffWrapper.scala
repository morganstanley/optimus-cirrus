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
package optimus.buildtool.compilers.venv
import com.github.plokhotnyuk.jsoniter_scala.core.readFromString
import optimus.buildtool.artifacts.CompilationMessage
import optimus.buildtool.artifacts.CompilationMessage._
import optimus.buildtool.artifacts.JsonImplicits.ruffOutputValueCodec
import optimus.buildtool.files.Directory
import optimus.buildtool.files.FileAsset
import optimus.buildtool.files.RelativePath
import optimus.platform._

import java.nio.file.Path
import java.nio.file.Paths
import scala.sys.process._

final case class RuffCheckOutputEndLocation(row: Int, column: Int)
final case class RuffCheckOutput(
    code: String,
    message: String,
    filename: String,
    end_location: RuffCheckOutputEndLocation) {
  def toCheckMessage(sourceDir: Directory): RuffCheckMessage =
    RuffCheckMessage(code, message, end_location, sourceDir.relativize(FileAsset(Paths.get(filename))))
}

final case class RuffCheckMessage(code: String, msg: String, location: RuffCheckOutputEndLocation, file: RelativePath) {
  def message = f"${file.toString}:${location.row}: $msg ($code)"
  def toCompilationMessage: CompilationMessage = error(message)
}

@entity object RuffWrapperFactory {
  @node def apply(venv: Path): (RuffWrapper, Seq[CompilationMessage]) = {
    val ruff = RuffWrapper(
      venv
        .resolve("bin")
        .resolve("ruff")
    )
    val existsCheck =
      if (!ruff.binary.toFile.exists())
        Seq(error(s"${ruff.binary} does not exist, ruff binary not found"))
      else Seq()

    (ruff, existsCheck)
  }
}

final case class RuffWrapper private (binary: Path) {
  @async def check(config: Path, target: Directory): Seq[RuffCheckMessage] = {
    val cmd: Seq[String] = Seq(
      f"$binary",
      "check",
      "--config",
      f"$config",
      "--output-format",
      "json",
      f"$target"
    )
    val output = new StringBuilder
    val logger = ProcessLogger(line => output.append(line).append("\n"))
    Process(cmd, cwd = target.path.toFile).!(logger)

    val ruffOutput: Seq[RuffCheckOutput] = readFromString[Seq[RuffCheckOutput]](output.toString)
    ruffOutput.map(_.toCheckMessage(target))
  }
}
