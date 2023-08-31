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
package optimus.platform.obt.openapi

import optimus.platform.OptimusAppCmdLine
import org.kohsuke.args4j

class OpenApiSpecBuilderCmdLine extends OptimusAppCmdLine {
  @args4j.Option(
    name = "-t",
    aliases = Array("--target"),
    usage = "target app to fetch the spec for, in FQN format",
    required = true)
  val target: String = ""

  @args4j.Option(
    name = "-o",
    aliases = Array("--outDir", "--outputFile"),
    usage = "location of the file to output the spec to",
    required = true)
  val outputFile: String = ""

  @args4j.Option(name = "-api", usage = "api endpoint to call to fetch spec. defaults to /v3/api-docs")
  val api: String = "v3/api-docs"

  override final val env = "none"

}
