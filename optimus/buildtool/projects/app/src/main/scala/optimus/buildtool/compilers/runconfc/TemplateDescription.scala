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

import com.samskivert.mustache.Mustache
import com.samskivert.mustache.Template
import optimus.buildtool.runconf.compile.InputFile

final case class TemplateDescription(
    name: String,
    templateInput: InputFile,
    outputFileExtension: String,
    lineFeed: String = "\n" // Default is to unix format
) {
  lazy val template: Template =
    Mustache.compiler().compile(templateInput.content.replace("\r\n", "\n").replace("\n", lineFeed))
}
