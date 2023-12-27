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
package optimus.buildtool.runconf.plugins

final case class ScriptTemplates(
    // 'default' template will use OBT templates; if something else is provided, it has to be found in the
    // workspace and accessible from disk.
    // 'disable' to prevent template generation.
    templates: Map[String, String] = Map.empty,
    customVariables: Map[String, String] = Map.empty,
    baseDir: Option[String] = None
)
