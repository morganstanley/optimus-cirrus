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

final case class TreadmillOpts(
    mem: Option[String] = None,
    disk: Option[String] = None,
    cpu: Option[String] = None,
    manifest: Option[String] = None,
    cell: Option[String] = None,
    app: Option[String] = None,
    kuu: Option[String] = None,
    priority: Option[Int] = None
)
