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
package optimus.buildtool.builders.postbuilders.installer.component

import optimus.buildtool.builders.postbuilders.installer.BatchInstallableArtifacts
import optimus.buildtool.builders.postbuilders.installer.InstallableArtifacts
import optimus.buildtool.files.FileAsset
import optimus.platform._

import scala.collection.immutable.Seq

trait ComponentInstaller {
  def descriptor: String
  @async def install(installable: InstallableArtifacts): Seq[FileAsset]
  def save(): Unit = ()
}

trait ComponentBatchInstaller {
  def descriptor: String
  @async def install(installable: BatchInstallableArtifacts): Seq[FileAsset]
}
