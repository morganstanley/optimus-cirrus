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
package optimus.platform.inputs.loaders

import optimus.platform.inputs.loaders.OptimusNodeInputStorage.OptimusStorageUnderlying
import optimus.platform.inputs.registry.JvmInputs
import org.apache.commons.io.FileUtils

object OptimusLoaders {
  private[platform] def defaults: OptimusStorageUnderlying = {
    var storage = OptimusNodeInputStorage.loadDefaults
    storage = runtime(storage)
    storage.underlying()
  }

  private def runtime(
      storage: NodeInputStorage[OptimusStorageUnderlying]): NodeInputStorage[OptimusStorageUnderlying] = {
    val strXmx = FileUtils
      .byteCountToDisplaySize(Runtime.getRuntime.maxMemory())
      .replaceAll("\\s+", "") // remove whitespaces
      .replace("B", "")
      .replace("bytes", "")
    storage.appendLocalInput(JvmInputs.Xmx, strXmx, false, LoaderSource.CODE)
  }
}
