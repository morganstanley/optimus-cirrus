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
package optimus.dal.silverking.base

import com.ms.silverking.cloud.dht.client.serialization.internal.ArrayMD5KeyCreator
import com.ms.silverking.cloud.dht.common.DHTKey
import com.ms.silverking.cloud.dht.common.KeyUtil
import optimus.platform.dsi.prc.cache.NonTemporalPrcKey

object DHTKeyUtils {
  private val keyCreator = new ArrayMD5KeyCreator()

  def createKey(bytes: Array[Byte]): DHTKey = {
    keyCreator.createKey(bytes)
  }

  def stringifyKey(key: DHTKey): String = {
    KeyUtil.keyToString(key)
  }

  def parseKey(keyStr: String): DHTKey = {
    KeyUtil.keyStringToKey(keyStr)
  }

  def createKey(k: NonTemporalPrcKey): DHTKey = {
    keyCreator.createKey(k.getBackendKeyField.rawArray)
  }
}
