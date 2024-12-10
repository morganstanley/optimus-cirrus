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
/*
package msjava

import java.security.MessageDigest

import msjava.base.util.uuid.MSUuid

object MSUuidGenerator {

  private val UUID_SIZE = 16

  def fromStringV5(key: String): MSUuid = fromStringV5(key.getBytes())

  def fromStringV5(key: String, namespace: MSUuid): MSUuid = {
    val keyBytes = key.getBytes
    val namespaceBytes = namespace.asBytes()
    val namespacedBytes = new Array[Byte](keyBytes.length + namespaceBytes.length)

    System.arraycopy(namespaceBytes, 0, namespacedBytes, 0, namespaceBytes.length)
    System.arraycopy(keyBytes, 0, namespacedBytes, namespaceBytes.length, keyBytes.length)

    fromStringV5(namespacedBytes)
  }

  def fromStringV5(bytes: Array[Byte]): MSUuid = {
    val md = MessageDigest.getInstance("SHA-1")
    md.update(bytes)

    val raw: Array[Byte] = new Array[Byte](UUID_SIZE)
    System.arraycopy(md.digest(), 0, raw, 0, UUID_SIZE)

    /* set the variand and version bits */
    raw(6) = (raw(6) & 0x0f).toByte /* clear version        */
    raw(6) = (raw(6) | 0x50).toByte /* set to version 5     */
    raw(8) = (raw(8) & 0x3f).toByte /* clear variant        */
    raw(8) = (raw(8) | 0x80).toByte /* set to IETF variant  */

    new MSUuid(raw, true)
  }
}
*/
