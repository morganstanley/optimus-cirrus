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
package optimus.utils

import java.math.BigInteger
import java.security.MessageDigest

final case class Sha1Hash(value: String)

object HashUtil {
  def sha1Hash(str: String): Sha1Hash = sha1Hash(str.getBytes())
  def sha1Hash(bytes: Array[Byte]): Sha1Hash = {
    val digest = MessageDigest.getInstance("SHA-1").digest(bytes)
    Sha1Hash(new BigInteger(1, digest).toString(16))
  }
}
