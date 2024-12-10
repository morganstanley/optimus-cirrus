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
package optimus.platform.pickling

import java.util.Arrays

import net.iharder.Base64

object ImmutableByteArray {
  def apply(data: Array[Byte]) = new ImmutableByteArray(data)
}

// This is a wrapper that gives value equality semantics to byte arrays.  It should
// be used when possible to e.g. ensure that equivalent entities pickle to equivalent pickled representations.
// Array[Byte] has reference equality semantics so using that as a pickled target always produces non-equal
// representations.
// data field needs to be exposed for e.g. serialization/pickling, but must not leak otherwise
// (else we violate immutability)
class ImmutableByteArray(private[optimus] val data: Array[Byte]) extends Serializable {
  private lazy val str = Base64.encodeBytes(data)
  override def hashCode = Arrays.hashCode(data)
  override def equals(rhs: Any) = rhs match {
    case x: ImmutableByteArray => Arrays.equals(x.data, data)
    case _                     => false
  }
  def copyOfBytes: Array[Byte] = data.clone()
  override def toString(): String = s"ImmutableByteArray($str)"
}
