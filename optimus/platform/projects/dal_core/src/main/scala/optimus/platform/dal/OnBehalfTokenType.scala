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
package optimus.platform.dal

sealed trait OnBehalfTokenType

object OnBehalfTokenType {
  // default OnBehalfSessionToken
  case object Default extends OnBehalfTokenType {
    override def toString = "Default"
  }
  case object Mock extends OnBehalfTokenType {
    override def toString: String = "Mock"
  }

  case object MatrixV1 extends OnBehalfTokenType {
    val token = Vector[Byte]('M', 'a', 't', 'r', 'i', 'x', 'V', '1')
    override def toString: String = "MatrixV1"
  }

  case object MatrixV2 extends OnBehalfTokenType {
    override def toString: String = "MatrixV2"
  }
  case object OptimusSOA extends OnBehalfTokenType {
    override def toString: String = "OptimusSOA"
  }

  case object TrustedInternal extends OnBehalfTokenType {
    val token: Vector[Byte] = "TrustedInternal".getBytes("ISO-8859-1").toVector
    override def toString: String = "TrustedInternal"
  }

  def apply(tokenType: String): OnBehalfTokenType = tokenType match {
    case "Default"         => Default
    case "OptimusSOA"      => OptimusSOA
    case "Mock"            => Mock
    case "MatrixV1"        => MatrixV1
    case "MatrixV2"        => MatrixV2
    case "TrustedInternal" => TrustedInternal
  }
}
