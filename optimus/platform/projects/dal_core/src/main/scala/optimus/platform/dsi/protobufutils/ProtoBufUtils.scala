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
package optimus.platform.dsi.protobufutils

import java.util.concurrent.atomic.AtomicInteger

import com.google.protobuf.ByteString
import com.google.protobuf.CodedInputStream
import optimus.graph.DiagnosticSettings
import optimus.platform.internal.SimpleStateHolder

class ProtoBufUtils {
  import ProtoBufUtils._
  val recursionLimitValue: AtomicInteger = new AtomicInteger(defaultProtobufRecursionLimit)
}

object ProtoBufUtils extends SimpleStateHolder(() => new ProtoBufUtils) {
  val protobufRecursionLimitProp = "optimus.platform.dsi.protobufutils.protobufRecursionLimit"
  val defaultProtobufRecursionLimit: Int = DiagnosticSettings.getIntProperty(protobufRecursionLimitProp, 128)
  def getProtoBufRecursionLimit: Int = getState.recursionLimitValue.get()
  def setProtoBufRecursionLimit(value: Int): Unit = getState.recursionLimitValue.set(value)
  def resetProtoBufRecursionLimit(): Unit = getState.recursionLimitValue.set(defaultProtobufRecursionLimit)

  def getCodedInputStream(payload: ByteString): CodedInputStream = {
    val input = payload.newCodedInput()
    input.setRecursionLimit(getProtoBufRecursionLimit)
    input
  }

  def getCodedInputStream(payload: Array[Byte]): CodedInputStream = {
    val input = CodedInputStream.newInstance(payload)
    input.setRecursionLimit(getProtoBufRecursionLimit)
    input
  }
}
