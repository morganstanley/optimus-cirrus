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
package optimus.platform.debugger

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.ClassTagExtensions

object StackElemType extends Enumeration {
  type StackElemType = Value
  val node, jvm, ec = Value

  /**
   * IDE plugin's version can be older than the version of codetree that is currently under debugging. It is then
   * possible to produce new StackElem that have a type which is not in our enum values. We use this "unknown" type to
   * cover those cases.
   *
   * This requires a custom deserializer.
   */
  val unknown = Value

  class StackElemTypeDeser extends StdDeserializer[StackElemType](classOf[StackElemType]) {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): StackElemType = {
      try { p.readValueAs(classOf[StackElemType]) }
      catch { case _: NoSuchElementException => unknown }
    }
  }
}

final case class StackElemData(
    className: String,
    methodRawName: String,
    methodDisplayName: String,
    lineNum: Int,
    @JsonDeserialize(using = classOf[StackElemType.StackElemTypeDeser])
    elemType: StackElemType.StackElemType
)

object StackElemData {
  private val mapper = new ObjectMapper() with ClassTagExtensions
  mapper.registerModule(DefaultScalaModule)

  def write(elem: StackElemData): String =
    mapper.writeValueAsString(elem)

  def read(s: String): StackElemData =
    mapper.readValue(s, classOf[StackElemData])
}
