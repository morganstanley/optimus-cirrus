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
package optimus.platform.util.json
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.ScalaObjectMapper

import java.io.StringWriter

object JsonSerializer {

  def toJson(m: Map[String, Any]): String = {
    val mapper = new ObjectMapper() with ScalaObjectMapper

    val prettyPrinter = new ScalaParserCombinatorsCompatiblePrettyPrinter
    prettyPrinter.indentObjectsWith(DefaultPrettyPrinter.NopIndenter.instance)
    prettyPrinter.indentArraysWith(DefaultPrettyPrinter.FixedSpaceIndenter.instance)
    mapper.registerModule(DefaultScalaModule)

    val writer = new StringWriter()
    mapper.writer(prettyPrinter).writeValue(writer, m)
    writer.toString
  }
}
