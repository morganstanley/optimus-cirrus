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
package optimus.stratosphere.utils

import java.io.Writer
import scala.annotation.nowarn
import scala.xml.Elem
import scala.xml.MinimizeMode
import scala.xml.Node
import scala.xml.XML
import scala.xml.dtd.DocType
import scala.xml.factory.XMLLoader
import scala.xml.parsing.FactoryAdapter
import scala.xml.parsing.NoBindingFactoryAdapter

// same as optimus.platform.util.xml.XmlLoader
object XmlLoader extends XMLLoader[Elem] {
  override def adapter: FactoryAdapter = new NoBindingFactoryAdapter() {
    override def comment(ch: Array[Char], start: Int, length: Int): Unit = ()
  }

  @nowarn("msg=10500 scala.xml.XML")
  val encoding: String = XML.encoding

  @nowarn("msg=10500 scala.xml.XML")
  def save(
      filename: String,
      node: Node,
      enc: String = "UTF-8",
      xmlDecl: Boolean = false,
      doctype: DocType = null
  ): Unit = XML.save(filename, node, enc, xmlDecl, doctype)

  @nowarn("msg=10500 scala.xml.XML")
  def write(
      w: Writer,
      node: Node,
      enc: String,
      xmlDecl: Boolean,
      doctype: DocType,
      minimizeTags: MinimizeMode.Value = MinimizeMode.Default
  ): Unit = XML.write(w, node, enc, xmlDecl, doctype, minimizeTags)
}
