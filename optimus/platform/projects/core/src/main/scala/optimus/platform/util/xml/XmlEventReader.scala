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
package optimus.platform.util.xml

import java.io.FileInputStream
import java.io.InputStream
import javax.xml.XMLConstants
import javax.xml.stream.XMLInputFactory
import javax.xml.stream.events.StartElement
import javax.xml.stream.events.XMLEvent
import scala.jdk.CollectionConverters._

/**
 * Usability tool for `javax.xml.stream.XMLEventReader` in Scala.
 */
object XmlEventReader {
  def apply[T](file: String)(body: BufferedIterator[XMLEvent] => T): T = apply(new FileInputStream(file))(body)

  def apply[T](input: InputStream)(body: BufferedIterator[XMLEvent] => T): T = {
    val reader = XMLInputFactory.newInstance().createXMLEventReader(input)
    val it = new BufferedIterator[XMLEvent] {
      def head: XMLEvent = reader.peek
      def hasNext: Boolean = reader.hasNext
      def next(): XMLEvent = reader.nextEvent()
    }
    val res = body(it)
    reader.close()
    res
  }
}

object StartEvent {
  def unapply(e: XMLEvent): Option[StartElem] = e match {
    case e if e.isStartElement =>
      val se = e.asStartElement()
      Some(StartElem(se.getName.getPrefix, se.getName.getLocalPart)(se))
    case _ => None
  }
}

final case class StartElem(pre: String, label: String)(e: StartElement) {
  def attributes: Map[String, String] =
    e.getAttributes.asScala.map(a => (a.getName.getLocalPart, a.getValue)).toMap
  def contextNamespaceURI: String = e.getNamespaceContext.getNamespaceURI(XMLConstants.DEFAULT_NS_PREFIX)
}

object EndEvent {
  def unapply(e: XMLEvent): Option[EndElem] = e match {
    case e if e.isEndElement =>
      val ee = e.asEndElement()
      Some(EndElem(ee.getName.getPrefix, ee.getName.getLocalPart))
    case _ => None
  }
}

final case class EndElem(pre: String, label: String)

object TextEvent {
  def unapply(e: XMLEvent): Option[String] = e match {
    case e if e.isCharacters => Some(e.asCharacters().getData)
    case _                   => None
  }
}
