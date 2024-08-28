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
package optimus.graph.audit

import java.util.IdentityHashMap
import java.io.{Writer, StringWriter, FileWriter}
import scala.collection.immutable.Map
import scala.xml.{Elem, MetaData, TopScope}
import optimus.graph.NodeExtendedInfo

class AuditTraceNamed(val name: String, val children: Map[AnyRef, AuditTraceNamed] = Map.empty[AnyRef, AuditTraceNamed])
    extends NodeExtendedInfo {

  private val INITIAL_DOT_SIZE = 1024

  override def +(child: NodeExtendedInfo): NodeExtendedInfo = child match {
    case n: AuditTraceNamed => new AuditTraceMergedChildren("trace", Map((name -> this), (n.name -> n)))
  }

  protected def toXmlAttributes: MetaData = scala.xml.Null

  def writeAsDot(fileName: String): Unit = {
    val out = new FileWriter(fileName)
    toDotAll(out)
  }

  def toDot: String = {
    val out = new StringWriter(INITIAL_DOT_SIZE)
    toDotAll(out)
    out.toString()
  }

  protected def toDotAll(out: Writer): Unit = {
    out.write("digraph G {\n")
    toDot(out, new IdentityHashMap[AuditTraceNamed, String])
    out.write("}")
    out.close()
  }

  protected def toDotAttributes: String = "label=\"" + name + "\""

  protected def toDot(out: Writer, ids: IdentityHashMap[AuditTraceNamed, String]): String = {

    def id(at: AuditTraceNamed) = {
      if (ids.containsKey(at)) ids.get(at)
      else { val name = "n" + ids.size(); ids.put(at, name); name }
    }

    val cid = id(this)
    out.write(cid)
    out.write("[")
    out.write(toDotAttributes)
    out.write("];\n")

    val childIds = children.values.map(_.toDot(out, ids))
    out.write(cid)
    out.write(" -> ")
    out.write(childIds.mkString("{", ";", "};\n"))

    cid
  }

  def toXml: Elem = {
    val childXml = children.map(_._2.toXml).toSeq
    Elem(null, name, toXmlAttributes, TopScope, true, childXml: _*)
  }

  override def toString() = toXml.toString
}

/**
 * All classes here are for experimenting only!!!!!!!!!
 */
class AuditTraceMergedChildren(name: String, override val children: Map[AnyRef, AuditTraceNamed])
    extends AuditTraceNamed(name) {
  override def +(child: NodeExtendedInfo): NodeExtendedInfo = child match {
    case n: AuditTraceNamed => new AuditTraceMergedChildren("trace", children + (n.name -> n))
  }
}
