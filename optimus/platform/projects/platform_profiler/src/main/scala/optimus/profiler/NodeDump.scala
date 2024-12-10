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
package optimus.profiler

import java.nio.charset.Charset

import net.iharder.Base64
import optimus.core.CoreHelpers
import optimus.graph.NodeTask
import optimus.platform.storable.Entity
import com.fasterxml.jackson.core.io.JsonStringEncoder

import scala.beans.BeanProperty
import scala.jdk.CollectionConverters._

class NodeDump() {
  var ntsk: NodeTask = _
  @BeanProperty protected var id: Int = _
  @BeanProperty protected var name: String = _
  // use java.util.List because it's serializable by jackson by default
  @BeanProperty protected var children: java.util.List[Int] = new java.util.ArrayList[Int]()
  @BeanProperty protected var parents: java.util.List[Int] = new java.util.ArrayList[Int]()
  @BeanProperty protected var args: java.util.List[String] = new java.util.ArrayList[String]()
  @BeanProperty protected var result: String = _
  @BeanProperty protected var vt: String = _
  @BeanProperty protected var tt: String = _

  private def dumpChildren(): Unit = {
    children.clear()
    val callees = ntsk.getCallees
    callees.asScala.foreach(callee => { children.add(callee.id) })
  }

  private def dumpParents(): Unit = {
    parents.clear()
    val callers = ntsk.getCallers
    if (!callers.isEmpty) {
      callers.asScala.foreach(caller => { parents.add(caller.id) })
    }
  }

  // dump args, result, vt and tt
  // dump them together because they are all should be underStackOf
  private def dumpOthers(): Unit = {
    val charset: Charset = Charset.forName("UTF-8")
    DebuggerUI.underStackOf(ntsk.scenarioStack()) {
      if (ntsk.isDoneWithResult) {
        // validTime and txTime
        val entity = ntsk.resultObject match {
          case e: Entity       => e
          case Some(e: Entity) => e
          case _               => null
        }
        if ((entity ne null) && !entity.dal$isTemporary) {
          vt = entity.validTime.toString
          tt = entity.txTime.toString
        }
      }

      // args
      args.clear()
      if (ntsk.args.nonEmpty) {
        ntsk.args.foreach(arg => {
          args.add(
            "\"" + Base64.encodeBytes(
              CoreHelpers.safeToString(arg, null, DebuggerUI.maxCharsInInlineArg).getBytes(charset)) + "\"")
        })
      }

      // result
      result = Base64.encodeBytes(ntsk.resultAsString.getBytes(charset))
    }
  }

  def setNodeTask(newNtsk: NodeTask): Unit = {
    ntsk = newNtsk
    id = ntsk.getId
    name = ntsk.toPrettyName(true, true)
    dumpChildren()
    dumpParents()
    dumpOthers()
  }

  private def dumpStringWithQuotes(target: AnyRef, sb: StringBuilder): StringBuilder = {
    if (target ne null) {
      sb.append('"')
      sb.appendAll(JsonStringEncoder.getInstance.quoteAsString(target.toString))
      sb.append('"')
    } else sb.append("null")
  }

  def dumpString(javaSB: java.lang.StringBuilder): StringBuilder = {
    val sb: StringBuilder = new StringBuilder(javaSB)
    sb.append("{")
    sb.append("\"id\":").append(id).append(",")
    sb.append("\"name\":"); dumpStringWithQuotes(name, sb); sb.append(",")
    // children
    sb.append("\"children\":").append(children.asScala.mkString("[", ",", "]")).append(",")
    // parents
    sb.append("\"parents\":").append(parents.asScala.mkString("[", ",", "]")).append(",")
    // others
    sb.append("\"args\":").append(args.asScala.mkString("[", ",", "]")).append(",")
    sb.append("\"result\":"); dumpStringWithQuotes(result, sb); sb.append(",")
    sb.append("\"vt\":"); dumpStringWithQuotes(vt, sb); sb.append(",")
    sb.append("\"tt\":"); dumpStringWithQuotes(tt, sb)
    sb.append("}")
  }
}
