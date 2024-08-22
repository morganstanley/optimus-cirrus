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
package optimus.profiler.utils

import java.io.StreamTokenizer
import java.io.StringReader

import optimus.core.EdgeList
import optimus.graph.NodeTaskInfo
import optimus.graph.PropertyInfo
import optimus.graph.diagnostics.PNodeTask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.PNodeTaskRecorded
import optimus.platform.util.Log

import scala.collection.mutable
import scala.util.control.Breaks
import scala.util.control.Breaks._

/**
 * Used to reconstruct hotspots from NodeDiGraphWriter graphs (currently only for uncompressed version) Heavily borrowed
 * from DigraphReader for TTrack graphs
 * @param text
 *   graphviz/dot representation as string
 */
final class NodeDiGraphReader(val text: String) extends Log {
  private val st = new StreamTokenizer(new StringReader(text))
  private val pntis = mutable.Map[String, PNodeTaskInfo]()
  private val tasks = mutable.Map[String, PNodeTask]()

  private def eatTo(ttype: Int): Unit = {
    breakable {
      while (st.nextToken() != StreamTokenizer.TT_EOF) {
        if (st.ttype == ttype || st.ttype == '\n')
          break()
      }
    }
  }

  // return pntis and pnts, together with ids, so that we can reconstruct graphviz representation from 'fake' hotspots
  // rather than from OGTraceReader
  def read(): (Map[String, PNodeTask], Map[String, PNodeTaskInfo]) = {
    st.nextToken()
    st.eolIsSignificant(true)
    st.whitespaceChars('-', '-')
    if (st.ttype != StreamTokenizer.TT_WORD || st.sval != "digraph")
      throw new IllegalArgumentException("Header?")
    st.nextToken()
    if (st.ttype != '{')
      throw new IllegalArgumentException("Expected '{'")

    val loop = new Breaks
    loop.breakable {
      while (st.nextToken() != StreamTokenizer.TT_EOF) {
        if (st.ttype == '{')
          eatTo('}') // Eat rank info....
        else if (st.ttype == StreamTokenizer.TT_WORD) {
          if (st.sval == "}") loop.break() // End sub-graph
          breakable {
            if (st.sval == "graph" || st.sval == "node" || st.sval == "edge") { // Graph, node, edge parameters
              eatTo(';') // Ignore the attributes...
              break() // break out of the 'breakable', continue the outside loop
            }
            val dotNodeName = st.sval // Graphviz node name
            val dotNodeId = dotNodeName.tail.toInt // node names are of the form "ni" where i is an int
            st.nextToken()
            if (st.ttype == '[') { // Read attributes
              st.nextToken()
              val attr = st.sval
              breakable {
                if (attr != "label") {
                  eatTo(']')
                  break() // break out of the 'breakable', continue the outside loop
                }
              }
              st.nextToken()
              if (st.ttype != '=')
                throw new IllegalArgumentException("Expected '='")
              st.nextToken()

              val pntiName = st.sval // task name
              var shape = ""
              st.nextToken()
              if (st.ttype == StreamTokenizer.TT_WORD && st.sval == "shape") {
                st.nextToken() // '='
                st.nextToken() // shape name
                shape = st.sval
              } else
                st.pushBack()

              var style = ""
              st.nextToken()
              if (st.ttype == StreamTokenizer.TT_WORD && st.sval == "style") {
                st.nextToken() // '='
                st.nextToken() // style name
                style = st.sval
              } else
                st.pushBack()

              var colour = ""
              st.nextToken()
              if (st.ttype == StreamTokenizer.TT_WORD && st.sval == "color") {
                st.nextToken() // '='
                st.nextToken() // colour name
                colour = st.sval
              } else
                st.pushBack()

              val pnti = new PNodeTaskInfo(dotNodeId)
              if (shape == NodeDiGraphWriter.PROXY_SHAPE) {
                pnti.flags |= NodeTaskInfo.PROFILER_PROXY
              }

              if (colour == NodeDiGraphWriter.TWEAKABLE_COLOUR) {
                pnti.flags |= NodeTaskInfo.TWEAKABLE
              }

              if (style == NodeDiGraphWriter.INTERNAL) {
                pnti.nti = NodeTaskInfo.internal(pntiName, pnti.flags)
                pnti.flags = pnti.nti.snapFlags() // because NodeTaskInfo.internal adds PROFILER_TRANSPARENT flag
              } else {
                val pinfo = new PropertyInfo(pntiName, pnti.flags, null)
                pnti.nti = pinfo
              }

              val pnt = new PNodeTaskRecorded(dotNodeId)
              pnt.info = pnti
              pntis.put(dotNodeName, pnti)
              tasks.put(dotNodeName, pnt)

              eatTo(']')
            } else if (st.ttype == '>') { // Read edge
              st.nextToken()
              val edgeTo = st.sval
              val fromTask = tasks.get(dotNodeName)
              val toTask = tasks.get(edgeTo)
              (fromTask, toTask) match {
                case (Some(from), Some(to)) => edge(from, to)
                // following cases can happen for constants generated by writer (ie, nodes in the graph that have no pntis)
                case (Some(from), None) =>
                  val dummyTo = addMissingNode(edgeTo)
                  edge(from, dummyTo)
                case (None, Some(to)) =>
                  val dummyFrom = addMissingNode(dotNodeName)
                  edge(dummyFrom, to)
                case _ => // unexpected
                  log.warn(s"No tasks found for fromTask ($dotNodeName) or toTask ($edgeTo)")
              }
            }
          }
        }
      }
    }
    (tasks.toMap, pntis.toMap)
  }

  private def addMissingNode(missingNode: String): PNodeTask = { // because edge in graph is of the form "ni" where i is an int
    val id = missingNode.tail.toInt
    val dummy = new PNodeTaskRecorded(id)
    dummy.info = new PNodeTaskInfo(id)
    pntis.put(missingNode, null) // we deal with null pntis in the writer
    tasks.put(missingNode, dummy)
    dummy
  }

  private def edge(from: PNodeTask, to: PNodeTask): Unit = {
    if (from.callees == null) from.callees = EdgeList.newDefault()
    from.callees.add(to)
  }
}
