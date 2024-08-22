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
package optimus.profiler.recipes

// No imports before here
import optimus.utils.MiscUtils // All other imports must go below under BEGINIMPORT, so they're available in console
// No more imports here

final case class Recipes(title: String, comment: String, code: String)

object Recipes {

  // Ensure that recipes compile with the same import settings as the graph console
  val defaultImport: String = "import " +
    MiscUtils
      .codeBetweenComments("BEGINIMPORT", "ENDIMPORT")
      .replaceAll("\n", ", ")
      .replaceAll("import ", "")

  // BEGINIMPORT
  import optimus.graph.diagnostics.PNodeTask
  import optimus.utils.MiscUtils
  import java.util.{ArrayList => JArrayList}
  import optimus.graph._
  import optimus.profiler.recipes.AlternativeAlgos._
  import optimus.profiler.DebuggerUI._
  import scala.jdk.CollectionConverters._
  import java.{util => jutil}
  import optimus.profiler.ui.GraphConsole
  import optimus.utils.misc.Color
  import scala.collection.mutable
  // ENDIMPORT
  import MiscUtils.codeStringCandy
  import MiscUtils.{codeString => code}

  val findJumpsInCache = new Recipes(
    "Find Jumps In Cache",
    "Compare 2 traces for differences in edge counts",
    code {
      val r0 = loadTrace("C:/MSDE/user/base.ogtrace", showTimeLine = true, showHotspots = true, showBrowser = true)
      val r1 = loadTrace("C:/MSDE/user/trainingQ.ogtrace", showTimeLine = true, showHotspots = true, showBrowser = true)
      compareNodeStacks(r0, compareTo = r1)
    }
  )

  val recipes = Array(
    new Recipes(
      "Show all traced nodes with exceptions...",
      "Filter all traced nodes and open is a browse dialog",
      code { browse("Title", n => n.isDoneWithException) }
    ),
    new Recipes(
      "Show all traced nodes with matching results ...",
      "Filter all traced nodes and open is a browse dialog",
      code {
        browse(
          "With result",
          n => {
            if (!n.isDoneWithResult) false
            else
              n.resultObject.asInstanceOf[Any] match {
                case d: Double => d > 30
                case _         => false
              }
          })
      }
    ),
    new Recipes(
      "Select from cache and browse...",
      "Filter all cached nodes (even if trace was off) and open is a browse dialog",
      code {
        browseCache(
          "Cached",
          n => {
            val args = n.args
            if (args.isEmpty) false
            else
              args(0) match {
                case d: Integer => d > 0
                case _          => false
              }
          })
      }
    ),
    new Recipes(
      "Find all property nodes of the same type",
      "Hint: look at the Console group in the browser context menu",
      code {
        val someNode: PropertyNode[_] = ???
        val pvs: mutable.Buffer[PNodeTask] = NodeTrace.getTrace(someNode.propertyInfo).asScala
      }
    ),
    new Recipes(
      "Organize top level callers",
      "Final result can be imported into excel",
      code {
        val pvs: Iterable[PNodeTask] = ???
        val callersByCallee: Iterable[Set[PNodeTask]] =
          pvs.map(n => ancestors(n).map(_._1).filter(_.getCallers.size == 0).toSet)
        val allCallers: Seq[PNodeTask] = callersByCallee.flatten.toSeq.distinct
        val cs: Iterable[(Set[PNodeTask], Int)] = callersByCallee.zipWithIndex
        // topLevelCaller -> Set(ordinal position of leaf)
        def leafSet(c: PNodeTask) = cs.filter(_._1.contains(c)).map(_._2).toSet
        val callerToLeaves: List[(PNodeTask, Set[Int])] = allCallers.toList.map(c => (c, leafSet(c)))
        // Output goes to console:
        callerToLeaves.foreach { case (c, pvset) =>
          val xs = (0 until pvs.size)
            .map { i =>
              if (pvset(i)) ",1" else ","
            }
            .mkString("")
          println(s"$c$xs") // for copy/paste into excel!
        }

      }
    ),
    new Recipes(
      "Find common ancestor candidates for parallelism improvements",
      "For each such candidate, list pairs (i,j), where the j'th leaf was started after the completion of the i'th",
      code {
        val pvs: Iterable[PNodeTask] = ???
        val cc: jutil.IdentityHashMap[PNodeTask, Set[(Int, Int)]] = closestCommonAncestors(pvs, exact = true)._2
        browse(new JArrayList[PNodeTask](cc.keySet), "ancestors")
        print(cc.asScala.mkString("\n")) // for copy/paste!
      }
    ),
    new Recipes(
      "Find common ancestors for caching",
      "Find common ancestors and count approximately how many leaf nodes they're responsible for",
      code {
        val pvs: Iterable[PNodeTask] = ???
        val ca: Map[String, Long] = closestCommonAncestors(pvs, exact = false)._1
      }
    ),
    findJumpsInCache
  )
}
