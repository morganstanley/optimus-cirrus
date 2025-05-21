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
package optimus.platform.temporalSurface.impl

import optimus.platform.dal.QueryTemporality
import optimus.platform._
import optimus.platform.temporalSurface.TemporalSurface
import optimus.platform.temporalSurface.operations._
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingDataManager
import optimus.platform.temporalSurface.tsprofiler.TemporalSurfaceProfilingData

object QueryTemporalityResolver {

  /**
   * This is DFS search for the fit temporal surface for a given composite Query tree. The basic logic will be:
   *   1. If the scope of branch temporal surface Always match the Query, than we only search that branch, no need for
   *      others
   *
   * 2. If the scope of branch temporal surface Never match the query, than we can skip that branch, just look for
   * others
   *
   * 3. If the scope of branch temporal surface may match the query, deep first search the whole temporal surface tree
   *
   * 4. If the current temporal surface match result is NeverMatch, search the next temporal surface (with previous
   * temporality)
   *
   * 5. If the current temporal surface match result is CantTell, and its temporality equals to previous CantTell
   * temporal surface, search the next temporal surface otherwise return None (different possible temporality, given up
   *   - need to fix if broker understand temporal surface)
   *
   * 6. If the current temporal surface match result is AlwaysMatch, and its temporality equals to previous CantTell
   * temporal surface, return current temporality otherwise return None (we can't give accurate result, so give up -
   * need to fix if broker understand temporal surface)
   *
   * @param tsList
   *   the temporal surfaces we need to search
   * @param qt
   *   the query tree which is composed by AND and OR
   * @return
   *   query temporality if found, else None
   */
  @async def resolveQueryTemporality(
      tsList: Seq[TemporalSurface],
      qt: QueryTree,
      previous: Option[QueryTemporality.At],
      headTsProf: Option[TemporalSurfaceProfilingData],
      parentTsProf: Option[TemporalSurfaceProfilingData]
  ): Option[QueryTemporality.At] = {
    val (time, queryTempRes) = AdvancedUtils.timed {
      if (tsList.isEmpty) None
      else {
        val headTS = tsList.head
        headTsProf.foreach(tsProf => require(tsProf.ts eq headTS))
        headTsProf.foreach(_.recordVisit())

        headTS match {
          case branch: BranchTemporalSurfaceImpl =>
            val (time, scopeMatch) = AdvancedUtils.timed(qt.doScopeMatch(branch))
            headTsProf.foreach(_.recMatchWalltime(time))

            scopeMatch match {
              case AlwaysMatch =>
                headTsProf.foreach(_.recordMatcherHit())
                // if the branch's scope match the query, search its children
                // if nothing matches in children, backtrack and search its siblings
                val temporalityRes = resolveTemporality(headTsProf, qt, branch.children, previous)
                if (temporalityRes.isDefined) temporalityRes
                else resolveTemporality(parentTsProf, qt, tsList.tail, previous)
              case NeverMatch =>
                // if the branch's scope won't match the query, we should skip this branch, so directly search others
                resolveTemporality(parentTsProf, qt, tsList.tail, previous)
              case other =>
                // we don't have enough to say if the branch matches the query or not, using the common logic
                val temporalityRes = resolveTemporality(headTsProf, qt, branch.children, previous)
                if (temporalityRes.isDefined) temporalityRes
                else resolveTemporality(parentTsProf, qt, tsList.tail, previous)
            }

          case leaf: LeafTemporalSurfaceImpl =>
            val (time, leafMatchRes) = AdvancedUtils.timed(qt.doMatch(leaf))
            headTsProf.foreach(_.recMatchWalltime(time))

            leafMatchRes match {
              case NeverMatch => resolveTemporality(parentTsProf, qt, tsList.tail, previous)
              case CantTell =>
                val needContinue = previous.map(_ == leaf.currentTemporality).getOrElse(true)
                if (needContinue)
                  resolveTemporality(parentTsProf, qt, tsList.tail, Some(leaf.currentTemporality))
                else None
              case AlwaysMatch =>
                headTsProf.foreach(_.recordHit())
                headTsProf.foreach(_.recordMatcherHit())
                val isResult = previous.map(_ == leaf.currentTemporality).getOrElse(true)
                if (isResult) Some(leaf.currentTemporality)
                else None
              case _ => None // for any other match result, we should throw exception
            }
        }
      }
    }
    headTsProf.foreach(_.recWallTime(time))
    queryTempRes
  }

  @async def resolveTemporality(
      tsProfToUpdate: Option[TemporalSurfaceProfilingData],
      qt: QueryTree,
      tsList: Iterable[TemporalSurface],
      previous: Option[QueryTemporality.At]): Option[QueryTemporality.At] = {
    if (tsList.nonEmpty) {
      tsProfToUpdate.foreach(_.recordVisitedChildren())
      val newCurrentHeadTSProf = updateCurrentHeadTSProf(tsList.toSeq, tsProfToUpdate)
      // called resolveQueryTemporality with new head TS and previous head TS
      resolveQueryTemporality(tsList.toSeq, qt, previous, newCurrentHeadTSProf, tsProfToUpdate)
    } else None
  }

  // returns the new current TS prof (the TS prof corresponding to head element)
  def updateCurrentHeadTSProf(
      passedTSList: Seq[TemporalSurface],
      currentTSProf: Option[TemporalSurfaceProfilingData]): Option[TemporalSurfaceProfilingData] =
    if (passedTSList.nonEmpty && currentTSProf.isDefined) {
      val newHeadTS = passedTSList.head
      val newHeadTSProf =
        TemporalSurfaceProfilingDataManager.maybeCreateChildProfilingEntry(newHeadTS, currentTSProf.get)
      newHeadTSProf
    } else None
}
