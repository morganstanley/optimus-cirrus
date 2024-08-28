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
package optimus.graph

import optimus.core.TPDMask
import optimus.graph.diagnostics.PNodeTaskInfo
import optimus.graph.diagnostics.pgo.Profiler

import scala.collection.mutable

object XSFTRemapper {

  private val internalTweakName = "INTERNAL_TweakMarker"

  /**
   * 'global' across all merged ogtraces. Note: MergeTraces can run multiple times in one go, since it works for groups
   * of optconfs. We don't reset this map in between but that's OK, if we already have a registered id for a given node
   * name then we can still use it for a new group that is about to be merged
   */
  private val nameToTweakID = mutable.Map[String, Int](internalTweakName -> 1) // [SEE_REMAP_TWEAK_DEPS]

  private[optimus] def TEST_ONLY_resetNameToTweakID(): Unit = {
    nameToTweakID.clear()
    nameToTweakID.put(internalTweakName, 1)
  }

  /**
   * Update remappedIds with freshly assigned IDs for each unique name. This is extremely similar to the logic in
   * GridProfiler.remapTweakIds, which combines dependencies reported from engines, but I haven't combined the logic
   * because of two key differences:
   *   1. GridProfiler needs to synchronize on its nameToTweakID map because multiple engines can be reporting to client
   *      and updating that map at the same time. Here, we read files one by one so we don't need the synchronization
   *
   *   2. In GridProfiler we only include pntis that were tweaked, because we're combining data from 'live' pntis. Here
   *      we're reading information from optconf files so we can't check if the pnti was tweaked, but it wouldn't have
   *      ended up in the file at all if it hadn't been tweaked on the training run
   */
  private def assignInitialID(name: String, pnti: PNodeTaskInfo, remappedIds: mutable.Map[Int, Int]): Unit = {
    val twkID = pnti.tweakID
    if (twkID > 0) {
      val newID =
        if (twkID == 1) 1 // don't remap internal tweaks
        else nameToTweakID.getOrElseUpdate(name, nameToTweakID.size + 1)
      remappedIds.put(twkID, newID)
    }
  }

  /** rewrite dependencies on pntis in terms of the ID remapping provided - note this mutates the pntis */
  def remapTweakDependencies(
      nameToPnti: Map[String, PNodeTaskInfo],
      remappedIds: Map[Int, Int]): Map[String, PNodeTaskInfo] = {
    val hotspots = mutable.Map[String, PNodeTaskInfo]()
    nameToPnti.foreach { case (name, pnti) =>
      if (pnti.tweakID > 0)
        pnti.tweakID = remappedIds.getOrElse(pnti.tweakID, 0 /* Ignore tweakables */ ) // [SEE_TWEAKED]
      val mask = if (!pnti.tweakDependencies.allBitsAreSet) {
        // TODO (OPTIMUS-38391): Deal with overflow (for better reuse, not correctness)
        val deps = pnti.tweakDependencies.toIndexes
        val remapped = deps.flatMap { remappedIds.get }
        TPDMask.fromIndexes(remapped)
      } else // TPDMask.empty represents 'uninitialized' rather than 'no dependencies', so won't write it to optconf
        TPDMask.empty
      pnti.tweakDependencies = mask
      hotspots.put(name, pnti)
    }
    hotspots.toMap
  }

  private def remap(pntis: collection.Seq[PNodeTaskInfo]): collection.Seq[PNodeTaskInfo] = {
    val remappedIds = mutable.Map[Int, Int](1 -> 1) // internal tweakables (1)

    // step 1: get a whole set of fresh IDs for each unique name in our traces
    pntis.foreach { pnti =>
      assignInitialID(pnti.fullName, pnti, remappedIds)
    }

    // step 2: rewrite dependencies learned from each trace in terms of new IDs
    val nameToPnti = pntis.map { pnti =>
      pnti.fullName -> pnti
    }.toMap
    val remapped = remapTweakDependencies(nameToPnti, remappedIds.toMap)

    // step 3: remove cache names from pntis
    remapped.values.foreach { pnti =>
      pnti.cacheName = null
    }
    remapped.values.toSeq
  }

  def combineDependencies(
      trace: collection.Seq[PNodeTaskInfo],
      otherTrace: collection.Seq[PNodeTaskInfo]): collection.Seq[PNodeTaskInfo] = {
    val remappedTrace = remap(trace)
    val remappedOtherTrace = remap(otherTrace)

    // step 3: the usual combine (which will 'add' dependencies from both traces)
    Profiler.combineTraces(remappedTrace ++ remappedOtherTrace)
  }
}
