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
package optimus.buildtool.runconf.compile.plugins.native

import optimus.buildtool.runconf.FilePath
import optimus.buildtool.runconf.JavaPattern
import optimus.buildtool.runconf.plugins.NativeLibraries
import optimus.buildtool.runconf.plugins.ReorderSpec

import scala.collection.immutable.Seq

object NativeLibrariesResolver {

  def resolve(nativeLibraries: NativeLibraries, fromDeps: Seq[FilePath]): Seq[FilePath] = {
    val allIncludes = nativeLibraries.defaults ++ fromDeps ++ nativeLibraries.includes
    val afterExcluding = applyExcludes(allIncludes, nativeLibraries.excludes)
    val reordered = applyReordering(nativeLibraries.reorder, afterExcluding)
    reordered.distinct
  }

  private def applyExcludes(includes: Seq[FilePath], excludes: Seq[JavaPattern]): Seq[FilePath] = {
    excludes.foldLeft(includes) { (paths, excludePattern) =>
      paths.filterNot(path => path.matches(excludePattern))
    }
  }

  /**
   * If there is any path that should be after, but is before (in terms of specified prefixes), then we take all these
   * paths that match should-be-before-prefix and put them all together (in the same order) right before the first path
   * that matches should-be-after-prefix.
   */
  private def applyReordering(reorder: Seq[ReorderSpec], excluded: Seq[FilePath]) = {
    reorder.foldLeft(excluded) { case (paths, ReorderSpec(shouldBeBefore, shouldBeAfter)) =>
      val firstIndices = paths.zipWithIndex.collect { case (path, index) if path.startsWith(shouldBeBefore) => index }
      val secondIndices = paths.zipWithIndex.collect { case (path, index) if path.startsWith(shouldBeAfter) => index }
      val indicesToMoveBefore = secondIndices.flatMap { shouldBeAfterIndex =>
        firstIndices.filter { shouldBeBeforeIndex =>
          shouldBeBeforeIndex > shouldBeAfterIndex
        }
      }.distinct
      if (indicesToMoveBefore.nonEmpty) {
        val toMove = indicesToMoveBefore.map(paths(_))
        val withoutToMove = paths.filterNot(toMove.contains)
        val insertAt = withoutToMove.indexOf(paths(secondIndices.min))
        val (prefix, suffix) = withoutToMove.splitAt(insertAt)
        prefix ++ toMove ++ suffix
      } else paths
    }
  }

}
