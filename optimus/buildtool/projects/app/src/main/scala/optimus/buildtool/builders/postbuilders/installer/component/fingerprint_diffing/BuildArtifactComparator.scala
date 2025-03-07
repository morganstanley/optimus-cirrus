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
package optimus.buildtool.builders.postbuilders.installer.component.fingerprint_diffing

import optimus.platform._
import optimus.platform.util.Log
import optimus.workflow.filer.BuildArtifactUtils.JarToHashes
import optimus.workflow.filer.DiffMode
import optimus.workflow.filer.FingerprintDirComparison
import optimus.workflow.filer.JarComparison
import optimus.workflow.filer.model.ScopeFingerprintDiffs

object BuildArtifactComparator extends Log {
  def fingerprintDiffs(
      lhs: JarToHashes,
      rhs: JarToHashes,
      fingerprints: FingerprintDirComparison): Set[ScopeFingerprintDiffs] = {
    val differingJars = (lhs.keySet ++ rhs.keySet).toSeq.sorted
      .map(jarName => JarComparison(jarName, lhs.get(jarName), rhs.get(jarName)))
      .filter(_.diffMode != DiffMode.SAME)
      .flatMap { jarComparison =>
        {
          val diff = jarComparison.contentComparison.filter(_.diffMode != DiffMode.SAME).map(_.path).toSet
          if (diff.nonEmpty) Some(jarComparison.inferredScopeName -> diff) else None
        }
      }
      .toMap

    val differingFingerprints =
      fingerprints.scopes
        .filter(_.diffMode != DiffMode.SAME)
        .flatMap(s => {
          val diff = s.diff
          if (diff.nonEmpty) Some(s.scope -> s.diff) else None
        })
        .toMap

    (differingJars.keys ++ differingFingerprints.keys).toSet.map(scope =>
      ScopeFingerprintDiffs(
        scope,
        differingJars.getOrElse(scope, Set.empty),
        differingFingerprints.getOrElse(scope, Set.empty)))
  }

}
