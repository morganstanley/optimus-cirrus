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
package optimus.scalacompat.collection

import scala.collection.BuildFrom
import scala.collection.BuildFrom
import scala.collection.SortedMap
import scala.collection.SortedMapOps
import scala.xml.Node
import scala.xml.NodeSeq

trait MapBuildFromImplicitsLow {
  implicit def buildFromMapOpsToIterable[K0, V0, A]: BuildFrom[Map[K0, V0], A, Iterable[A]] =
    BuildFrom.buildFromIterableOps[Iterable, (K0, V0), A]
}

trait MapBuildFromImplicits extends MapBuildFromImplicitsLow {
  implicit def buildFromSortedMapOps[CC[X, Y] <: SortedMap[X, Y] with SortedMapOps[X, Y, CC, _], K0, V0, K: Ordering, V]
      : BuildFrom[CC[K0, V0], (K, V), CC[K, V]] = BuildFrom.buildFromSortedMapOps
  implicit def buildFromMapOps[CC[X, Y] <: Map[X, Y] with collection.MapOps[X, Y, CC, _], K0, V0, K, V]
      : BuildFrom[CC[K0, V0], (K, V), CC[K, V]] =
    BuildFrom.buildFromMapOps

  implicit def NodeSeqBuildFrom[A]: BuildFrom[NodeSeq, A, Seq[A]] = BuildFrom.buildFromIterableOps[Seq, Node, A]
}
