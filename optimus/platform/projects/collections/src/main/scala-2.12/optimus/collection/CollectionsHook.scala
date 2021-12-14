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
package scala.collection.immutable

import java.util

import optimus.collection.OptimusSeq
import scala.collection.generic.CanBuildFrom
import org.slf4j.LoggerFactory

import scala.collection.WrappedCanBuildFrom

object CollectionsHook {
  val log = LoggerFactory.getLogger(this.getClass)

  private[this] val allOptimusSeqCompatableCBF = new util.IdentityHashMap[AnyRef, String]()

  private def addCBF(cbf: CanBuildFrom[_, _, _], comment: String): Unit = {
    val existing = allOptimusSeqCompatableCBF.put(cbf, comment)
    if (existing ne null) {
      log.info(s"shared canBuildFrom?? $existing, $comment")
    }
  }

  private def checkNotPresent(cbf: CanBuildFrom[_, _, _], comment: String): Unit = {
    val existing = allOptimusSeqCompatableCBF.remove(cbf)
    if (existing ne null) {
      log.warn(s"shared excluded canBuildFrom?? $existing, $comment")
    }
  }

  addCBF(OptimusSeq.canBuildFrom[String], "OptimusSeq")
  addCBF(scala.collection.immutable.IndexedSeq.canBuildFrom[String], "immutable.IndexedSeq")
  addCBF(scala.collection.IndexedSeq.canBuildFrom[String], "collection.IndexedSeq")
  addCBF(scala.collection.immutable.Seq.canBuildFrom[String], "immutable.Seq")
  addCBF(scala.collection.Seq.canBuildFrom[String], "collection.Seq")
  addCBF(scala.collection.immutable.Iterable.canBuildFrom[String], "scala.collection.immutable.Iterable")
  addCBF(scala.collection.Iterable.canBuildFrom[String], "scala.collection.Iterable")
  addCBF(scala.collection.immutable.Traversable.canBuildFrom[String], "immutable.Traversable")
  addCBF(scala.collection.Traversable.canBuildFrom[String], "collection.Traversable")

  checkNotPresent(Vector.canBuildFrom[String], "Vector")
  checkNotPresent(List.canBuildFrom[String], "List")

  def isOptimusSeqCompatibleCBF(cbf: CanBuildFrom[_, _, _]): Boolean = {
    allOptimusSeqCompatableCBF.containsKey(cbf) ||
    (cbf.isInstanceOf[WrappedCanBuildFrom[_, _, _]] && isOptimusSeqCompatibleCBF(
      cbf.asInstanceOf[WrappedCanBuildFrom[_, _, _]].wrapped))
  }
}
