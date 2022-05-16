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

object CollectionsHook {
  val log = LoggerFactory.getLogger(this.getClass)

  def isOptimusSeqCompatableCBF(cbf: CanBuildFrom[_, _, _]): Boolean = false
}
