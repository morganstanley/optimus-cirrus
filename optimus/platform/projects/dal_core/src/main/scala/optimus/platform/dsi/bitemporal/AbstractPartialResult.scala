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
package optimus.platform.dsi.bitemporal

abstract class AbstractPartialResult[T, R](val values: Iterable[T]) {
  type FullType <: AbstractFullResult[T, R]

  def isLast: Boolean

  def generateFullResult(partialResults: Iterable[T]): FullType
}

object AbstractPartialResult {
  def getFullResult[T, R, PR <: AbstractPartialResult[T, R]](allPartialResults: Iterable[PR]): PR#FullType = {
    allPartialResults.head.generateFullResult(allPartialResults.flatMap(_.values))
  }
}
