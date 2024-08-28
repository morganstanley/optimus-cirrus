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

/**
 * Allow different types of progress.
 *
 *   1. Exact: show the exact progress based on the children's progress and weights. Note that if the weights do not add
 *      to one then progress will not behave nicely (it will either report less than 100% when done or report 100%
 *      before being done).
 *
 *   1. Exponential Decay: Every time a child tracker reports progress, multiply the remainder of the progress by a
 *      factor smaller than one, which will result in the progress being incremented. The downside of such progress type
 *      is that weights play no role and also the first increments will be relatively large and will become smaller with
 *      each update.
 */
object ProgressType extends Enumeration {
  type ProgressType = Value

  val Exact, ExponentialDecay = Value
}
