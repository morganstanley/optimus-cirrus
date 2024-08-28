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
import optimus.graph.ProgressType.ProgressType
import optimus.platform.ProgressListener

/**
 * Params to be passed when initializing a progress tracker.
 * @param weight
 *   The weight of the tracker.
 * @param initialMessage
 *   Initial progress message to be shown after the tracker is initialized.
 * @param listener
 *   Progress listener.
 * @param reportingIntervalMs
 *   How frequently the tracker should report progress to its parent.
 * @param progressType
 *   How progress is updated when the tracker receives an update from a child tracker.
 * @param cancellable
 *   Flag that determines if the underlying handler that we track for progress can be cancelled.
 * @param userCancellationScope
 *   The cancellation scope used for cancelling an underlying handler (through progress channel).
 */

private[optimus] final case class ProgressTrackerParams(
    weight: Double = 0.0,
    initialMessage: String = null,
    @transient listener: ProgressListener = null,
    reportingIntervalMs: Long = -1,
    progressType: ProgressType = ProgressType.Exact,
    cancellable: Boolean = false,
    userCancellationScope: CancellationScope = null
)
