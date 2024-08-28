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
 * A class that includes all the progress data that should be propagated, either to the parent tracker or to the
 * progress listener.
 * @param progress
 *   How much progress a tracker should add to the parent
 * @param message
 *   Progress message to report to parent
 * @param progressToListener
 *   Progress update to send to a listener. This will be defined only if it is different from the last progress update
 *   sent to the listener. This is to avoid sending the same progress value to the listener multiple times.
 * @param messageToListener
 *   Same as [[progressToListener]] but for the progress message.
 * @param shouldReportToParent
 *   Whether a child should update its parent.
 * @param shouldWaitBeforeReportingToParent
 *   Whether we should wait before reporting to parent or report immediately.
 * @param isUpdateFromChild
 *   Whether this progress state is an update from a child tracker
 * .
 */
private[graph] final case class ProgressTrackerState(
    progress: Double = 0.0,
    message: String = null,
    progressToListener: Double = -1.0,
    messageToListener: String = null,
    shouldReportToParent: Boolean = false,
    shouldWaitBeforeReportingToParent: Boolean = false,
    isUpdateFromChild: Boolean = false)
