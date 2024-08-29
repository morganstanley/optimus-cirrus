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
package optimus.dsi.base

import optimus.platform.dsi.bitemporal.Command

/**
 * Exception type that causes DSI to completely halt without attempting to rollback transaction or commit final actions
 */
sealed class DSIHaltException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(cause: Throwable) = this(if (cause == null) null else cause.toString, cause)
  def this(message: String) = this(message, null)
  def this() = this("", null)
}

/**
 * for testing to inject non-recoverable errors into the metadata write
 */
class InjectedFaultException extends DSIHaltException

/**
 * Exception thrown by the dsi writer in case of an exception during the metadata write phase
 */
class MetadataWriteFailureException(val requestInfo: String, cause: Throwable) extends DSIHaltException(cause)

/**
 * Exception thrown by the post commit executor in case of an exception during the post commit execution
 */
class PostCommitFailureException(cause: Throwable) extends DSIHaltException(cause)
