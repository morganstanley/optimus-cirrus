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
package optimus.dht.common.api.transport;

import javax.annotation.Nullable;

/** Interface implemented by all stream handlers that can be notified on connection closed event. */
public interface CloseableStreamHandler {

  /**
   * Called when connection was closed.
   *
   * @param exception an exception that caused the connection to be closed, if known
   */
  default void connectionClosed(@Nullable Throwable exception) {}
}
