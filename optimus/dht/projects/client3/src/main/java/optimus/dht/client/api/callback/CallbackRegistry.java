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
package optimus.dht.client.api.callback;

import java.time.Duration;

import javax.annotation.Nullable;

import optimus.dht.client.api.servers.ServerConnection;
import optimus.dht.common.api.transport.ReceivedMessageMetrics;

/**
 * A simple registry for callbacks.
 *
 * <p>It guarantees only one callback will be called at the same time.
 */
public interface CallbackRegistry {

  /** A reason why callback was fired. */
  enum CallbackReason {
    UPDATE(false),
    COMPLETED(false),
    TIMEOUT(true),
    CONNECTION_FAILED(true),
    CONNECTION_LOST(true);

    private final boolean failure;

    private CallbackReason(boolean failure) {
      this.failure = failure;
    }

    public boolean isFailure() {
      return failure;
    }
  }

  @FunctionalInterface
  interface Callback {
    void act(CallbackContext callbackContext);
  }

  interface CallbackContext {
    @Nullable
    ServerConnection server();

    Object key();

    CallbackReason reason();

    @Nullable
    Object param();

    @Nullable
    ReceivedMessageMetrics receivedMessageMetrics();

    /**
     * Cancels the current timeout and schedules a new one. Can only be called from inside UPDATE
     * callback.
     */
    void rescheduleTimeout(Duration timeout);
    /**
     * Removes the callback from the registry, so it won't be called again. Can only be called from
     * inside UPDATE callback.
     */
    void completed();
  }

  void registerCallback(
      @Nullable ServerConnection node, Object key, Callback callback, Object data);

  void registerCallbackWithTimeout(
      @Nullable ServerConnection node,
      Object key,
      Duration timeout,
      Callback callback,
      Object data);

  /**
   * Retrieves optional custom data stored for the given key.
   *
   * @return custom data for key, if found and exist
   */
  Object getData(@Nullable ServerConnection node, Object key);

  /**
   * Fires an UPDATE callback. May be called multiple times. Does not clear the timeout.
   *
   * @return if callback was found
   */
  boolean update(
      @Nullable ServerConnection node,
      Object key,
      @Nullable Object param,
      @Nullable ReceivedMessageMetrics receivedMessageMetrics);

  /**
   * Fires an COMPLETED callback. May be called only once. Clears the timeout.
   *
   * @return if callback was found
   */
  boolean complete(
      @Nullable ServerConnection node,
      Object key,
      @Nullable Object param,
      @Nullable ReceivedMessageMetrics receivedMessageMetrics);
}
