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
package com.ms.silverking.cloud.dht.client;

import com.ms.silverking.cloud.dht.client.gen.OmitGeneration;

/**
 * Implemented by classes that listen for callbacks on operation completion or update.
 */
@OmitGeneration // Omitting generation due to lack of circular reference resolution presently
public interface AsyncOperationListener {
  /**
   * Issued when an asynchronous operation has updated in a
   * significant way. If INCOMPLETE is *not* specified in listenStates
   * when registering this callback, then this method will be called
   * exactly once for any complete operation. If INCOMPLETE is specified
   * in the listenStates when registering this callback, then this method
   * may be called multiple times for this operation for any given OperationState.
   *
   * @param asyncOperation the operation that has updated
   */
  public void asyncOperationUpdated(AsyncOperation asyncOperation);
}
