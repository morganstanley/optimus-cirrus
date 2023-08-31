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

import java.util.Map;
import java.util.Set;

import com.ms.silverking.cloud.dht.common.OpResult;

/**
 * An asynchronous keyed operation. Provides OperationState for all keys.
 *
 * @param <K> key type
 */
public interface AsyncKeyedOperation<K> extends AsyncOperation {
  /**
   * Get the number of keys in this operation.
   *
   * @return the number of keys in this operation
   */
  public int getNumKeys();

  /**
   * Get the set of keys in this operation.
   *
   * @return the set of keys in this operation
   */
  public Set<K> getKeys();

  /**
   * Get the set of all incomplete keys in this operation.
   *
   * @return the set of keys in this operation
   */
  public Set<K> getIncompleteKeys();

  /**
   * Get the OperationState for the given key.
   *
   * @param key given key
   * @return OperationState for the given key
   */
  public OperationState getOperationState(K key);

  /**
   * Get the OperationState for all keys in this operation
   *
   * @return the OperationState for all keys in this operation
   */
  public Map<K, OperationState> getOperationStateMap();

  public Map<K, OpResult> getOpResultMap();

  // NOTE: must not return null
  public OpResult getOpResult(K key);
}
