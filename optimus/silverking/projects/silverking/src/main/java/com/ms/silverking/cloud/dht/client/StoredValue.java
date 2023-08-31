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

/**
 * Represents data returned from a retrieval operation. Depending on the type of retrieval, this
 * object may contain an actual value and/or meta data for the stored value. For EXISTENCE
 * retrievals, a non-null object with no value and potentially no meta-data will be returned.
 *
 * <p>This class extends StoredValueBase with convenience forms of meta data methods.
 *
 * <p>This class also adds a guarantee that meta data retrieved is stored distinctly from the value.
 * Thus values may be garbage collected independently from the meta data.
 *
 * @param <V> value type
 */
public interface StoredValue<V> extends StoredValueBase<V>, MetaData {
  // FUTURE - think about the separate gc guarantee. Might need to provide an explicit method
  // for that.

  // FUTURE - think about multi-versioned results

  /**
   * Experimental - not yet supported
   *
   * @return TODO (OPTIMUS-0000): describe
   */
  public StoredValue<V> next();
}
