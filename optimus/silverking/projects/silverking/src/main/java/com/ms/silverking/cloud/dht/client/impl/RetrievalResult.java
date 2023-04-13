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
package com.ms.silverking.cloud.dht.client.impl;

import java.nio.ByteBuffer;

import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;

/*
 * Groups OpResult of retrieval operation with the retrieved data and metadata.
 * Retrievals may result in returned data, metadata, data+metadata, or simply
 * existence results. Existence is indicated by OpResult only.
 */
class RetrievalResult<V> extends RetrievalResultBase<V> {
  private final RawRetrievalResult rawResult;

  public RetrievalResult(RawRetrievalResult rawResult, BufferSourceDeserializer<V> valueDeserializer) {
    super(valueDeserializer);
    this.rawResult = rawResult;
  }

  @Override
  public OpResult getOpResult() {
    return rawResult.getOpResult();
  }

  @Override
  public V getValue() {
    if (value == valueNotSet) {
      ByteBuffer rawValue;

      // FUTURE - have an option to perform an eager deserialization
      rawValue = rawResult.getValue();
      if (rawValue != null) {
        value = valueDeserializer.deserialize(rawValue);
      } else {
        value = null;
      }
    }
    return value;
  }

  @Override
  public MetaData getMetaData() {
    return rawResult.getMetaData();
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(value);
    sb.append(':');
    sb.append(rawResult.getMetaData());
    return sb.toString();
  }

  @Override
  public String toString(boolean labeled) {
    return MetaDataTextUtil.toMetaDataString(this, labeled);
  }
}
