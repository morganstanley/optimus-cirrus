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

/*
 * Groups OpResult of retrieval operation with the retrieved data and metadata.
 * Retrievals may result in returned data, metadata, data+metadata, or simply
 * existence results. Existence is indicated by OpResult only.
 */
class SegmentedRetrievalResult<V> extends RetrievalResultBase<V> {
  private final MetaData metaData;
  private final ByteBuffer[] buffers;
  private final OpResult opResult;

  public SegmentedRetrievalResult(MetaData metaData, BufferSourceDeserializer<V> valueDeserializer,
      ByteBuffer[] buffers, OpResult opResult) {
    super(valueDeserializer);
    this.metaData = metaData;
    this.buffers = buffers;
    this.opResult = opResult;
  }

  @Override
  public OpResult getOpResult() {
    return opResult;
  }

  @Override
  public V getValue() {
    if (value == valueNotSet) {
      // FUTURE - have an option to perform an eager deserialization
      if (buffers != null && buffers.length != 0) {
        //System.out.println("getValue()");
        //for (ByteBuffer buffer : buffers) {
        //    System.out.println(buffer +"\t"+ StringUtil.byteArrayToHexString(buffer.array(), 0, buffer.limit()));
        //}
        value = valueDeserializer.deserialize(buffers);
      } else {
        value = null;
      }
    }
    return value;
  }

  @Override
  public MetaData getMetaData() {
    return metaData;
  }
}
