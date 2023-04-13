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

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;
import com.ms.silverking.cloud.dht.common.DHTKey;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.common.RawRetrievalResult;
import com.ms.silverking.cloud.dht.net.MessageGroupRetrievalResponseEntry;

class SegmentedRetrievalValue<K, V> extends FragmentedValue<MessageGroupRetrievalResponseEntry>
    implements ActiveKeyedOperationResultListener<MessageGroupRetrievalResponseEntry> {
  private final BufferSourceDeserializer<V> deserializer;
  private final MetaData metaData;
  private final AsyncRetrievalOperationImpl<K, V> parent;

  SegmentedRetrievalValue(DHTKey[] keys, DHTKey relayKey, AsyncRetrievalOperationImpl<K, V> parent,
      BufferSourceDeserializer<V> deserializer, MetaData metaData) {
    super(keys, relayKey, parent, false);
    this.deserializer = deserializer;
    this.metaData = metaData;
    this.parent = parent;
  }

  private OpResult getResult(DHTKey key) {
    MessageGroupRetrievalResponseEntry response;

    response = results.get(key);
    //System.out.printf("getResult(%s):\t%s\t%s\n", key, response, response == null ? OpResult.INCOMPLETE : response
    // .getOpResult());
    return response == null ? OpResult.INCOMPLETE : response.getOpResult();
  }

  @Override
  protected void checkForCompletion() {
    MessageGroupRetrievalResponseEntry response;
    OpResult result;

    result = OpResult.SUCCEEDED;
    for (DHTKey key : keys) {
      if (getResult(key) != OpResult.SUCCEEDED) {
        result = getResult(key);
        //System.out.println("Incomplete: "+ key);
      }
    }
    if (result == OpResult.SUCCEEDED) {
      //System.out.println("SegmentedRetrievalValue complete");
      parent.reassembledResultReceived(relayKey,
          new SegmentedRetrievalResult<>(metaData, deserializer, getBuffers(), result));
    } else {
      //System.out.println("SegmentedRetrievalValue incomplete");
      //parent.resultReceived(relayKey, MessageGroupRetrievalResponseEntry);
    }
  }

  private ByteBuffer[] getBuffers() {
    ByteBuffer[] buffers;

    buffers = new ByteBuffer[keys.length];
    for (int i = 0; i < keys.length; i++) {
      //buffers[i] = translateRawResult(results.get(keys[i]).getValue()); // deprecated
      buffers[i] = results.get(keys[i]).getValue();
    }
    return buffers;
  }

  // deprecated
  private ByteBuffer translateRawResult(ByteBuffer rawValue) {
    RawRetrievalResult rawResult;

    rawResult = new RawRetrievalResult(RetrievalType.VALUE);
    rawResult.setStoredValue_direct(rawValue);
    return rawResult.getValue();
  }
    
    /*
    private V deserialize() {
        ByteBuffer[]    buffers;
        
        buffers = new ByteBuffer[keys.length];
        for (int i = 0; i < keys.length; i++) {
            buffers[i] = results.get(keys[i]).getValue();
        }
        return deserializer.deserialize(buffers);
    }
    */
}
