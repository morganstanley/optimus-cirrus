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

import com.ms.silverking.cloud.dht.CreationTime;
import com.ms.silverking.cloud.dht.ValueCreator;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.serialization.BufferSourceDeserializer;
import com.ms.silverking.cloud.dht.common.OpResult;

/*
 * Groups OpResult of retrieval operation with the retrieved data and metadata.
 * Retrievals may result in returned data, metadata, data+metadata, or simply
 * existence results. Existence is indicated by OpResult only.
 */
abstract class RetrievalResultBase<V> implements StoredValue<V> {
  protected V value;
  protected final BufferSourceDeserializer<V> valueDeserializer;
  protected RetrievalResultBase<V> next;

  protected static final Object valueNotSet = new Object();

  public RetrievalResultBase(BufferSourceDeserializer<V> valueDeserializer) {
    this.valueDeserializer = valueDeserializer;
    value = (V) valueNotSet;
  }

  @Override
  public StoredValue<V> next() {
    return null;
  }

  public abstract OpResult getOpResult();

  @Override
  public int getStoredLength() {
    return getMetaData().getStoredLength();
  }

  @Override
  public int getUncompressedLength() {
    return getMetaData().getUncompressedLength();
  }

  @Override
  public long getVersion() {
    return getMetaData().getVersion();
  }

  @Override
  public CreationTime getCreationTime() {
    return getMetaData().getCreationTime();
  }

  @Override
  public ValueCreator getCreator() {
    return getMetaData().getCreator();
  }

  @Override
  public long getLockMillisRemaining() {
    return getMetaData().getLockMillisRemaining();
  }

  @Override
  public short getLockSeconds() {
    return getMetaData().getLockSeconds();
  }

  @Override
  public boolean isLocked() {
    return getMetaData().isLocked();
  }

  @Override
  public boolean isInvalidation() {
    return getMetaData().isInvalidation();
  }

  // TODO (OPTIMUS-43326): Remove userdata. It should be part of value rather than metadata
  @Override
  public byte[] getUserData() {
    return getMetaData().getUserData();
  }

  @Override
  public byte[] getChecksum() {
    return getMetaData().getChecksum();
  }

  @Override
  public Compression getCompression() {
    return getMetaData().getCompression();
  }

  @Override
  public ChecksumType getChecksumType() {
    return getMetaData().getChecksumType();
  }

  public void setNext(RetrievalResultBase<V> next) {
    this.next = next;
  }

  public RetrievalResultBase<V> getNext() {
    return next;
  }

  @Override
  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(value);
    sb.append(':');
    sb.append(getMetaData());
    return sb.toString();
  }

  @Override
  public String toString(boolean labeled) {
    return MetaDataTextUtil.toMetaDataString(this, labeled);
  }
}
