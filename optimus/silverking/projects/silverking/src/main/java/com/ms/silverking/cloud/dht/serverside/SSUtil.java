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
package com.ms.silverking.cloud.dht.serverside;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.cloud.dht.daemon.storage.StorageFormat;
import com.ms.silverking.cloud.dht.daemon.storage.StorageParameters;

public class SSUtil {
  private static final byte[] emptyUserData = new byte[0];
  private static final ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);

  public static ByteBuffer retrievalResultBufferFromValue(ByteBuffer value,
                                                          SSStorageParameters storageParams,
                                                          SSRetrievalOptions options) {
    boolean returnValue = options.getRetrievalType().hasValue();
    ByteBuffer buf = ByteBuffer.allocate(MetaDataUtil.computeStoredLength(returnValue
                                                                          ? storageParams.getCompressedSize()
                                                                          : 0,
                                                                          storageParams.getChecksumType().length(),
                                                                          emptyUserData.length));
    StorageFormat.writeToBuf(null,
                             value,
                             StorageParameters.fromSSStorageParameters(storageParams),
                             emptyUserData,
                             buf,
                             new AtomicInteger(0),
                             Integer.MAX_VALUE,
                             returnValue);
    buf.position(0);
    return buf;
  }

  public static byte[] rawValueToStoredValue(byte[] rawValue, SSStorageParameters storageParams) {
    ByteBuffer buf = ByteBuffer.allocate(MetaDataUtil.computeStoredLength(storageParams.getCompressedSize(),
                                                                          storageParams.getChecksumType().length(),
                                                                          emptyUserData.length));
    StorageFormat.writeToBuf(null,
                             ByteBuffer.wrap(rawValue),
                             StorageParameters.fromSSStorageParameters(storageParams),
                             emptyUserData,
                             buf,
                             new AtomicInteger(0),
                             Integer.MAX_VALUE,
                             true);
    return buf.array();
  }

  public static byte[] metaDataToStoredValue(SSStorageParameters storageParams) {
    ByteBuffer buf = ByteBuffer.allocate(MetaDataUtil.computeMetaDataLength(storageParams.getChecksumType().length(),
                                                                            emptyUserData.length));
    StorageFormat.writeToBuf(null,
                             emptyBuffer,
                             StorageParameters.fromSSStorageParameters(storageParams),
                             emptyUserData,
                             buf,
                             new AtomicInteger(0),
                             Integer.MAX_VALUE,
                             true);
    return buf.array();
  }
}
