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

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentMap;

import com.ms.silverking.cloud.dht.common.EnumValues;
import com.ms.silverking.cloud.dht.common.OpResult;
import com.ms.silverking.cloud.dht.net.MessageGroup;
import com.ms.silverking.id.UUIDBase;
import com.ms.silverking.numeric.NumConversion;
import org.hibernate.validator.internal.util.ConcurrentReferenceHashMap;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

class ActiveVersionedBasicOperations {
  private final ConcurrentMap<UUIDBase, WeakReference<AsyncVersionedBasicOperationImpl>> activeOps;

  private static Logger log = LoggerFactory.getLogger(ActiveVersionedBasicOperations.class);

  ActiveVersionedBasicOperations() {
    activeOps = new ConcurrentReferenceHashMap<>();
  }

  public void addOp(AsyncVersionedBasicOperationImpl op) {
    activeOps.put(op.getUUID(), new WeakReference<>(op));
  }

  protected AsyncVersionedBasicOperationImpl getOperation(UUIDBase uuid) {
    WeakReference<AsyncVersionedBasicOperationImpl> ref = activeOps.get(uuid);
    if (ref == null) {
      return null;
    } else {
      return ref.get();
    }
  }

  public void receivedOpResponse(MessageGroup message) {
    long uuidMSL = message.getBuffers()[0].getLong(0);
    long uuidLSL = message.getBuffers()[0].getLong(NumConversion.BYTES_PER_LONG);
    byte resultCode = message.getBuffers()[0].get(2 * NumConversion.BYTES_PER_LONG);
    UUIDBase uuid = new UUIDBase(uuidMSL, uuidLSL);
    AsyncVersionedBasicOperationImpl operation = getOperation(uuid);
    if (operation != null) {
      OpResult opResult = EnumValues.opResult[resultCode];
      operation.setResult(opResult);
    } else {
      log.info("No operation for response: {}", uuid);
    }
  }
}
