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

import com.ms.silverking.cloud.dht.client.AsyncSnapshot;
import com.ms.silverking.cloud.dht.client.OperationException;
import com.ms.silverking.cloud.dht.common.Context;

class AsyncSnapshotOperationImpl extends AsyncVersionedBasicOperationImpl implements AsyncSnapshot {
  public AsyncSnapshotOperationImpl(VersionedBasicNamespaceOperation versionedOperation, Context context, long curTime,
      byte[] originator) {
    super(versionedOperation, context, curTime, originator);
  }

  @Override
  protected void throwFailedException() throws OperationException {
    throw new SnapshotExceptionImpl(getFailureCause().toString());
  }
}
