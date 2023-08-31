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

import java.util.Comparator;

/**
 * This class imposes an ordering on instances that are being ordered for sending. We want to send
 * earlier versions first. Other ordering is arbitrary but intended to group compatible operations
 * together.
 */
class AsyncPutOperationImplComparator implements Comparator<AsyncPutOperationImpl> {
  public static final AsyncPutOperationImplComparator instance =
      new AsyncPutOperationImplComparator();

  @Override
  public int compare(AsyncPutOperationImpl o1, AsyncPutOperationImpl o2) {
    if (o1.getPotentiallyUnresolvedVersion() < o2.getPotentiallyUnresolvedVersion()) {
      return -1;
    } else if (o1.getPotentiallyUnresolvedVersion() > o2.getPotentiallyUnresolvedVersion()) {
      return 1;
    } else {
      return PutOptionsComparator.instance.compare(o1.putOptions(), o2.putOptions());
    }
  }
}
