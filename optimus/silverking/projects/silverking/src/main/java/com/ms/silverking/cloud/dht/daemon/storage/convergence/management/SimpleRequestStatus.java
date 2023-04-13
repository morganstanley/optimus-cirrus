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
package com.ms.silverking.cloud.dht.daemon.storage.convergence.management;

import java.io.Serializable;

public class SimpleRequestStatus implements RequestStatus, Serializable {
  private final RequestState requestState;
  private final String statusString;

  private static final long serialVersionUID = -9154548957915879684L;

  public SimpleRequestStatus(RequestState requestState, String statusString) {
    this.requestState = requestState;
    this.statusString = statusString;
  }

  @Override
  public RequestState getRequestState() {
    return requestState;
  }

  @Override
  public boolean requestComplete() {
    return requestState.isComplete();
  }

  @Override
  public String getStatusString() {
    return statusString;
  }

  @Override
  public int hashCode() {
    return requestState.hashCode() ^ statusString.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    SimpleRequestStatus o;

    o = (SimpleRequestStatus) obj;
    return this.requestState == o.requestState && this.statusString.equals(o.statusString);
  }
}
