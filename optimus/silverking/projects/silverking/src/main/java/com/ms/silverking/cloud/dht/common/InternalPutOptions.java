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
package com.ms.silverking.cloud.dht.common;

import java.util.Optional;

import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.serverside.SSPutOptions;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;

/**
 * Adds internally useful information to PutOptions that should not be exposed to end users. Also
 * removes PutOptions that only apply in the client.
 */
public class InternalPutOptions implements SSPutOptions {
  private final PutOptions putOptions;
  private final SkTraceId maybeTraceID;

  public InternalPutOptions(PutOptions putOptions, SkTraceId maybeTraceID) {
    this.putOptions = putOptions;
    this.maybeTraceID = maybeTraceID;
  }

  public InternalPutOptions PutOptions(PutOptions putOptions) {
    return new InternalPutOptions(putOptions, TraceIDProvider.noTraceID);
  }

  public PutOptions getPutOptions() {
    return putOptions;
  }

  public final InternalPutOptions authorizedAs(byte[] authorizationUser) {
    PutOptions opt = putOptions.authorizationUser(authorizationUser);
    return new InternalPutOptions(opt, maybeTraceID);
  }

  @Override
  public final byte[] getAuthorizationUser() {
    return putOptions.getAuthorizationUser();
  }

  @Override
  public byte[] getUserData() {
    return putOptions.getUserData();
  }

  @Override
  public int hashCode() {
    throw new RuntimeException("not implemented");
  }

  @Override
  public boolean equals(Object other) {
    throw new RuntimeException("not implemented");
  }

  @Override
  public String toString() {
    return putOptions.toString();
  }

  @Override
  public Optional<SkTraceId> getTraceID() {
    return Optional.ofNullable(maybeTraceID);
  }
}
