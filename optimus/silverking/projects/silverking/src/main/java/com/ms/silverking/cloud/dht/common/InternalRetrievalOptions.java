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

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.RetrievalOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitMode;
import com.ms.silverking.cloud.dht.serverside.SSRetrievalOptions;
import com.ms.silverking.cloud.dht.trace.SkTraceId;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;

/**
 * Adds internally useful information to RetrievalOptions that should not be exposed to end users.
 * Also removes RetrievalOptions that only apply in the client.
 */
public class InternalRetrievalOptions implements SSRetrievalOptions {
  private final RetrievalOptions retrievalOptions;
  private final boolean verifyIntegrity;
  private final ConsistencyProtocol
      cpSSToVerify; // ConsistencyProtocol to verify storage state against
  // non-null value implies that state should be verified
  private final SkTraceId maybeTraceID;
  private final byte[] originator;

  public static final byte[] noOriginator = null;

  public InternalRetrievalOptions(
      RetrievalOptions retrievalOptions,
      boolean verifyIntegrity,
      ConsistencyProtocol cpSSToVerify,
      SkTraceId maybeTraceID,
      byte[] originator) {
    this.retrievalOptions = retrievalOptions;
    this.verifyIntegrity = verifyIntegrity;
    this.cpSSToVerify = cpSSToVerify;
    this.maybeTraceID = maybeTraceID;
    this.originator = originator;
  }

  public InternalRetrievalOptions(
      RetrievalOptions retrievalOptions,
      boolean verifyIntegrity,
      ConsistencyProtocol cpSSToVerify) {
    this(
        retrievalOptions,
        verifyIntegrity,
        cpSSToVerify,
        TraceIDProvider.noTraceID,
        InternalRetrievalOptions.noOriginator);
  }

  public InternalRetrievalOptions(RetrievalOptions retrievalOptions, boolean verifyIntegrity) {
    this(retrievalOptions, verifyIntegrity, null);
  }

  public InternalRetrievalOptions(RetrievalOptions retrievalOptions) {
    this(retrievalOptions, false, null);
  }

  public static InternalRetrievalOptions fromSSRetrievalOptions(SSRetrievalOptions options) {
    if (options instanceof InternalRetrievalOptions) {
      return (InternalRetrievalOptions) options;
    } else {
      RetrievalOptions retrievalOptions;

      retrievalOptions =
          new RetrievalOptions(
              null,
              null,
              options.getRetrievalType(),
              WaitMode.GET,
              options.getVersionConstraint(),
              null,
              options.getVerifyIntegrity(),
              options.getReturnInvalidations(),
              null,
              false,
              null,
              null);
      return new InternalRetrievalOptions(retrievalOptions, options.getVerifyIntegrity());
    }
  }

  public InternalRetrievalOptions retrievalOptions(RetrievalOptions retrievalOptions) {
    return new InternalRetrievalOptions(retrievalOptions, verifyIntegrity, cpSSToVerify);
  }

  public InternalRetrievalOptions retrievalType(RetrievalType retrievalType) {
    return retrievalOptions(retrievalOptions.retrievalType(retrievalType));
  }

  public RetrievalOptions getRetrievalOptions() {
    return retrievalOptions;
  }

  @Override
  public boolean getVerifyIntegrity() {
    return verifyIntegrity;
  }

  public ConsistencyProtocol getCPSSToVerify() {
    return cpSSToVerify;
  }

  public boolean getVerifyStorageState() {
    return cpSSToVerify != null;
  }

  @Override
  public boolean getReturnInvalidations() {
    return retrievalOptions.getReturnInvalidations();
  }

  /**
   * @return
   */
  @Override
  public RetrievalType getRetrievalType() {
    return retrievalOptions.getRetrievalType();
  }

  /**
   * waitMode getter
   *
   * @return waidMode
   */
  public final WaitMode getWaitMode() {
    return retrievalOptions.getWaitMode();
  }

  /**
   * versionConstraint getter
   *
   * @return
   */
  @Override
  public final VersionConstraint getVersionConstraint() {
    return retrievalOptions.getVersionConstraint();
  }

  /**
   * nonexistenceResponse getter
   *
   * @return
   */
  public final NonExistenceResponse getNonExistenceResponse() {
    return retrievalOptions.getNonExistenceResponse();
  }

  /**
   * userOptions getter
   *
   * @return
   */
  @Override
  public final byte[] getUserOptions() {
    return retrievalOptions.getUserOptions();
  }

  @Override
  public final byte[] getAuthorizationUser() {
    return retrievalOptions.getAuthorizationUser();
  }

  @Override
  public final String getZoneId() {
    return retrievalOptions.getZoneId();
  }

  public final InternalRetrievalOptions authorizedAs(byte[] authorizationUser) {
    RetrievalOptions opt = retrievalOptions.authorizationUser(authorizationUser);
    return new InternalRetrievalOptions(
        opt, verifyIntegrity, cpSSToVerify, maybeTraceID, originator);
  }

  @Override
  public final byte[] getOriginator() {
    return originator;
  }

  @Override
  public Optional<SkTraceId> getTraceID() {
    return Optional.ofNullable(maybeTraceID);
  }

  /**
   * Return copy of this object with modified VersionConstraint
   *
   * @param vc
   * @return
   */
  public InternalRetrievalOptions versionConstraint(VersionConstraint vc) {
    return new InternalRetrievalOptions(
        retrievalOptions.versionConstraint(vc),
        verifyIntegrity,
        cpSSToVerify,
        maybeTraceID,
        originator);
  }

  /**
   * Return copy of this object with modified WaitMode
   *
   * @param waitMode
   * @return
   */
  public InternalRetrievalOptions waitMode(WaitMode waitMode) {
    return new InternalRetrievalOptions(
        retrievalOptions.waitMode(waitMode),
        verifyIntegrity,
        cpSSToVerify,
        maybeTraceID,
        originator);
  }

  /**
   * Return copy of this object with modified verifyStorageState
   *
   * @param cpSSToVerify
   * @return
   */
  public InternalRetrievalOptions cpSSToVerify(ConsistencyProtocol cpSSToVerify) {
    return new InternalRetrievalOptions(
        retrievalOptions, verifyIntegrity, cpSSToVerify, maybeTraceID, originator);
  }

  public InternalRetrievalOptions maybeTraceID(SkTraceId maybeTraceID) {
    return new InternalRetrievalOptions(
        retrievalOptions, verifyIntegrity, cpSSToVerify, maybeTraceID, originator);
  }

  public InternalRetrievalOptions originator(byte[] originator) {
    return new InternalRetrievalOptions(
        retrievalOptions, verifyIntegrity, cpSSToVerify, maybeTraceID, originator);
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
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(retrievalOptions);
    sb.append(':');
    sb.append(verifyIntegrity);
    return sb.toString();
  }
}
