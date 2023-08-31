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
package com.ms.silverking.cloud.dht;

import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.text.ObjectDefParser2;

/** Options for Get operations. (RetrievalOptions with WaitMode fixed at GET.) */
public final class GetOptions extends RetrievalOptions {
  private static final Set<String> exclusionFields = ImmutableSet.of("waitMode");

  private static final GetOptions template = new GetOptions();

  static {
    ObjectDefParser2.addParserWithExclusions(template, exclusionFields);
  }

  private GetOptions() {
    this(
        DHTConstants.standardTimeoutController,
        DHTConstants.noSecondaryTargets,
        RetrievalType.VALUE,
        VersionConstraint.defaultConstraint,
        NonExistenceResponse.defaultResponse,
        true,
        false,
        ForwardingMode.FORWARD,
        false);
  }

  ///
  /// TODO (OPTIMUS-0000): this is C++ only.
  /// This should be removed once C++ SKGetOptions.cpp is using the other constructor below
  // properly.
  ///

  /**
   * Construct a fully-specified GetOptions. Usage should be avoided; an instance should be obtained
   * and modified from an enclosing environment.
   *
   * @param opTimeoutController opTimeoutController for the operation
   * @param secondaryTargets constrains queried secondary replicas to operation solely on the node
   *     that receives this operation
   * @param retrievalType type of retrieval
   * @param versionConstraint specify the version
   * @param nonExistenceResponse action to perform for non-existent keys
   * @param verifyChecksums whether or not to verify checksums
   * @param returnInvalidations normally false, true causes invalidated values to be returned. only
   *     valid for META_DATA retrievals
   * @param forwardingMode FORWARD is for normal operation. DO_NOT_FORWARD restricts the get to the
   *     receiving node
   * @param updateSecondariesOnMiss update secondary replicas when a value is not found at the
   *     replica, but is found at the primary
   */
  public GetOptions(
      OpTimeoutController opTimeoutController,
      Set<SecondaryTarget> secondaryTargets,
      RetrievalType retrievalType,
      VersionConstraint versionConstraint,
      NonExistenceResponse nonExistenceResponse,
      boolean verifyChecksums,
      boolean returnInvalidations,
      ForwardingMode forwardingMode,
      boolean updateSecondariesOnMiss) {
    super(
        opTimeoutController,
        secondaryTargets,
        retrievalType,
        WaitMode.GET,
        versionConstraint,
        nonExistenceResponse,
        verifyChecksums,
        returnInvalidations,
        forwardingMode,
        false,
        null,
        null);
  }

  public GetOptions(
      OpTimeoutController opTimeoutController,
      Set<SecondaryTarget> secondaryTargets,
      RetrievalType retrievalType,
      VersionConstraint versionConstraint,
      NonExistenceResponse nonExistenceResponse,
      boolean verifyChecksums,
      boolean returnInvalidations,
      ForwardingMode forwardingMode,
      boolean updateSecondariesOnMiss,
      byte[] userOptions,
      byte[] authorizationUser,
      AllReplicasExcludedResponse allReplicasExcludedResponse,
      String zoneId) {
    this(
        opTimeoutController,
        secondaryTargets,
        DHTConstants.defaultTraceIDProvider,
        allReplicasExcludedResponse,
        retrievalType,
        versionConstraint,
        nonExistenceResponse,
        verifyChecksums,
        returnInvalidations,
        forwardingMode,
        updateSecondariesOnMiss,
        userOptions,
        authorizationUser,
        zoneId);
  }

  /**
   * Construct a fully-specified GetOptions. (Complete constructor for reflection) Usage should be
   * avoided; an instance should be obtained and modified from an enclosing environment.
   *
   * @param opTimeoutController opTimeoutController for the operation
   * @param secondaryTargets constrains queried secondary replicas to operation solely on the node
   *     that receives this operation
   * @param traceIDProvider trace provider for message group
   * @param retrievalType type of retrieval
   * @param versionConstraint specify the version
   * @param nonExistenceResponse action to perform for non-existent keys
   * @param verifyChecksums whether or not to verify checksums
   * @param returnInvalidations normally false, true causes invalidated values to be returned. only
   *     valid for META_DATA retrievals
   * @param updateSecondariesOnMiss update secondary replicas when a value is not found at the
   *     replica, but is found at the primary
   * @param userOptions side channel for user options that can be handled with custom logic
   * @param authorizationUser a username which may be required by an authorization plugin on the
   *     server
   */
  public GetOptions(
      OpTimeoutController opTimeoutController,
      Set<SecondaryTarget> secondaryTargets,
      TraceIDProvider traceIDProvider,
      AllReplicasExcludedResponse allReplicasExcludedResponse,
      RetrievalType retrievalType,
      VersionConstraint versionConstraint,
      NonExistenceResponse nonExistenceResponse,
      boolean verifyChecksums,
      boolean returnInvalidations,
      ForwardingMode forwardingMode,
      boolean updateSecondariesOnMiss,
      byte[] userOptions,
      byte[] authorizationUser,
      String zoneId) {
    super(
        opTimeoutController,
        secondaryTargets,
        traceIDProvider,
        allReplicasExcludedResponse,
        retrievalType,
        WaitMode.GET,
        versionConstraint,
        nonExistenceResponse,
        verifyChecksums,
        returnInvalidations,
        forwardingMode,
        updateSecondariesOnMiss,
        userOptions,
        authorizationUser,
        zoneId);
  }

  public GetOptions(
      OpTimeoutController opTimeoutController,
      Set<SecondaryTarget> secondaryTargets,
      TraceIDProvider traceIDProvider,
      AllReplicasExcludedResponse allReplicasExcludedResponse,
      RetrievalType retrievalType,
      VersionConstraint versionConstraint,
      NonExistenceResponse nonExistenceResponse,
      boolean verifyChecksums,
      boolean returnInvalidations,
      ForwardingMode forwardingMode,
      boolean updateSecondariesOnMiss,
      byte[] userOptions,
      byte[] authorizationUser) {
    this(
        opTimeoutController,
        secondaryTargets,
        traceIDProvider,
        allReplicasExcludedResponse,
        retrievalType,
        versionConstraint,
        nonExistenceResponse,
        verifyChecksums,
        returnInvalidations,
        forwardingMode,
        updateSecondariesOnMiss,
        userOptions,
        authorizationUser,
        DHTConstants.noZoneId);
  }

  /**
   * Return a GetOptions instance like this instance, but with a new OpTimeoutController.
   *
   * @param opTimeoutController the new field value
   * @return the modified GetOptions
   */
  public GetOptions opTimeoutController(OpTimeoutController opTimeoutController) {
    return new GetOptions(
        opTimeoutController,
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new SecondaryTargets.
   *
   * @param secondaryTargets the new field value
   * @return the modified GetOptions
   */
  public GetOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
    return new GetOptions(
        getOpTimeoutController(),
        secondaryTargets,
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new SecondaryTargets.
   *
   * @param secondaryTarget the new field value
   * @return the modified GetOptions
   */
  public GetOptions secondaryTargets(SecondaryTarget secondaryTarget) {
    Preconditions.checkNotNull(secondaryTarget);
    return new GetOptions(
        getOpTimeoutController(),
        ImmutableSet.of(secondaryTarget),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new traceIDProvider.
   *
   * @param traceIDProvider the new field value
   * @return the modified GetOptions
   */
  public GetOptions traceIDProvider(TraceIDProvider traceIDProvider) {
    Preconditions.checkNotNull(traceIDProvider);
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        traceIDProvider,
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new RetrievalType.
   *
   * @param retrievalType the new field value
   * @return the modified GetOptions
   */
  public GetOptions retrievalType(RetrievalType retrievalType) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new VersionConstraint.
   *
   * @param versionConstraint the new field value
   * @return the modified GetOptions
   */
  public GetOptions versionConstraint(VersionConstraint versionConstraint) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        versionConstraint,
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new NonExistenceResponse.
   *
   * @param nonExistenceResponse the new field value
   * @return the modified GetOptions
   */
  public GetOptions nonExistenceResponse(NonExistenceResponse nonExistenceResponse) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        nonExistenceResponse,
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new verifyChecksums.
   *
   * @param verifyChecksums the new field value
   * @return the modified GetOptions
   */
  public GetOptions verifyChecksums(boolean verifyChecksums) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        verifyChecksums,
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new returnInvalidations.
   *
   * @param returnInvalidations the new field value
   * @return the modified GetOptions
   */
  public GetOptions returnInvalidations(boolean returnInvalidations) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        returnInvalidations,
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new updateSecondariesOnMiss.
   *
   * @param updateSecondariesOnMiss the new field value
   * @return the modified GetOptions
   */
  public GetOptions updateSecondariesOnMiss(boolean updateSecondariesOnMiss) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        updateSecondariesOnMiss,
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new forwardingMode.
   *
   * @param forwardingMode the new field value
   * @return the modified GetOptions
   */
  public GetOptions forwardingMode(ForwardingMode forwardingMode) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        forwardingMode,
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new userOptions.
   *
   * @param userOptions the new field value
   * @return the modified GetOptions
   */
  public GetOptions userOptions(byte[] userOptions) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        userOptions,
        getAuthorizationUser(),
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new authorizationUser.
   *
   * @param authorizationUser the new field value
   * @return the modified GetOptions
   */
  public GetOptions authorizationUser(byte[] authorizationUser) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        authorizationUser,
        getZoneId());
  }

  /**
   * Return a GetOptions instance like this instance, but with a new allReplicasExcludedResponse.
   *
   * @param allReplicasExcludedResponse the new field value
   * @return the modified GetOptions
   */
  public GetOptions allReplicasExcludedResponse(
      AllReplicasExcludedResponse allReplicasExcludedResponse) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        allReplicasExcludedResponse,
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        getZoneId());
  }

  public GetOptions zoneId(String zoneId) {
    return new GetOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        getRetrievalType(),
        getVersionConstraint(),
        getNonExistenceResponse(),
        getVerifyChecksums(),
        getReturnInvalidations(),
        getForwardingMode(),
        getUpdateSecondariesOnMiss(),
        getUserOptions(),
        getAuthorizationUser(),
        zoneId);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }
}
