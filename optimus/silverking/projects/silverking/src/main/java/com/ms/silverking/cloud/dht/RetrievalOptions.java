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

import java.util.Arrays;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.WaitForTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.common.OptionsHelper;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Options for RetrievalOperations. Specifies what to retrieve, how to respond to non-existent entries, and
 * whether or not to validate value checksums.
 */
public class RetrievalOptions extends OperationOptions {
  private final RetrievalType retrievalType;
  private final WaitMode waitMode;
  private final VersionConstraint versionConstraint;
  private final NonExistenceResponse nonExistenceResponse;
  private final boolean verifyChecksums;
  private final boolean returnInvalidations;
  private final ForwardingMode forwardingMode;
  private final boolean updateSecondariesOnMiss;
  private final byte[] userOptions;
  private final byte[] authorizationUser;
  private final String zoneId;

  public static final byte[] noUserOptions = null;

  // for parsing only
  private static final RetrievalOptions templateOptions = OptionsHelper.newRetrievalOptions(RetrievalType.VALUE,
                                                                                            WaitMode.GET);

  static {
    ObjectDefParser2.addParser(templateOptions);
    ObjectDefParser2.addSetType(RetrievalOptions.class, "secondaryTargets", SecondaryTarget.class);
  }

  ///
  /// TODO (OPTIMUS-0000): this is C++ only.
  /// This should be removed once C++ SKRetrievalOptions.cpp is using the other constructor below properly.
  ///
  public RetrievalOptions(OpTimeoutController opTimeoutController,
                          Set<SecondaryTarget> secondaryTargets,
                          RetrievalType retrievalType,
                          WaitMode waitMode,
                          VersionConstraint versionConstraint,
                          NonExistenceResponse nonExistenceResponse,
                          boolean verifyChecksums,
                          boolean returnInvalidations,
                          ForwardingMode forwardingMode,
                          boolean updateSecondariesOnMiss) {
    this(
        opTimeoutController,
        secondaryTargets,
        retrievalType,
        waitMode,
        versionConstraint,
        nonExistenceResponse,
        verifyChecksums,
        returnInvalidations,
        forwardingMode,
        updateSecondariesOnMiss,
        RetrievalOptions.noUserOptions,
        RetrievalOptions.noAuthorizationUser);
  }

  public RetrievalOptions(OpTimeoutController opTimeoutController,
                          Set<SecondaryTarget> secondaryTargets,
                          RetrievalType retrievalType,
                          WaitMode waitMode,
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
        DHTConstants.defaultTraceIDProvider,
        AllReplicasExcludedResponse.defaultResponse,
        retrievalType,
        waitMode,
        versionConstraint,
        nonExistenceResponse,
        verifyChecksums,
        returnInvalidations,
        forwardingMode,
        updateSecondariesOnMiss,
        userOptions,
        authorizationUser);
  }

  public RetrievalOptions(OpTimeoutController opTimeoutController,
                          Set<SecondaryTarget> secondaryTargets,
                          RetrievalType retrievalType,
                          WaitMode waitMode,
                          VersionConstraint versionConstraint,
                          NonExistenceResponse nonExistenceResponse,
                          boolean verifyChecksums,
                          boolean returnInvalidations,
                          ForwardingMode forwardingMode,
                          boolean updateSecondariesOnMiss,
                          byte[] userOptions,
                          byte[] authorizationUser, String zoneId) {
    this(
        opTimeoutController,
        secondaryTargets,
        DHTConstants.defaultTraceIDProvider,
        AllReplicasExcludedResponse.defaultResponse,
        retrievalType,
        waitMode,
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
   * Construct a fully-specified RetrievalOptions.
   * Usage should be avoided; an instance should be obtained and modified from an enclosing environment.
   *
   * @param opTimeoutController     opTimeoutController for the operation
   * @param secondaryTargets        constrains queried secondary replicas
   *                                to operation solely on the node that receives this operation
   * @param traceIDProvider         trace provider for message group
   * @param retrievalType           type of retrieval
   * @param waitMode                whether to perform a WaitFor or a Get
   * @param versionConstraint       specify the version
   * @param nonExistenceResponse    action to perform for non-existent keys
   * @param verifyChecksums         whether or not to verify checksums
   * @param returnInvalidations     normally false, true causes invalidated values to be returned.
   *                                only valid for META_DATA retrievals
   * @param forwardingMode          FORWARD is for normal operation. DO_NOT_FORWARD restricts the retrieval
   *                                to the receiving node
   * @param updateSecondariesOnMiss update secondary replicas when a value is not found at the
   *                                replica, but is found at the primary
   * @param userOptions             additional space for user defined options which may be required by custom handlers
   * @param authorizationUser       a username which may be required by an authorization plugin on the server
   */
  public RetrievalOptions(OpTimeoutController opTimeoutController,
                          Set<SecondaryTarget> secondaryTargets,
                          TraceIDProvider traceIDProvider,
                          AllReplicasExcludedResponse allReplicasExcludedResponse,
                          RetrievalType retrievalType,
                          WaitMode waitMode,
                          VersionConstraint versionConstraint,
                          NonExistenceResponse nonExistenceResponse,
                          boolean verifyChecksums,
                          boolean returnInvalidations,
                          ForwardingMode forwardingMode,
                          boolean updateSecondariesOnMiss,
                          byte[] userOptions,
                          byte[] authorizationUser,
                          String zoneId) {
    super(opTimeoutController, secondaryTargets, traceIDProvider, allReplicasExcludedResponse);
    if (waitMode == WaitMode.WAIT_FOR) {
      if (!(opTimeoutController instanceof WaitForTimeoutController)) {
        throw new IllegalArgumentException(
            "opTimeoutController must be an a descendant of WaitForTimeoutController for WaitFor operations");
      }
    }
    Preconditions.checkNotNull(retrievalType);
    Preconditions.checkNotNull(waitMode);
    Preconditions.checkNotNull(versionConstraint);
    Preconditions.checkNotNull(nonExistenceResponse);
    this.retrievalType = retrievalType;
    this.waitMode = waitMode;
    this.versionConstraint = versionConstraint;
    this.nonExistenceResponse = nonExistenceResponse;
    this.verifyChecksums = verifyChecksums;
    this.returnInvalidations = returnInvalidations;
    if (returnInvalidations && (retrievalType != RetrievalType.META_DATA && retrievalType != RetrievalType.EXISTENCE)) {
      throw new IllegalArgumentException("returnInvalidations is incompatible with " + retrievalType);
    }
    this.forwardingMode = forwardingMode;
    this.updateSecondariesOnMiss = updateSecondariesOnMiss;
    this.userOptions = userOptions;
    this.authorizationUser = authorizationUser;
    this.zoneId = zoneId;
  }

  public RetrievalOptions(OpTimeoutController opTimeoutController,
                          Set<SecondaryTarget> secondaryTargets,
                          TraceIDProvider traceIDProvider,
                          AllReplicasExcludedResponse allReplicasExcludedResponse,
                          RetrievalType retrievalType,
                          WaitMode waitMode,
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
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new opTimeoutController.
   *
   * @param opTimeoutController the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions opTimeoutController(OpTimeoutController opTimeoutController) {
    return new RetrievalOptions(
        opTimeoutController,
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new secondaryTargets.
   *
   * @param secondaryTargets the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        secondaryTargets,
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new secondaryTargets.
   *
   * @param secondaryTarget the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions secondaryTargets(SecondaryTarget secondaryTarget) {
    Preconditions.checkNotNull(secondaryTarget);
    return new RetrievalOptions(
        getOpTimeoutController(),
        ImmutableSet.of(secondaryTarget),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new traceIDProvider.
   *
   * @param traceIDProvider the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions traceIDProvider(TraceIDProvider traceIDProvider) {
    Preconditions.checkNotNull(traceIDProvider);
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        traceIDProvider,
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new retrievalType.
   *
   * @param retrievalType the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions retrievalType(RetrievalType retrievalType) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new waitMode.
   *
   * @param waitMode the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions waitMode(WaitMode waitMode) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new versionConstraint.
   *
   * @param versionConstraint the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions versionConstraint(VersionConstraint versionConstraint) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new nonExistenceResponse.
   *
   * @param nonExistenceResponse the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions nonExistenceResponse(NonExistenceResponse nonExistenceResponse) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new verifyChecksums.
   *
   * @param verifyChecksums the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions verifyChecksums(boolean verifyChecksums) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new returnInvalidations.
   *
   * @param returnInvalidations the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions returnInvalidations(boolean returnInvalidations) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new forwardingMode.
   *
   * @param forwardingMode the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions forwardingMode(ForwardingMode forwardingMode) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new updateSecondariesOnMiss.
   *
   * @param updateSecondariesOnMiss the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions updateSecondariesOnMiss(boolean updateSecondariesOnMiss) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new userOptions.
   *
   * @param userOptions the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions userOptions(byte[] userOptions) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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
   * Return a RetrievalOptions instance like this instance, but with a new authorizationUser.
   *
   * @param authorizationUser the new field value
   * @return the modified RetrievalOptions
   */
  public RetrievalOptions authorizationUser(byte[] authorizationUser) {
    return new RetrievalOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        getAllReplicasExcludedResponse(),
        retrievalType,
        waitMode,
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

  public RetrievalOptions zoneId(String zoneId) {
    return new RetrievalOptions(getOpTimeoutController(),
                                getSecondaryTargets(),
                                getTraceIDProvider(),
                                getAllReplicasExcludedResponse(),
                                retrievalType,
                                waitMode,
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
   * Return retrievalType
   *
   * @return retrievalType
   */
  public RetrievalType getRetrievalType() {
    return retrievalType;
  }

  /**
   * Return waitMode
   *
   * @return waitMode
   */
  public final WaitMode getWaitMode() {
    return waitMode;
  }

  /**
   * Return versionConstraint
   *
   * @return versionConstraint
   */
  public final VersionConstraint getVersionConstraint() {
    return versionConstraint;
  }

  /**
   * Return nonexistenceResponse
   *
   * @return nonexistenceResponse
   */
  public final NonExistenceResponse getNonExistenceResponse() {
    return nonExistenceResponse;
  }

  /**
   * Return verifyChecksums
   *
   * @return verifyChecksums
   */
  public boolean getVerifyChecksums() {
    return verifyChecksums;
  }

  /**
   * Return returnInvalidations
   *
   * @return returnInvalidations
   */
  public boolean getReturnInvalidations() {
    return returnInvalidations;
  }

  /**
   * Return forwardingMode
   *
   * @return forwardingMode
   */
  public ForwardingMode getForwardingMode() {
    return forwardingMode;
  }

  /**
   * Return updateSecondariesOnMiss
   *
   * @return updateSecondariesOnMiss
   */
  public boolean getUpdateSecondariesOnMiss() {
    return updateSecondariesOnMiss;
  }

  /**
   * Return userOptions
   *
   * @return userOptions
   */
  public byte[] getUserOptions() {
    return userOptions;
  }

  /**
   * Return authorizationUser
   *
   * @return authorizationUser
   */
  public byte[] getAuthorizationUser() {
    return authorizationUser;
  }

  public String getZoneId() {
    return zoneId;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^
           retrievalType.hashCode() ^
           waitMode.hashCode() ^
           versionConstraint.hashCode() ^
           nonExistenceResponse.hashCode() ^
           Boolean.hashCode(verifyChecksums) ^
           Boolean.hashCode(returnInvalidations) ^
           forwardingMode.hashCode() ^
           Boolean.hashCode(updateSecondariesOnMiss) ^
           Arrays.hashCode(userOptions) ^
           Arrays.hashCode(authorizationUser);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else {
      RetrievalOptions oRetrievalOptions;

      oRetrievalOptions = (RetrievalOptions) other;
      return this.retrievalType == oRetrievalOptions.retrievalType &&
             this.waitMode == oRetrievalOptions.waitMode &&
             this.versionConstraint.equals(oRetrievalOptions.versionConstraint) &&
             this.nonExistenceResponse == oRetrievalOptions.nonExistenceResponse &&
             this.verifyChecksums == oRetrievalOptions.verifyChecksums &&
             this.returnInvalidations == oRetrievalOptions.returnInvalidations &&
             this.forwardingMode == oRetrievalOptions.forwardingMode &&
             this.updateSecondariesOnMiss == oRetrievalOptions.updateSecondariesOnMiss &&
             Arrays.equals(this.userOptions, oRetrievalOptions.userOptions) &&
             Arrays.equals(this.authorizationUser, oRetrievalOptions.authorizationUser) &&
             super.equals(other);
    }
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  /*
   * Parse a RetrievalOptions definition
   * @param def a RetrievalOptions definition
   * @return a parsed RetrievalOptions instance
   */
  public static RetrievalOptions parse(String def) {
    return ObjectDefParser2.parse(RetrievalOptions.class, def);
  }
}
