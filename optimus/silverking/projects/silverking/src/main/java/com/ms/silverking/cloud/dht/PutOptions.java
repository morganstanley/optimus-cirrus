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
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;

/**
 * Options for Put operations.
 */
public class PutOptions extends OperationOptions {
  private final Compression compression;
  private final ChecksumType checksumType;
  private final boolean checksumCompressedValues;
  private final long version;
  private final long requiredPreviousVersion;
  private final short lockSeconds;
  private final int fragmentationThreshold;
  private final byte[] userData;
  private final byte[] authorizationUser;

  public static final byte[] noAuthorizationUser = null;
  /**
   * Use the default version of the NamespacePerspective
   */
  public static final long defaultVersion = DHTConstants.unspecifiedVersion;
  /**
   * Specify that no requiredPreviousVersion is needed
   */
  public static final long noVersionRequired = DHTConstants.unspecifiedVersion;
  /**
   * Value for requiredPreviousVersion that specifies that this must be the first version/value associated with the key
   */
  public static final long previousVersionNonexistent = -1;
  /**
   * Value for requiredPreviousVersion that specifies that either this must be the first version/value associated
   * with the key,
   * or the previous version must be an invalidation
   */
  public static final long previousVersionNonexistentOrInvalid = -2;
  /**
   * Value for lockSeconds specifying no lock
   */
  public static final short noLock = 0;

  // begin temp replicated from DHTConstants
  // FUTURE - A limitation in the parser seems to require replication until we have a proper fix
  private static final OpTimeoutController standardTimeoutController = new OpSizeBasedTimeoutController();
  private static final PutOptions template = new PutOptions();
  // end temp replicated from DHTConstants

  static {
    ObjectDefParser2.addParser(template, FieldsRequirement.ALLOW_INCOMPLETE);
    ObjectDefParser2.addSetType(PutOptions.class, "secondaryTargets", SecondaryTarget.class);
  }

  public PutOptions(OpTimeoutController opTimeoutController,
                    Set<SecondaryTarget> secondaryTargets,
                    Compression compression,
                    ChecksumType checksumType,
                    boolean checksumCompressedValues,
                    long version,
                    long requiredPreviousVersion,
                    short lockSeconds,
                    int fragmentationThreshold,
                    byte[] userData,
                    byte[] authorizationUser) {
    this(
        opTimeoutController,
        secondaryTargets,
        DHTConstants.defaultTraceIDProvider,
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  public PutOptions(OpTimeoutController opTimeoutController,
                    Set<SecondaryTarget> secondaryTargets,
                    TraceIDProvider traceIDProvider,
                    Compression compression,
                    ChecksumType checksumType,
                    boolean checksumCompressedValues,
                    long version,
                    long requiredPreviousVersion,
                    short lockSeconds,
                    int fragmentationThreshold,
                    byte[] userData,
                    byte[] authorizationUser) {
    this(
        opTimeoutController,
        secondaryTargets,
        traceIDProvider,
        AllReplicasExcludedResponse.EXCEPTION,
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  private PutOptions() {
    this(
        standardTimeoutController,
        DHTConstants.noSecondaryTargets,
        DHTConstants.defaultTraceIDProvider,
        AllReplicasExcludedResponse.defaultResponse,
        Compression.LZ4,
        ChecksumType.MURMUR3_32,
        false,
        defaultVersion,
        noVersionRequired,
        noLock,
        DHTConstants.defaultFragmentationThreshold,
        null,
        null);
  }

  /**
   * Construct PutOptions from the given arguments. Usage is generally not recommended.
   * Instead of using this constructor, applications should obtain an instance
   * from a valid source such as the Session, the Namespace, or the NamespacePerspective.
   *
   * @param opTimeoutController      opTimeoutController for the operation
   * @param secondaryTargets         constrains queried secondary replicas
   *                                 to operation solely on the node that receives this operation
   * @param traceIDProvider          trace provider for message group
   * @param compression              type of compression to use
   * @param checksumType             checksum to use for value
   * @param checksumCompressedValues controls whether or not compressed values are checksummed
   * @param version                  version to use for a Put operation. Using defaultVersion will allow the
   *                                 version mode
   *                                 to set this automatically.
   * @param requiredPreviousVersion  latest version must match this value for put to succeed
   * @param lockSeconds              seconds to lock this key
   * @param fragmentationThreshold   values longer than this threshold will be fragmented
   * @param userData                 out of band data to store with value. May not exceed 255.
   * @param authorizationUser        a username which may be required by an authorization plugin on the server
   */
  public PutOptions(OpTimeoutController opTimeoutController,
                    Set<SecondaryTarget> secondaryTargets,
                    TraceIDProvider traceIDProvider,
                    AllReplicasExcludedResponse allReplicasExcludedResponse,
                    Compression compression,
                    ChecksumType checksumType,
                    boolean checksumCompressedValues,
                    long version,
                    long requiredPreviousVersion,
                    short lockSeconds,
                    int fragmentationThreshold,
                    byte[] userData,
                    byte[] authorizationUser) {
    super(opTimeoutController, secondaryTargets, traceIDProvider, allReplicasExcludedResponse);
    Preconditions.checkNotNull(compression);
    Preconditions.checkNotNull(checksumType);
    if (version < 0) {
      throw new IllegalArgumentException("version < 0");
    }
    // We can't completely check here as we can have invalid combinations of
    // values during construction; we quick check upon use
    if (lockSeconds < 0) {
      throw new IllegalArgumentException("lockSeconds < 0");
    }
    if (fragmentationThreshold < DHTConstants.minFragmentationThreshold) {
      throw new IllegalArgumentException("fragmentationThreshold < DHTConstants.minFragmentationThreshold");
    }
    this.compression = compression;
    this.version = version;
    this.requiredPreviousVersion = requiredPreviousVersion;
    this.lockSeconds = lockSeconds;
    this.fragmentationThreshold = fragmentationThreshold;
    this.userData = userData;
    this.checksumType = checksumType;
    this.checksumCompressedValues = checksumCompressedValues;
    this.authorizationUser = authorizationUser;
  }

  /**
   * Return a PutOptions instance like this instance, but with a new OpTimeoutController.
   *
   * @param opTimeoutController OpTimeoutController to use
   * @return the modified PutOptions instance
   */
  public PutOptions opTimeoutController(OpTimeoutController opTimeoutController) {
    return new PutOptions(
        opTimeoutController,
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return an PutOptions instance like this instance, but with a new secondaryTargets.
   *
   * @param secondaryTargets the new field value
   * @return the modified InvalidationOptions
   */
  public PutOptions secondaryTargets(Set<SecondaryTarget> secondaryTargets) {
    return new PutOptions(
        getOpTimeoutController(),
        secondaryTargets,
        getTraceIDProvider(),
        getCompression(),
        getChecksumType(),
        getChecksumCompressedValues(),
        getVersion(),
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        getUserData(),
        getAuthorizationUser());
  }

  /**
   * Return an PutOptions instance like this instance, but with a new secondaryTargets.
   *
   * @param secondaryTarget the new field value
   * @return the modified InvalidationOptions
   */
  public PutOptions secondaryTargets(SecondaryTarget secondaryTarget) {
    Preconditions.checkNotNull(secondaryTarget);
    return new PutOptions(
        getOpTimeoutController(),
        ImmutableSet.of(secondaryTarget),
        getTraceIDProvider(),
        getCompression(),
        getChecksumType(),
        getChecksumCompressedValues(),
        getVersion(),
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        getUserData(),
        getAuthorizationUser());
  }

  /**
   * Return an PutOptions instance like this instance, but with a new traceIDProvider.
   *
   * @param traceIDProvider the new field value
   * @return the modified InvalidationOptions
   */
  public PutOptions traceIDProvider(TraceIDProvider traceIDProvider) {
    Preconditions.checkNotNull(traceIDProvider);
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        traceIDProvider,
        getCompression(),
        getChecksumType(),
        getChecksumCompressedValues(),
        getVersion(),
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        getUserData(),
        getAuthorizationUser());
  }

  /**
   * Return a PutOptions instance like this instance, but with a new compression.
   *
   * @param compression type of compression to use
   * @return the modified PutOptions instance
   */
  public PutOptions compression(Compression compression) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with a new checksumType.
   *
   * @param checksumType checksum to use for value
   * @return the modified PutOptions instance
   */
  public PutOptions checksumType(ChecksumType checksumType) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with a new checksumCompressedValues.
   *
   * @param checksumCompressedValues checksumCompressedValues to use for value
   * @return the modified PutOptions instance
   */
  public PutOptions checksumCompressedValues(boolean checksumCompressedValues) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with a new version.
   *
   * @param version new version to use
   * @return a PutOptions instance like this instance, but with a new version.
   */
  public PutOptions version(long version) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with a new requiredPreviousVersion.
   *
   * @param requiredPreviousVersion new requiredPreviousVersion to use
   * @return a PutOptions instance like this instance, but with a new requiredPreviousVersion.
   */
  public PutOptions requiredPreviousVersion(long requiredPreviousVersion) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with a new lockSeconds.
   *
   * @param lockSeconds new lockSeconds to use
   * @return a PutOptions instance like this instance, but with a new lockSeconds.
   */
  public PutOptions lockSeconds(short lockSeconds) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with a new fragmentationThreshold.
   *
   * @param fragmentationThreshold new fragmentationThreshold to use
   * @return a PutOptions instance like this instance, but with a new fragmentationThreshold.
   */
  public PutOptions fragmentationThreshold(int fragmentationThreshold) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with new userData.
   *
   * @param userData new user data to use
   * @return a PutOptions instance like this instance, but with new userData.
   */
  public PutOptions userData(byte[] userData) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return a PutOptions instance like this instance, but with a new authorizationUser.
   *
   * @param authorizationUser the new field value
   * @return the modified PutOptions
   */
  public PutOptions authorizationUser(byte[] authorizationUser) {
    return new PutOptions(
        getOpTimeoutController(),
        getSecondaryTargets(),
        getTraceIDProvider(),
        compression,
        checksumType,
        checksumCompressedValues,
        version,
        requiredPreviousVersion,
        lockSeconds,
        fragmentationThreshold,
        userData,
        authorizationUser);
  }

  /**
   * Return compression
   *
   * @return compression
   */
  public Compression getCompression() {
    return compression;
  }

  /**
   * Return checksumType
   *
   * @return checksumType
   */
  public ChecksumType getChecksumType() {
    return checksumType;
  }

  /**
   * Return checksumCompressedValues
   *
   * @return checksumCompressedValues
   */
  public boolean getChecksumCompressedValues() {
    return checksumCompressedValues;
  }

  /**
   * Return version
   *
   * @return version
   */
  public long getVersion() {
    return version;
  }

  /**
   * Return requiredPreviousVersion
   *
   * @return requiredPreviousVersion
   */
  public long getRequiredPreviousVersion() {
    return requiredPreviousVersion;
  }

  /**
   * Return lockSeconds
   *
   * @return lockSeconds
   */
  public short getLockSeconds() {
    return lockSeconds;
  }

  /**
   * Return fragmentationThreshold
   *
   * @return fragmentationThreshold
   */
  public int getFragmentationThreshold() {
    return fragmentationThreshold;
  }

  /**
   * Return userData
   *
   * @return userData
   */
  public byte[] getUserData() {
    return userData;
  }

  /**
   * Return authorizationUser
   *
   * @return authorizationUser
   */
  public byte[] getAuthorizationUser() {
    return authorizationUser;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ compression.hashCode() ^ checksumType.hashCode() ^ Boolean.hashCode(
        checksumCompressedValues) ^ Long.hashCode(version) ^ Long.hashCode(requiredPreviousVersion) ^ Short.hashCode(
        lockSeconds) ^ Integer.hashCode(fragmentationThreshold) ^ Arrays.hashCode(userData);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else {
      PutOptions oPutOptions;

      oPutOptions = (PutOptions) other;
      if (!super.equals(other)) {
        return false;
      }
      if (this.userData != null) {
        if (oPutOptions.userData != null) {
          if (!Arrays.equals(this.userData, oPutOptions.userData)) {
            return false;
          }
        } else {
          return false;
        }
      } else {
        if (oPutOptions.userData != null) {
          return false;
        }
      }

      return this.compression == oPutOptions.compression &&
             this.checksumType == oPutOptions.checksumType &&
             this.checksumCompressedValues == oPutOptions.checksumCompressedValues &&
             this.version == oPutOptions.version &&
             this.requiredPreviousVersion == oPutOptions.requiredPreviousVersion &&
             this.lockSeconds == oPutOptions.lockSeconds &&
             this.fragmentationThreshold == oPutOptions.fragmentationThreshold &&
             Arrays.equals(authorizationUser, oPutOptions.authorizationUser);
    }
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  /**
   * Parse a definition
   *
   * @param def object definition in ObjectDefParser format
   * @return a parsed PutOptions instance
   */
  public static PutOptions parse(String def) {
    return ObjectDefParser2.parse(PutOptions.class, def);
  }
}
