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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.ms.silverking.SKConstants;
import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.NamespaceCreationOptions.Mode;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.RevisionMode;
import com.ms.silverking.cloud.dht.SecondaryTarget;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.WaitOptions;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.OpSizeBasedTimeoutController;
import com.ms.silverking.cloud.dht.client.OpTimeoutController;
import com.ms.silverking.cloud.dht.client.WaitForTimeoutController;
import com.ms.silverking.cloud.dht.client.crypto.AESEncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.EncrypterDecrypter;
import com.ms.silverking.cloud.dht.client.crypto.XOREncrypterDecrypter;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.cloud.dht.meta.ClassVars;
import com.ms.silverking.cloud.dht.trace.TraceIDProvider;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;

/** DHTConstants for internal use. Not exposed to clients. */
public class DHTConstants {
  public static final long noSuchVersion = Long.MIN_VALUE;
  public static final long unspecifiedVersion = 0;

  public static final byte[] emptyByteArray = new byte[0];
  public static final ByteBuffer emptyByteBuffer = ByteBuffer.wrap(emptyByteArray);

  public static final StorageType defaultStorageType = StorageType.FILE;
  public static final ConsistencyProtocol defaultConsistencyProtocol =
      ConsistencyProtocol.TWO_PHASE_COMMIT;
  public static final NamespaceVersionMode defaultVersionMode = NamespaceVersionMode.SINGLE_VERSION;
  public static final RevisionMode defaultRevisionMode = RevisionMode.NO_REVISIONS;
  public static final String defaultStorageFormat = "0";
  public static final int defaultSecondarySyncIntervalSeconds = 30 * 60;
  public static final int defaultSecondaryReplicaUpdateTimeoutMillis = 2 * 60 * 1000;

  public static final int defaultMinPrimaryUnderFailure = 1;
  public static final int defaultMinFinalizationIntervalMillis = 12 * 60 * 60 * 1000;
  public static final boolean defaultEnablePendingPuts = true;
  public static final long defaultMaxUnfinalizedDeletedBytes = 10L * 1024L * 1024L * 1024L;

  public static final int minSegmentSize = 2 * 1024;
  public static final int defaultSegmentSize = 64 * 1024 * 1024;
  public static final int segmentSafetyMargin = 1 * 1024;

  public static final int defaultFragmentationThreshold = 10 * 1024 * 1024;
  public static final int minFragmentationThreshold = 1 * 1024;

  // NOTE: max value size for a single value in namespace cannot exceeds 2GB (due to integer limit)
  public static final int defaultMaxValueSize = 1 * 1024 * 1024 * 1024; // 1GB

  public static final int noCapacityLimit = -1;

  public static final String systemClassBase = "com.ms.silverking";
  public static final String daemonPackageBase = systemClassBase + ".cloud.dht.daemon";

  public static final String heapDumpOnOOMProp = systemClassBase + ".heapDumpOnOOM";

  public static final String daemonLogFile = "Daemon.out";
  public static final String stopSKFSLogFile = "StopSKFS.out";
  public static final String checkSKFSLogFile = "CheckSKFS.out";
  public static final String prevDaemonLogFile = "Daemon.out.1";
  public static final String heapDumpFile = "DHTNode.heap";

  public static final String rawOptionsVar = "rawOptions";
  public static final String ipAliasMapFileVar = "skIPAliasMapFile";
  public static final String ipAliasMapFileEnvVar = "skIPAliasMapFile";
  public static final String dataBasePathProperty = daemonPackageBase + ".DataBasePath";

  public static final String fileSegmentCacheCapacityProperty =
      daemonPackageBase + ".FileSegmentCacheCapacity";

  public static final String retrievalImplementationProperty =
      daemonPackageBase + ".RetrievalImplementation";

  public static final String segmentIndexLocationProperty =
      daemonPackageBase + ".SegmentIndexLocation";

  public static final String nsPrereadGBProperty = daemonPackageBase + ".NSPrereadGB";
  public static final String minFinalizationIntervalMillisProperty =
      daemonPackageBase + ".MinFinalizationIntervalMillis";
  public static final String enablePendingPutsProperty = daemonPackageBase + ".EnablePendingPuts";
  public static final String maxUnfinalizedDeletedBytesProperty =
      daemonPackageBase + ".MaxUnfinalizedDeletedBytes";
  public static final String verboseReapLogInfoProperty = daemonPackageBase + ".VerboseReapLogInfo";
  public static final String defaultCompactionThresholdProperty =
      daemonPackageBase + ".DefaultCompactionThreshold";
  public static final String forceDataSegmentLoadOnReapProperty =
      daemonPackageBase + ".ForceDataSegmentLoadOnReap";

  public static final String verboseGlobalFinalizationProperty =
      systemClassBase + ".VerboseGlobalFinalization";
  public static final String jvmMonitorMaxIntervalMillisProperty =
      systemClassBase + ".JvmMonitorMaxIntervalMillis";
  public static final String jvmMonitorMinIntervalMillisProperty =
      systemClassBase + ".JvmMonitorMinIntervalMillis";
  public static final String internalRelTimeoutMillisProp =
      systemClassBase + ".InternalRelTimeoutMillisProp";

  public static final String exclusionSetRetainedVersionsProperty =
      systemClassBase + ".ExclusionSetRetainedVersions";
  public static final String onSelfExclusionProperty = daemonPackageBase + ".OnSelfExclusion";

  public static final String zookeeperSessionTimeoutProperty =
      systemClassBase + ".ZookeeperSessionTimeout";

  public static final String ssSubDirName = "ss";
  public static final String ssTempSubDirName = "ssTemp";

  public static final String classpathEnv = "SK_CLASSPATH";
  public static final String classpathProperty = "java.class.path";
  public static final String jaceHomeEnv = "SK_JACE_HOME";
  public static final String defaultNamespaceOptionsModeEnv = "SK_DEFAULT_NS_OPTIONS_MODE";
  public static final String defaultEnableMsgGroupTraceEnv = "SK_ENABLE_MSG_GROUP_TRACE";
  public static final String defaultEnableMsgGroupRecorderEnv = "SK_ENABLE_MSG_GROUP_RECORDER";
  public static final String defaultEnableMsgThrottling = "SK_ENABLE_THROTTLING";
  public static final String javaHomeEnv = SKConstants.javaHomeEnv;
  public static final String javaHomeProperty = "java.home";

  public static final String dataBasePathDelimiter = "%%";

  public static boolean isDaemon = false;

  public static String noZoneId = "";

  public static String aliasMapFile =
      PropertiesHelper.envHelper.getString(ipAliasMapFileEnvVar, "");

  public static Boolean getHeapDumpOnOutOfMemory() {
    return Boolean.parseBoolean(System.getProperty(heapDumpOnOOMProp, "true"));
  }

  public static final Set<SecondaryTarget> noSecondaryTargets = null;
  public static final TraceIDProvider defaultTraceIDProvider = TraceIDProvider.noTraceIDProvider;
  public static final OpTimeoutController standardTimeoutController =
      new OpSizeBasedTimeoutController();
  public static final OpTimeoutController standardWaitForTimeoutController =
      new WaitForTimeoutController();
  /**
   * Standard PutOptions. Subject to change. Recommended practice is for each SilverKing instance to
   * specify an instance default (using
   * NamespaceCreationOptions.defaultNSOptions.defaultPutOptions.)
   */
  public static final PutOptions standardPutOptions =
      new PutOptions(
          standardTimeoutController,
          noSecondaryTargets,
          Compression.LZ4,
          ChecksumType.MURMUR3_32,
          false,
          PutOptions.defaultVersion,
          PutOptions.noVersionRequired,
          PutOptions.noLock,
          defaultFragmentationThreshold,
          null,
          PutOptions
              .noAuthorizationUser); // FUTURE - FOR NOW THIS MUST BE REPLICATED IN PUT OPTIONS,
  // parse limitation

  public static final InvalidationOptions standardInvalidationOptions =
      OptionsHelper.newInvalidationOptions(
          standardTimeoutController,
          InvalidationOptions.defaultVersion,
          PutOptions.noVersionRequired,
          PutOptions.noLock,
          noSecondaryTargets);
  public static final GetOptions standardGetOptions =
      OptionsHelper.newGetOptions(
          standardTimeoutController, RetrievalType.VALUE, VersionConstraint.defaultConstraint);
  public static final WaitOptions standardWaitOptions =
      OptionsHelper.newWaitOptions(
          RetrievalType.VALUE,
          VersionConstraint.defaultConstraint,
          Integer.MAX_VALUE,
          WaitOptions.THRESHOLD_MAX);

  public static final NamespaceOptions defaultNamespaceOptions =
      OptionsHelper.newNamespaceOptions(
          defaultStorageType,
          defaultConsistencyProtocol,
          defaultVersionMode,
          defaultRevisionMode,
          standardPutOptions,
          standardInvalidationOptions,
          standardGetOptions,
          standardWaitOptions,
          defaultSecondarySyncIntervalSeconds,
          defaultSegmentSize,
          defaultMaxValueSize);
  public static final NamespaceOptions dynamicNamespaceOptions =
      defaultNamespaceOptions
          .storageType(StorageType.RAM)
          .consistencyProtocol(ConsistencyProtocol.TWO_PHASE_COMMIT)
          .versionMode(NamespaceVersionMode.CLIENT_SPECIFIED);
  public static final NamespaceProperties dynamicNamespaceProperties =
      new NamespaceProperties(dynamicNamespaceOptions);
  public static final NamespaceProperties metricsNamespaceProperties = dynamicNamespaceProperties;

  public static final Class<String> defaultKeyClass = String.class;
  public static final Class<byte[]> defaultValueClass = byte[].class;

  /**
   * Default NamespaceCreationOptions. Subject to change. Recommended practice is for each
   * SilverKing instance to explicitly specify.
   */
  public static final NamespaceCreationOptions defaultNamespaceCreationOptions =
      new NamespaceCreationOptions(
          Mode.OptionalAutoCreation_AllowMatches, "^_.*", defaultNamespaceOptions);

  // default encryption
  public static final String defaultEncrypterDecrypterProperty =
      systemClassBase + ".DefaultEncrypterDecrypter";
  public static final EncrypterDecrypter defaultDefaultEncrypterDecrypter = null;
  public static final EncrypterDecrypter defaultEncrypterDecrypter;

  static {
    String val;

    val =
        PropertiesHelper.systemHelper.getString(
            defaultEncrypterDecrypterProperty, UndefinedAction.ZeroOnUndefined);
    if (val == null) {
      defaultEncrypterDecrypter = defaultDefaultEncrypterDecrypter;
    } else {
      try {
        if (val.equals(AESEncrypterDecrypter.name)
            || val.equals(AESEncrypterDecrypter.class.getName())) {
          defaultEncrypterDecrypter = new AESEncrypterDecrypter();
        } else if (val.equals(XOREncrypterDecrypter.name)
            || val.equals(XOREncrypterDecrypter.class.getName())) {
          defaultEncrypterDecrypter = new XOREncrypterDecrypter();
        } else {
          throw new RuntimeException("Unknown EncrypterDecrypter: " + val);
        }
      } catch (Exception e) {
        throw new RuntimeException("Exception initializing EncrypterDecrypter", e);
      }
    }
  }

  public static final int noPortOverride = -1;
  public static final int uninitializedPort = -1;
}
