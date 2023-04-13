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

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.ConsistencyProtocol;
import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.InvalidationOptions;
import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.NamespacePerspectiveOptions;
import com.ms.silverking.cloud.dht.NamespaceVersionMode;
import com.ms.silverking.cloud.dht.PutOptions;
import com.ms.silverking.cloud.dht.StorageType;
import com.ms.silverking.cloud.dht.client.AbsMillisVersionProvider;
import com.ms.silverking.cloud.dht.client.ChecksumType;
import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.KeyDigestType;
import com.ms.silverking.cloud.dht.client.impl.NamespaceCreator;
import com.ms.silverking.cloud.dht.client.impl.SimpleNamespaceCreator;
import com.ms.silverking.cloud.dht.ForwardingMode;
import com.ms.silverking.numeric.NumConversion;

public class NamespaceUtil {
  private static final NamespaceCreator creator = new SimpleNamespaceCreator();

  public static final String reservedNamespacePrefix = "__"; // All reserved namespaces must start with this. Users
  // may not use namespaces that begin with this
  public static final String metaInfoNamespaceName = "__DHT_Meta__"; // Current dir name 273d6df499e32426
  public static final Namespace metaInfoNamespace = creator.createNamespace(metaInfoNamespaceName);
  public static final GetOptions metaNSDefaultGetOptions = DHTConstants.standardGetOptions.forwardingMode(ForwardingMode.ALL); // Required to bootstrap lost replicas
  public static final PutOptions metaNSDefaultPutOptions = new PutOptions(DHTConstants.standardTimeoutController,
                                                                          DHTConstants.noSecondaryTargets,
                                                                          Compression.NONE,
                                                                          ChecksumType.MD5,
                                                                          false,
                                                                          PutOptions.defaultVersion,
                                                                          PutOptions.noVersionRequired,
                                                                          PutOptions.noLock,
                                                                          DHTConstants.defaultFragmentationThreshold,
                                                                          null,
                                                                          PutOptions.noAuthorizationUser);
  public static final InvalidationOptions metaNSDefaultInvalidationOptions = OptionsHelper.newInvalidationOptions(
      DHTConstants.standardTimeoutController, PutOptions.defaultVersion, PutOptions.noVersionRequired,
      PutOptions.noLock, DHTConstants.noSecondaryTargets);
  public static final NamespaceOptions metaInfoNamespaceOptions = OptionsHelper.newNamespaceOptions(StorageType.FILE,
                                                                                                    ConsistencyProtocol.TWO_PHASE_COMMIT,
                                                                                                    NamespaceVersionMode.SINGLE_VERSION,
                                                                                                    metaNSDefaultPutOptions,
                                                                                                    metaNSDefaultInvalidationOptions,
                                                                                                    metaNSDefaultGetOptions,
                                                                                                    DHTConstants.standardWaitOptions)
                                                                               .asWriteOnce();
  // meta info ns must be write once currently because convergence only supports
  // SINGLE_VERSION currently
  // This implies that namespace options of a namespace can never change.
  // That's probably a sensible invariant to require anyway.
  public static final NamespaceProperties metaInfoNamespaceProperties = new NamespaceProperties(metaInfoNamespaceOptions);
  public static final Map<Long, NamespaceProperties> systemNamespaceProperties;

  static {
    SimpleNamespaceCreator nsCreator;

    nsCreator = new SimpleNamespaceCreator();
    systemNamespaceProperties = new HashMap<>();
    systemNamespaceProperties.put(nsCreator.createNamespace(com.ms.silverking.cloud.dht.client.Namespace.systemName).contextAsLong(),
                                  new NamespaceProperties(DHTConstants.dynamicNamespaceOptions));
    systemNamespaceProperties.put(nsCreator.createNamespace(com.ms.silverking.cloud.dht.client.Namespace.nodeName).contextAsLong(),
                                  new NamespaceProperties(DHTConstants.dynamicNamespaceOptions));
    systemNamespaceProperties.put(nsCreator.createNamespace(com.ms.silverking.cloud.dht.client.Namespace.replicasName).contextAsLong(),
                                  new NamespaceProperties(DHTConstants.dynamicNamespaceOptions));
  }

  public static final NamespacePerspectiveOptions<String, String> metaNSPOptions = new NamespacePerspectiveOptions<>(
      String.class, String.class, KeyDigestType.MD5, metaInfoNamespaceOptions.getDefaultPutOptions(),
      metaInfoNamespaceOptions.getDefaultInvalidationOptions(), metaInfoNamespaceOptions.getDefaultGetOptions(),
      metaInfoNamespaceOptions.getDefaultWaitOptions(), new AbsMillisVersionProvider(SystemTimeUtil.skSystemTimeSource),
      null);

  public static long nameToContext(String name) {
    return new SimpleNamespaceCreator().createNamespace(name).contextAsLong();
  }

  public static String nameToDirName(String name) {
    return Long.toHexString(nameToContext(name));
  }

  public static String contextToDirName(long context) {
    return Long.toHexString(context);
  }

  public static long dirNameToContext(String dirName) {
    return NumConversion.parseHexStringAsUnsignedLong(dirName);
  }

  public static boolean canMutateWith(NamespaceOptions oldOpts, NamespaceOptions newOpts) {
    if (oldOpts == newOpts) {
      return true;
    }

    /* The following selective fields are mutable, so they are waived from equality check:
     *  - secondarySyncIntervalSeconds
     *  - valueRetentionPolicy
     *  - defaultPutOptions
     *  - defaultInvalidationOptions
     *  - defaultGetOptions
     *  - defaultWaitOptions
     *  - namespaceServerSideCode
     */

    // The following fields are immutable, so we still need to check its equality
    return oldOpts.getStorageType() == newOpts.getStorageType() &&
           oldOpts.getConsistencyProtocol() == newOpts.getConsistencyProtocol() &&
           oldOpts.getVersionMode() == newOpts.getVersionMode() &&
           oldOpts.getRevisionMode() == newOpts.getRevisionMode() &&
           oldOpts.getSegmentSize() == newOpts.getSegmentSize() &&
           oldOpts.getMaxValueSize() == newOpts.getMaxValueSize() &&
           oldOpts.getAllowLinks() == newOpts.getAllowLinks();
  }
}
