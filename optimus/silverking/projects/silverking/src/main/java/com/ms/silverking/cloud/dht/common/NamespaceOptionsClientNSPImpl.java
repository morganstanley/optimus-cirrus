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

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import com.ms.silverking.cloud.dht.RetrievalType;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.daemon.storage.NamespacePropertiesIO;

public class NamespaceOptionsClientNSPImpl extends NamespaceOptionsClientBase {
  private static final String implName = "MetaNamespaceStore";

  private final SynchronousNamespacePerspective<String, String> syncNSP;
  private final SessionEstablishmentTimeoutController seTimeoutController;

  public NamespaceOptionsClientNSPImpl(
      DHTSession session,
      ClientDHTConfigurationProvider dhtConfigProvider,
      SessionEstablishmentTimeoutController seTimeoutController) {
    super(dhtConfigProvider);
    this.syncNSP =
        session.openSyncNamespacePerspective(
            NamespaceUtil.metaInfoNamespaceName, NamespaceUtil.metaNSPOptions);
    this.seTimeoutController = seTimeoutController;
  }

  public NamespaceOptionsClientNSPImpl(
      DHTSession session, ClientDHTConfigurationProvider dhtConfigProvider) {
    this(session, dhtConfigProvider, SessionOptions.getDefaultTimeoutController());
  }

  @Override
  protected long getDefaultRelTimeoutMillis() {
    return seTimeoutController.getMaxRelativeTimeoutMillis(null);
  }

  @Override
  protected void putNamespaceProperties(long nsContext, NamespaceProperties nsProperties)
      throws NamespacePropertiesPutException {
    try {
      if (debug) {
        System.out.printf("putNamespaceProperties(%x, %s)\n", nsContext, nsProperties);
      }
      // Legacy format is used for NSP (for now still needs "properties" file)
      syncNSP.put(getOptionsKey(nsContext), nsProperties.toLegacySKDef());
      if (debug) {
        System.out.println("Done storeNamespaceOptions");
      }
    } catch (PutException pe) {
      throw new NamespacePropertiesPutException(pe);
    }
  }

  private String getOptionsKey(long context) {
    return Long.toString(context);
  }

  @Override
  protected NamespaceProperties retrieveFullNamespaceProperties(long nsContext)
      throws NamespacePropertiesRetrievalException {
    StoredValue<String> storedDef;

    try {
      if (debug) {
        System.out.printf(
            "%s::retrieveFullNamespaceProperties(%x)\n", implementationName(), nsContext);
      }
      storedDef =
          syncNSP.retrieve(
              getOptionsKey(nsContext),
              syncNSP
                  .getOptions()
                  .getDefaultGetOptions()
                  .retrievalType(RetrievalType.VALUE_AND_META_DATA));
      if (debug) {
        System.out.printf(
            "%s::retrieveFullNamespaceProperties(%x) complete %s\n",
            implementationName(), nsContext, storedDef);
      }
      if (storedDef != null) {
        return NamespaceProperties.parse(
            storedDef.getValue(), storedDef.getCreationTime().inNanos());
      } else {
        return null;
      }
    } catch (RetrievalException re) {
      throw new NamespacePropertiesRetrievalException(re);
    }
  }

  @Override
  protected void deleteNamespaceProperties(long nsContext)
      throws NamespacePropertiesDeleteException {
    throw new NamespacePropertiesDeleteException(
        "Deletion for ns [" + nsContext + "] is not supported in [" + implementationName() + "]");
  }

  @Override
  protected String implementationName() {
    return implName;
  }

  @Override
  public Optional<NamespaceProperties> getNsPropertiesForRecovery(File nsDir)
      throws NamespacePropertiesRetrievalException {
    try {
      // This implementation will still need "properties" file for bootstrap in recovery
      return Optional.of(NamespacePropertiesIO.read(nsDir));
    } catch (IOException ioe) {
      throw new NamespacePropertiesRetrievalException(ioe);
    }
  }
}
