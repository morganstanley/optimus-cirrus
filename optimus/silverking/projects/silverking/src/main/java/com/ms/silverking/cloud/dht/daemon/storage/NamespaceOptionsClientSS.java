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
package com.ms.silverking.cloud.dht.daemon.storage;

import java.io.File;
import java.util.Optional;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.NamespacePropertiesRetrievalException;
import com.ms.silverking.cloud.dht.common.TimeoutException;

public interface NamespaceOptionsClientSS {
  ////// ====== server side internal query API (take namespace context as arg) ======
  NamespaceProperties getNamespaceProperties(long nsContext) throws NamespacePropertiesRetrievalException;

  NamespaceOptions getNamespaceOptions(long nsContext) throws NamespacePropertiesRetrievalException;

  NamespaceProperties getNamespacePropertiesWithTimeout(long nsContext, long relTimeoutMillis)
      throws NamespacePropertiesRetrievalException, TimeoutException;

  // For backward compatibility, since some implementation may still need properties file to bootstrap
  Optional<NamespaceProperties> getNsPropertiesForRecovery(File nsDir) throws NamespacePropertiesRetrievalException;
}
