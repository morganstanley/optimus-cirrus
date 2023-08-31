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
package com.ms.silverking.cloud.dht.client.impl;

import com.ms.silverking.cloud.dht.NamespaceCreationOptions;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.cloud.dht.client.NamespaceModificationException;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.cloud.dht.common.NamespacePropertiesDeleteException;
import com.ms.silverking.cloud.dht.common.NamespacePropertiesRetrievalException;
import com.ms.silverking.cloud.dht.common.TimeoutException;

public interface NamespaceOptionsClientCS {
  ////// ====== client side namespace admin API (take human-readable namespace name as arg) ======
  void createNamespace(String nsName, NamespaceProperties nsProperties)
      throws NamespaceCreationException;

  void modifyNamespace(String nsName, NamespaceProperties nsProperties)
      throws NamespaceModificationException;

  void deleteNamespace(String nsName) throws NamespacePropertiesDeleteException;

  ////// ====== client side internal query API (take human-readable namespace name as arg) ======
  NamespaceProperties getNamespacePropertiesAndTryAutoCreate(String nsName)
      throws NamespacePropertiesRetrievalException, TimeoutException;

  NamespaceCreationOptions getNamespaceCreationOptions();
}
