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
package com.ms.silverking.cloud.skfs.management;

import java.io.IOException;
import java.util.Set;

import com.ms.silverking.cloud.dht.NamespaceOptions;
import com.ms.silverking.cloud.dht.SessionOptions;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.NamespaceCreationException;
import com.ms.silverking.collection.CollectionUtil;
import com.ms.silverking.time.SimpleStopwatch;
import com.ms.silverking.time.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SKFSNamespaceCreator {
  private final DHTClient client;
  private final DHTSession session;

  private static Logger log = LoggerFactory.getLogger(SKFSNamespaceCreator.class);

  public SKFSNamespaceCreator(ClientDHTConfiguration dhtConfig, String preferredServer)
      throws IOException, ClientException {
    client = new DHTClient();
    session = client.openSession(new SessionOptions(dhtConfig, preferredServer));
  }

  public void createNamespaces(Set<String> namespaces, NamespaceOptions nsOptions)
      throws NamespaceCreationException {
    log.info("Creating: {}", CollectionUtil.toString(namespaces));
    log.info("nsOptions: {}", nsOptions);
    for (String namespace : namespaces) {
      createNamespace(namespace, nsOptions);
    }
  }

  public void createNamespace(String namespace, NamespaceOptions nsOptions)
      throws NamespaceCreationException {
    Stopwatch sw;

    sw = new SimpleStopwatch();
    session.createNamespace(namespace, nsOptions);
    log.info("Created namespace: {}   Elapsed: {}", namespace, sw.getElapsedSeconds());
  }

  public void close() {
    session.close();
  }
}
