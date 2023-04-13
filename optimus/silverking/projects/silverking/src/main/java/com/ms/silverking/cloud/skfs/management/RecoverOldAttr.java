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

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.NonExistenceResponse;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.DHTSession;
import com.ms.silverking.cloud.dht.client.PutException;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoverOldAttr {
  private final SynchronousNamespacePerspective<String, byte[]> syncNSP;

  private static Logger log = LoggerFactory.getLogger(RecoverOldAttr.class);

  private static final String attrNamespace = "attr";

  public RecoverOldAttr(SKGridConfiguration gc, String namespace) throws ClientException, IOException {
    DHTClient client;
    DHTSession session;

    client = new DHTClient();
    session = client.openSession(gc);
    syncNSP = session.openSyncNamespacePerspective(namespace, String.class, byte[].class);
  }

  public void recover(String file, long version, String outputFile) throws PutException, RetrievalException {
    StoredValue<byte[]> oldAttrValue;
    GetOptions getOptions;

    getOptions = syncNSP.getOptions().getDefaultGetOptions().versionConstraint(
        VersionConstraint.exactMatch(version)).nonExistenceResponse(NonExistenceResponse.NULL_VALUE);
    oldAttrValue = syncNSP.retrieve(file, getOptions);
    if (oldAttrValue == null || oldAttrValue.getValue() == null) {
      log.info("Couldn't find file {} version {}", file, version);
    } else {
      log.info("Rewriting file {} version {}", file, version);
      syncNSP.put(outputFile, oldAttrValue.getValue());
    }
  }

  public static void main(String[] args) {
    if (args.length < 3 || args.length > 5) {
      log.info("args: <gridConfig> <file> <version> [outputFile] [namespace]");
    } else {
      try {
        RecoverOldAttr roa;
        SKGridConfiguration gc;
        String file;
        long version;
        String namespace;
        String  outputFile;

        gc = SKGridConfiguration.parseFile(args[0]);
        file = args[1];
        version = Long.parseLong(args[2]);
        if (args.length >= 4) {
          outputFile = args[3];
        } else {
          outputFile = file;
        }
        if (args.length == 5) {
          namespace = args[4];
        } else {
          namespace = attrNamespace;
        }
        roa = new RecoverOldAttr(gc, namespace);
        roa.recover(file, version, outputFile);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
