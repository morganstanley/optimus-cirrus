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
package com.ms.silverking.cloud.skfs.dir;

import java.io.IOException;

import com.ms.silverking.cloud.dht.GetOptions;
import com.ms.silverking.cloud.dht.VersionConstraint;
import com.ms.silverking.cloud.dht.client.ClientException;
import com.ms.silverking.cloud.dht.client.DHTClient;
import com.ms.silverking.cloud.dht.client.RetrievalException;
import com.ms.silverking.cloud.dht.client.StoredValue;
import com.ms.silverking.cloud.dht.client.SynchronousNamespacePerspective;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.text.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirReader {
  private SynchronousNamespacePerspective<String, byte[]> dirNSP;

  private static Logger log = LoggerFactory.getLogger(DirReader.class);

  private static final String dirNamespaceName = "dir";

  public DirReader(SKGridConfiguration gc) throws IOException, ClientException {
    DHTClient dhtClient;

    dhtClient = new DHTClient();
    dirNSP = dhtClient.openSession(gc).openSyncNamespacePerspective(dirNamespaceName);
  }

  public byte[] readDir(String dirName, long maxVersion) {
    try {
      GetOptions getOptions;
      StoredValue<byte[]> sv;

      getOptions = dirNSP.getOptions().getDefaultGetOptions();
      if (maxVersion > 0) {
        getOptions = getOptions.versionConstraint(VersionConstraint.maxBelowOrEqual(maxVersion));
      }
      sv = dirNSP.retrieve(dirName, getOptions);
      return sv.getValue();
    } catch (RetrievalException e) {
      e.printStackTrace();
      return null;
    }
  }

  public void displayDir(String dirName, long maxVersion) {
    byte[] rawDir;
    Directory dir;

    rawDir = readDir(dirName, maxVersion);
    log.info("{}  {}", dirName, rawDir.length);

    dir = new DirectoryInPlace(rawDir, 0, rawDir.length);
    for (int i = 0; i < dir.getNumEntries(); i++) {
      log.info("{} {}", i, dir.getEntry(i));
    }
    log.info("");

    DirectoryInMemory dirInMem;

    dirInMem = new DirectoryInMemory((DirectoryInPlace) dir);
    for (int i = 0; i < dir.getNumEntries(); i++) {
      log.info("{}  {}", i, dir.getEntry(i));
    }
    log.info("");

    log.info("serialize");
    byte[] d;
    d = dirInMem.serialize();
    log.info("{}", StringUtil.byteArrayToHexString(rawDir));
    // System.out.printf("%s\n", StringUtil.byteArrayToHexString(d));
    log.info("serialization complete");
    dir = new DirectoryInPlace(d, 0, d.length);
    for (int i = 0; i < dir.getNumEntries(); i++) {
      log.info("{}  {}", i, dir.getEntry(i));
    }
  }

  public static void main(String[] args) {
    if (args.length != 2 && args.length != 3) {
      log.info("<gridConfig> <dir> [maxVersion]");
    } else {
      try {
        DirReader dirReader;
        SKGridConfiguration gc;
        String dirName;
        long maxVersion;

        gc = SKGridConfiguration.parseFile(args[0]);
        dirName = args[1];
        dirReader = new DirReader(gc);
        if (args.length == 3) {
          maxVersion = Long.parseLong(args[2]);
        } else {
          maxVersion = 0;
        }
        dirReader.displayDir(dirName, maxVersion);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
