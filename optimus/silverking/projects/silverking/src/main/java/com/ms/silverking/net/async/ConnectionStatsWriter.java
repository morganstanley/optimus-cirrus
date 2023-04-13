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
package com.ms.silverking.net.async;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ConnectionStatsWriter {
  private final File statsBaseDir;

  public ConnectionStatsWriter(File baseLogDir) {
    statsBaseDir = baseLogDir;
  }

  private File statsFile(Connection c) {
    return new File(statsBaseDir, c.getRemoteSocketAddress().toString());
  }

  public void writeStats(Connection c) throws IOException {
    File _statsFile;
    RandomAccessFile statsFile;

    _statsFile = statsFile(c);
    _statsFile.getParentFile().mkdirs();
    statsFile = new RandomAccessFile(_statsFile, "rw");
    statsFile.seek(statsFile.length());
    statsFile.writeBytes(System.currentTimeMillis() + "\t" + c.statString() + "\n");
    statsFile.close();
  }
}
