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
package com.ms.silverking.cloud.topology;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyParser {
  private static final long noVersion = Long.MAX_VALUE;

  private static Logger log = LoggerFactory.getLogger(TopologyParser.class);

  public TopologyParser() {}

  public static Topology parse(File file) throws IOException, TopologyParseException {
    return parse(new FileInputStream(file));
  }

  public static Topology _parse(InputStream in, long version)
      throws IOException, TopologyParseException {
    BufferedReader reader;
    String line;
    int lineNumber;
    List<TopologyEntry> parentEntries;
    TopologyFileToken prevToken;
    TopologyEntry root;
    Node rootNode;

    root = null;
    prevToken = null;
    lineNumber = 0;
    parentEntries = new LinkedList<TopologyEntry>();
    try {
      reader = new BufferedReader(new InputStreamReader(in));
      do {
        line = reader.readLine();
        if (line != null) {
          line = line.trim();
          if (line.length() > 0) {
            TopologyFileToken[] tokens;

            lineNumber++;
            tokens = TopologyFileToken.parseLine(line);
            // handle new tokens
            for (TopologyFileToken token : tokens) {
              if (root == null) {
                switch (token.getType()) {
                  case OPEN_BLOCK:
                  case CLOSE_BLOCK:
                    throw new TopologyParseException("Invalid block defiition");
                  case ENTRY:
                    root = new TopologyEntry(token.getEntry());
                    parentEntries.add(root);
                    break;
                  default:
                    throw new RuntimeException("panic");
                }
              } else {
                switch (token.getType()) {
                  case OPEN_BLOCK:
                    if (prevToken != null) {
                      switch (prevToken.getType()) {
                        case OPEN_BLOCK:
                        case CLOSE_BLOCK:
                          throw new TopologyParseException("Invalid block defiition");
                        case ENTRY:
                          TopologyEntry parent;
                          TopologyEntry lastAdded;

                          parent = parentEntries.get(parentEntries.size() - 1);
                          lastAdded = parent.getLastChild();
                          parentEntries.add(lastAdded);
                          break;
                        default:
                          throw new RuntimeException("panic");
                      }
                    }
                    break;
                  case CLOSE_BLOCK:
                    switch (prevToken.getType()) {
                      case CLOSE_BLOCK:
                      case OPEN_BLOCK:
                      case ENTRY:
                        parentEntries.remove(parentEntries.size() - 1);
                        break;
                      default:
                        throw new RuntimeException("panic");
                    }
                    break;
                  case ENTRY:
                    parentEntries
                        .get(parentEntries.size() - 1)
                        .addChild(new TopologyEntry(token.getEntry()));
                    break;
                  default:
                    throw new RuntimeException("panic");
                }
                prevToken = token;
              }
            }
          }
        }
      } while (line != null);
      rootNode = TopologyEntry.entriesToNodes(root);
      if (version == noVersion) {
        return Topology.fromRoot(null, rootNode);
      } else {
        return Topology.fromRoot(null, rootNode, version);
      }
    } catch (TopologyParseException tpe) {
      throw new TopologyParseException(tpe.getMessage() + " lineNumber: " + lineNumber);
    } catch (IOException ioe) {
      throw new IOException(ioe.getMessage() + " lineNumber: " + lineNumber);
    }
  }

  public static Topology parse(InputStream in) throws IOException, TopologyParseException {
    return _parse(in, noVersion);
  }

  public static Topology parseVersioned(File topoFile, long version) throws IOException {
    return _parse(new FileInputStream(topoFile), version);
  }

  public static Topology parseVersioned(String def, long version) throws IOException {
    return _parse(new ByteArrayInputStream(def.getBytes()), version);
  }

  private static void quickCheckLine(String line) throws IOException {
    if (line.indexOf(' ') >= 0) {
      throw new IOException("Spaces disallowed: " + line);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      File topologyFile;

      if (args.length != 1) {
        log.info("<file>");
        return;
      } else {
        TopologyParser topologyParser;

        topologyFile = new File(args[0]);
        topologyParser = new TopologyParser();
        topologyParser.parse(topologyFile);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
