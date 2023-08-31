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
package com.ms.silverking.cloud.dht.net;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.common.DHTConstants;
import com.ms.silverking.cloud.dht.meta.DHTConfiguration;
import com.ms.silverking.cloud.dht.meta.IpAliasConfiguration;
import com.ms.silverking.cloud.dht.meta.MetaClient;
import com.ms.silverking.cloud.zookeeper.SilverKingZooKeeperClient.KeeperException;
import com.ms.silverking.collection.Pair;
import com.ms.silverking.net.IPAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPAliasingUtil {
  public static IPAliasMap readAliases(
      DHTConfiguration dhtConfig, IpAliasConfiguration aliasConfig) {
    Map<String, String> aliasMap = aliasConfig.getIPAliasMap();
    Map<IPAndPort, IPAndPort[]> resolvedMap;
    if (aliasMap != null) {
      resolvedMap = parseConfigAliasMap(aliasMap, dhtConfig.getPort());
    } else {
      resolvedMap = readFileAliases(dhtConfig.getPort());
    }
    return new IPAliasMap(resolvedMap);
  }

  private static Logger log = LoggerFactory.getLogger(IPAliasingUtil.class);

  public static IPAliasMap readAliases(ClientDHTConfiguration config) {
    try {
      MetaClient mc = new MetaClient(config);
      DHTConfiguration dhtConfig = mc.getDHTConfiguration();
      IpAliasConfiguration ipAliasConfiguration =
          mc.getIpAliasConfiguration(dhtConfig.getIpAliasMapName());
      return readAliases(dhtConfig, ipAliasConfiguration);
    } catch (KeeperException | IOException ex) {
      log.error("Failed to lookup aliases on ZK, will try locally", ex);
      return new IPAliasMap(readFileAliases(config.getPort()));
    }
  }

  static Map<IPAndPort, IPAndPort[]> parseConfigAliasMap(
      Map<String, String> aliasMap, int configPort) {
    if (aliasMap != null) {
      ImmutableMap.Builder<IPAndPort, IPAndPort[]> builder = ImmutableMap.builder();

      for (Map.Entry<String, String> entry : aliasMap.entrySet()) {
        String ip = entry.getKey();
        if (ip.contains(":")) {
          throw new RuntimeException("lhs must be daemon ip only, not daemon ip:port");
        }
        String aliasToken = entry.getValue();
        builder.put(new IPAndPort(ip, configPort), parseAliasToken(aliasToken, configPort));
      }
      return builder.build();
    } else {
      return readFileAliases(configPort);
    }
  }

  /**
   * Reads the IP alias map from a file declared by DHTConstants.ipAliasMapFileVar envVar File
   * structure - ip+ip,ip,ip - one set per line. The LHS is the real IP, the RHS is a set of aliases
   * OR ip+ip:port,ip:port,ip:port.. N.b. if port is not provided in the alias, a fixed port is
   * assumed as declared in the DHT config
   *
   * @param fixedPort the fixed port expected for the SK cluster (from DHT config)
   * @return a map of ip -> List of aliases
   */
  private static Map<IPAndPort, IPAndPort[]> readFileAliases(int fixedPort) {
    String aliasMapFile;

    aliasMapFile = DHTConstants.aliasMapFile;
    log.info("{}={}", DHTConstants.ipAliasMapFileVar, aliasMapFile);
    if (aliasMapFile != null && aliasMapFile.trim().length() > 0) {
      try {
        FileInputStream in = new FileInputStream(new File(aliasMapFile));
        BufferedReader reader;
        String line;
        Map<IPAndPort, IPAndPort[]> map;

        map = new HashMap<>();
        reader = new BufferedReader(new InputStreamReader(in));
        do {
          line = reader.readLine();
          if (line != null) {
            Pair<IPAndPort, IPAndPort[]> entry;
            entry = parseAliasFileLine(line, fixedPort);
            map.put(entry.getV1(), entry.getV2());
          }
        } while (line != null);
        reader.close();
        return ImmutableMap.copyOf(map);
      } catch (IOException ioe) {
        log.error("Unable to read ip alias map: {}", aliasMapFile, ioe);
        return null;
      }
    } else {
      return null;
    }
  }

  private static Pair<IPAndPort, IPAndPort[]> parseAliasFileLine(String s, int port) {
    String[] toks;

    toks = s.trim().split("\\s+");
    if (toks.length != 2) {
      throw new RuntimeException("Invalid map entry: " + s);
    } else {
      IPAndPort addr;
      IPAndPort[] aliases;
      addr = new IPAndPort(toks[0], port);
      aliases = parseAliasToken(toks[1], port);
      return new Pair<>(addr, aliases);
    }
  }

  private static IPAndPort[] parseAliasToken(String token, int port) {
    if (token.contains(":")) {
      return IPAndPort.parseToArray(token);
    } else {
      return IPAndPort.parseToArray(token, port);
    }
  }
}
