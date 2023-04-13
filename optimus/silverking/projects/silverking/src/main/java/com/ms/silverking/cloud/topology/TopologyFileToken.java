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

class TopologyFileToken {
  private final Type type;
  private final String entry;

  enum Type {ENTRY, OPEN_BLOCK, CLOSE_BLOCK}

  ;

  private static final String OPEN_BLOCK_DELIMITER = "{";
  private static final String CLOSE_BLOCK_DELIMITER = "}";

  private static final TopologyFileToken OPEN_BLOCK_TOKEN = new TopologyFileToken(Type.OPEN_BLOCK, null);
  private static final TopologyFileToken CLOSE_BLOCK_TOKEN = new TopologyFileToken(Type.CLOSE_BLOCK, null);

  static TopologyFileToken[] parseLine(String def) {
    TopologyFileToken[] tokens;
    String[] defs;

    defs = def.split("\\s+");
    tokens = new TopologyFileToken[defs.length];
    for (int i = 0; i < tokens.length; i++) {
      tokens[i] = parse(defs[i]);
    }
    return tokens;
  }

  static TopologyFileToken parse(String def) {
    if (def.equals(OPEN_BLOCK_DELIMITER)) {
      return OPEN_BLOCK_TOKEN;
    } else if (def.equals(CLOSE_BLOCK_DELIMITER)) {
      return CLOSE_BLOCK_TOKEN;
    } else {
      return new TopologyFileToken(Type.ENTRY, def);
    }
  }

  TopologyFileToken(Type type, String entry) {
    this.type = type;
    this.entry = entry;
  }

  Type getType() {
    return type;
  }

  String getEntry() {
    return entry;
  }

  @Override
  public String toString() {
    return type + ":" + entry;
  }
}
