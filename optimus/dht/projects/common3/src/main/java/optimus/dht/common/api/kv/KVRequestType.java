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
package optimus.dht.common.api.kv;

public enum KVRequestType {
  GET(1),
  PUT(2),
  REMOVE(3),
  CONTAINS(4),
  REMOVE_TRANSIENT_KEYSPACE(5);

  private final int typeId;

  private static final KVRequestType[] values = new KVRequestType[6];

  private KVRequestType(int typeId) {
    this.typeId = typeId;
  }

  public int getTypeId() {
    return typeId;
  }

  public short getTypeIdShort() {
    return (short) typeId;
  }

  static {
    for (KVRequestType type : KVRequestType.values()) {
      values[type.typeId] = type;
    }
  }

  public static KVRequestType getTypeById(int id) {
    if (id < 0 || id > 5) {
      return null;
    } else {
      return values[id];
    }
  }
}
