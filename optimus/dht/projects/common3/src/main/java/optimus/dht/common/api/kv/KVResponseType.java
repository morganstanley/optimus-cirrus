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

public enum KVResponseType {
  GENERIC(1),
  VALUE(2);

  private final int typeId;

  private static final KVResponseType[] values = new KVResponseType[3];

  private KVResponseType(int typeId) {
    this.typeId = typeId;
  }

  public int getTypeId() {
    return typeId;
  }

  public short getTypeIdShort() {
    return (short) typeId;
  }

  static {
    for (KVResponseType type : KVResponseType.values()) {
      values[type.typeId] = type;
    }
  }

  public static KVResponseType getTypeById(int id) {
    if (id < 0 || id > 2) {
      return null;
    } else {
      return values[id];
    }
  }
}
