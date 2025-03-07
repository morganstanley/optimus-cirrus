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
package optimus.dht.client.internal.kv.message;

import java.util.List;
import optimus.dht.client.api.kv.KVStreamValue;
import optimus.dht.common.api.transport.DataStreamConsumer;

public class KVValueStreamResponse<S extends DataStreamConsumer> implements KVResponse {

  private final long requestId;

  private final List<KVStreamValue<S>> values;

  private final String errorCode;
  private final String errorText;

  public KVValueStreamResponse(long requestId, List<KVStreamValue<S>> responses) {
    this(requestId, responses, null, null);
  }

  public KVValueStreamResponse(long requestId, String errorCode, String errorText) {
    this(requestId, null, errorCode, errorText);
  }

  public KVValueStreamResponse(
      long requestId, List<KVStreamValue<S>> values, String errorCode, String errorText) {
    this.requestId = requestId;
    this.values = values;
    this.errorCode = errorCode;
    this.errorText = errorText;
  }

  public long requestId() {
    return requestId;
  }

  public List<KVStreamValue<S>> values() {
    return values;
  }

  public String errorCode() {
    return errorCode;
  }

  public String errorText() {
    return errorText;
  }
}
