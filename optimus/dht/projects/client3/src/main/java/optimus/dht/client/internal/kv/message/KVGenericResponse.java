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

public class KVGenericResponse implements KVResponse {

  private final long requestId;

  private final List<Boolean> responses;

  private final String errorCode;
  private final String errorText;

  public KVGenericResponse(long requestId, List<Boolean> responses) {
    this(requestId, responses, null, null);
  }

  public KVGenericResponse(long requestId, String errorCode, String errorText) {
    this(requestId, null, errorCode, errorText);
  }

  public KVGenericResponse(
      long requestId, List<Boolean> responses, String errorCode, String errorText) {
    this.requestId = requestId;
    this.responses = responses;
    this.errorCode = errorCode;
    this.errorText = errorText;
  }

  public long requestId() {
    return requestId;
  }

  public List<Boolean> responses() {
    return responses;
  }

  public String errorCode() {
    return errorCode;
  }

  public String errorText() {
    return errorText;
  }
}
