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
package optimus.graph.loom;

import java.util.Objects;

/** record would have been just fine here! */
class LNodeCall {
  public String owner;
  public String method;

  public LNodeCall(String owner, String method) {
    this.owner = owner;
    this.method = method;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    var nodeCall = (LNodeCall) o;

    if (!Objects.equals(owner, nodeCall.owner)) return false;
    return Objects.equals(method, nodeCall.method);
  }

  @Override
  public int hashCode() {
    int result = owner != null ? owner.hashCode() : 0;
    result = 31 * result + (method != null ? method.hashCode() : 0);
    return result;
  }
}
