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
  public final String owner;
  public final String method;
  public final String desc;

  public LNodeCall(String owner, String method) {
    this(owner, method, "");
  }

  public LNodeCall(String owner, String method, String desc) {
    this.owner = owner;
    this.method = method;
    this.desc = desc;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LNodeCall lNodeCall = (LNodeCall) o;
    return owner.equals(lNodeCall.owner)
        && method.equals(lNodeCall.method)
        && desc.equals(lNodeCall.desc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(owner, method, desc);
  }
}
