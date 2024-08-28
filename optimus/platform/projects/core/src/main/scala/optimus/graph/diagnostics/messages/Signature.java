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
package optimus.graph.diagnostics.messages;

import java.util.Arrays;

public final class Signature {
  private String[] names;
  private String[] descriptors;

  public String[] getDescriptors() {
    return descriptors;
  }

  public String[] getNames() {
    return names;
  }

  public void setDescriptors(String[] descriptors) {
    this.descriptors = descriptors;
  }

  public void setNames(String[] names) {
    this.names = names;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Signature) {
      var other = (Signature) obj;
      return Arrays.equals(names, other.names) && Arrays.equals(descriptors, other.descriptors);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(names);
  }
}
