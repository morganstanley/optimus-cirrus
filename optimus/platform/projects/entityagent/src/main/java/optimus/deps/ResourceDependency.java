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
package optimus.deps;

import java.io.Serializable;
import java.util.Objects;

public class ResourceDependency implements Serializable {
  public final String resourceId;
  public final ResourceAccessType accessType;

  public ResourceDependency(String resourceId, ResourceAccessType accessType) {
    super();
    this.resourceId = resourceId;
    this.accessType = accessType;
  }

  @Override
  public String toString() {
    return "ResourceDependency{"
        + "resourceId='"
        + resourceId
        + '\''
        + ", accessType="
        + accessType
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ResourceDependency that = (ResourceDependency) o;
    return Objects.equals(resourceId, that.resourceId) && accessType == that.accessType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceId, accessType);
  }
}
