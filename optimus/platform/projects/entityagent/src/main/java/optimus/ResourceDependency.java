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
package optimus;

import static optimus.ClassMonitorInjector.FILE_PREFIX;
import static optimus.ClassMonitorInjector.NETWORK_PREFIX;

import java.io.Serializable;
import java.util.Objects;


public class ResourceDependency implements Serializable {
  private final String resourceId;
  private final ResourceAccessType accessType;

  ResourceDependency(String resourceId, ResourceAccessType accessType) {
    super();
    this.resourceId = resourceId;
    this.accessType = accessType;
  }

  public String getResourceId() {
    return resourceId;
  }

  public String getQualifiedResourceId() {
    switch (accessType) {
      case network:
        return NETWORK_PREFIX + resourceId;
      default:
        return FILE_PREFIX + resourceId;
    }
  }

  ResourceAccessType getAccessType() {
    return accessType;
  }

  @Override
  public String toString() {
    return "ResourceDependency{" + "resourceId='" + resourceId + '\'' + ", accessType=" + accessType + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ResourceDependency that = (ResourceDependency) o;
    return Objects.equals(resourceId, that.resourceId) && accessType == that.accessType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourceId, accessType);
  }
}

