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
package optimus.stratosphere.bootstrap.config;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class FilteredClassLoader extends ClassLoader {

  private final List<String> excludedPaths;

  public FilteredClassLoader(ClassLoader parent, List<String> excludedPaths) {
    super(parent);
    this.excludedPaths = excludedPaths;
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration<URL> found = getParent().getResources(name);
    List<URL> filtered =
        Collections.list(found).stream().filter(url -> url != null && notExcluded(url)).toList();
    return Collections.enumeration(filtered);
  }

  /** Comes from typesafeconfig.Parseable */
  public static String convertResourceName(Class<?> clazz, String resource) {
    if (resource.startsWith("/")) {
      // "absolute" resource, chop the slash
      return resource.substring(1);
    } else {
      String className = clazz.getName();
      int i = className.lastIndexOf('.');
      if (i < 0) {
        // no package
        return resource;
      } else {
        // need to be relative to the package
        String packageName = className.substring(0, i);
        String packagePath = packageName.replace('.', '/');
        return packagePath + "/" + resource;
      }
    }
  }

  private String normalizePath(String path) {
    return path.replace("\\", "/");
  }

  private Boolean notExcluded(URL url) {
    return excludedPaths.stream()
        .noneMatch(substring -> normalizePath(url.getPath()).contains(substring));
  }
}
