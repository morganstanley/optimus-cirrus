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

public class SingleResourceClassLoader extends ClassLoader {
  public SingleResourceClassLoader(ClassLoader parent) {
    super(parent);
  }

  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    URL resource = getParent().getResource(name);

    if (resource != null) return Collections.enumeration(Collections.singletonList(resource));
    else return Collections.emptyEnumeration();
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
}
