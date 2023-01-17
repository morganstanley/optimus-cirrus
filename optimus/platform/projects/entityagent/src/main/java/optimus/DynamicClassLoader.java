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

import java.lang.invoke.MethodHandles;

public class DynamicClassLoader extends ClassLoader {

  public DynamicClassLoader() {
    new DynamicClassLoader(this.getClass());
  }

  public DynamicClassLoader(Class<?> cls) {
    super(cls.getClassLoader());
  }

  public Class<?> loadClass(byte[] bytes) {
    return defineClass(null /* Use from byte code */, bytes, 0, bytes.length);
  }

  public static Class<?> loadClassInCurrentClassLoader(byte[] bytes){
    try {
      return MethodHandles.lookup().defineClass(bytes);
    } catch (IllegalAccessException ex) {
      ex.printStackTrace();
      return null;
    }
  }


  public Object createInstance(byte[] bytes) {
    try {
      var cls = loadClass(bytes);
      return cls.getConstructor().newInstance();
    } catch (Exception ex) {
      ex.printStackTrace();
      return null;
    }
  }
}
