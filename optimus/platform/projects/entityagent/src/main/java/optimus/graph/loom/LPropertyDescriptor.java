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

import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Objects;
import org.objectweb.asm.tree.ClassNode;

public class LPropertyDescriptor {
  public static final int COLUMN_NA = Integer.MIN_VALUE;
  private static final Object _descriptorLock = new Object();
  private static int descriptorsCount = 1;
  private static LPropertyDescriptor[] descriptors = new LPropertyDescriptor[1024];

  static {
    var cls = LPropertyDescriptor.class.getName();
    descriptors[0] =
        new LPropertyDescriptor(cls, "ctor", "LPropertyDescriptor.java", 26, COLUMN_NA, 0);
  }

  public final String className;
  public final String methodName;
  public String source;
  public int lineNumber;
  public int columnNumber;
  public MethodType methodType;
  public final int localID; // Number of the lambda method in the bytecode (serialization)
  public int profileID; // See getProfileID. Assigned in core

  public LPropertyDescriptor(String className, String methodName) {
    this.className = className;
    this.methodName = methodName;
    this.localID = -1;
  }

  public LPropertyDescriptor(String cls, String method, String src, int line, int col, int lID) {
    this.className = cls;
    this.methodName = method;
    this.source = src;
    this.lineNumber = line;
    this.columnNumber = col;
    this.localID = lID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LPropertyDescriptor that = (LPropertyDescriptor) o;
    return Objects.equals(className, that.className) && Objects.equals(methodName, that.methodName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className, methodName);
  }

  public static int register(ClassNode cls, String methodName, int line, int col, int localID) {
    return register(cls.name, methodName, cls.sourceFile, line, col, localID);
  }

  private static int register(
      String className, String methodName, String source, int line, int col, int localID) {
    synchronized (_descriptorLock) {
      var id = descriptorsCount;
      if (descriptorsCount >= descriptors.length)
        descriptors = Arrays.copyOf(descriptors, descriptors.length * 2);
      var cleanName = className.replace('/', '.');
      var desc = new LPropertyDescriptor(cleanName, methodName, source, line, col, localID);
      descriptors[descriptorsCount++] = desc;
      return id;
    }
  }

  public static LPropertyDescriptor get(int id) {
    return descriptors[id];
  }
}
