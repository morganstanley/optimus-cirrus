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

import static optimus.graph.loom.LoomConfig.COLUMN_NA;
import java.lang.invoke.MethodType;
import java.util.Arrays;
import java.util.Objects;
import optimus.graph.NodeTaskInfo;
import optimus.graph.NodeTaskInfoRegistry;

public class LPropertyDescriptor {
  private static final Object _descriptorLock = new Object();
  private static int descriptorsCount = 0;
  private static LPropertyDescriptor[] descriptors = new LPropertyDescriptor[1024];

  static {
    register(LPropertyDescriptor.class, "ctor", "LPropertyDescriptor.java", 26, COLUMN_NA, 0, -1);
  }

  public final String className;
  public final String methodName;
  public String source;
  public int lineNumber;
  public int columnNumber;
  public MethodType methodType;
  public int localID; // Number of the lambda method in the bytecode (serialization)
  public int profileID; // See getProfileID. Assigned in core

  public LPropertyDescriptor(String className, String methodName) {
    this.className = className;
    this.methodName = methodName;
    this.localID = -1;
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

  /*
   * returns clsID
   * */
  public static int register(
      Class<?> clazz,
      String methodName,
      String source,
      int line,
      int col,
      int localID,
      int propertyID) {
    var className = clazz.getName();
    LPropertyDescriptor desc;
    if (propertyID <= NodeTaskInfo.Default.profile)
      desc = new LPropertyDescriptor(className, methodName);
    else desc = NodeTaskInfoRegistry.getDescriptor(propertyID);

    desc.source = source;
    desc.lineNumber = line;
    desc.columnNumber = col;
    desc.localID = localID;

    synchronized (_descriptorLock) {
      var id = descriptorsCount;
      if (descriptorsCount >= descriptors.length)
        descriptors = Arrays.copyOf(descriptors, descriptors.length * 2);

      descriptors[descriptorsCount++] = desc;
      return id;
    }
  }

  public static LPropertyDescriptor get(int clsID) {
    return descriptors[clsID];
  }
}
