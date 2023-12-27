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
package optimus.platform.inputs.registry;

import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import optimus.platform.inputs.NodeInputMapValue;
import optimus.platform.inputs.loaders.LoaderSource;

public class CombinationStrategies {
  public static <T> DistCombinator<T> distCombinator() {
    return new DistCombinator<>();
  }

  public static <T> GraphCombinator<T> graphCombinator() {
    return new GraphCombinator<>();
  }

  public static <T> JvmCombinator<T> jvmCombinator() {
    return new JvmCombinator<>();
  }

  private static class GraphCombinator<T> extends CombinationStrategy<T> {
    private static Map<LoaderSource, Integer> makeMap() {
      Map<LoaderSource, Integer> myMap = new HashMap<>();
      myMap.put(LoaderSource.CODE, 6);
      myMap.put(LoaderSource.SYSTEM_PROPERTIES, 5);
      myMap.put(LoaderSource.CMDLINE, 4);
      myMap.put(LoaderSource.ENVIRONMENT_VARIABLES, 3);
      return myMap;
    }

    public GraphCombinator() {
      super(new EnumMap<>(makeMap()));
    }
  }

  private static class DistCombinator<T> extends CombinationStrategy<T> {
    private static Map<LoaderSource, Integer> makeMap() {
      Map<LoaderSource, Integer> myMap = new HashMap<>();
      myMap.put(LoaderSource.CODE, 6);
      myMap.put(LoaderSource.SYSTEM_PROPERTIES, 5);
      myMap.put(LoaderSource.ENVIRONMENT_VARIABLES, 4);
      myMap.put(LoaderSource.FILE, 3);
      return myMap;
    }

    public DistCombinator() {
      super(new EnumMap<>(makeMap()));
    }
  }

  private static class JvmCombinator<T> extends CombinationStrategy<T> {
    private static Map<LoaderSource, Integer> makeMap() {
      Map<LoaderSource, Integer> myMap = new HashMap<>();
      myMap.put(LoaderSource.SYSTEM_PROPERTIES, 6);
      myMap.put(LoaderSource.ENVIRONMENT_VARIABLES, 5);
      myMap.put(LoaderSource.CODE, 4);
      return myMap;
    }

    public JvmCombinator() {
      super(new EnumMap<>(makeMap()));
    }
  }

  public abstract static class CombinationStrategy<T> implements Comparator<LoaderSource> {
    final EnumMap<LoaderSource, Integer> priorities;

    public CombinationStrategy(EnumMap<LoaderSource, Integer> priorities) {
      this.priorities = priorities;
    }

    @Override
    public final int compare(LoaderSource currSource, LoaderSource newSource) {
      Integer currSourcePriority = priorities.get(currSource);
      Integer newSourcePriority = priorities.get(newSource);
      String baseExStr = "This input should not be loaded from source:";
      if (currSourcePriority == null)
        throw new UnsupportedOperationException(baseExStr + currSource);
      else if (newSourcePriority == null)
        throw new UnsupportedOperationException(baseExStr + newSource);
      return currSourcePriority.compareTo(newSourcePriority);
    }

    public final NodeInputMapValue<T> choose(
        LoaderSource currSource, T currValue, LoaderSource newSource, T newValue) {
      int comparisonResult = compare(currSource, newSource);
      if (comparisonResult > 0) return new NodeInputMapValue<>(currSource, currValue);
      else return new NodeInputMapValue<>(newSource, newValue);
    }
  }
}
