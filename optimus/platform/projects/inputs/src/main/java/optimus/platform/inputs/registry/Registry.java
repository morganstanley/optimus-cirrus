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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.registry.parameters.DistInputs;
import optimus.platform.inputs.registry.parameters.GraphInputs;
import optimus.platform.inputs.registry.parameters.SafeDistInputs;
import optimus.platform.inputs.typesafe.SourceCastableMap;

public class Registry {
  private static final List<Class<?>> ClassesToLoad =
      Arrays.asList(DistInputs.class, GraphInputs.class, SafeDistInputs.class);
  private static final List<String> ReflectiveJavaClassesToLoad =
      Arrays.asList(
          "optimus.platform.inputs.registry.parameters.AuditorSafeInputs",
          "optimus.platform.inputs.registry.parameters.ModelSafeInputs",
          "optimus.platform.inputs.registry.parameters.DTCInputs");
  /* these scala objects listed musts only contain vals of the process inputs and nothing else */
  private static final List<String> ReflectiveScalaObjectsToLoad =
      Arrays.asList(
          "optimus.platform.inputs.registry.JvmInputs",
          "optimus.platform.inputs.registry.ProcessGraphInputs",
          "optimus.sandbox.SandboxInputs",
          "optimus.platform.inputs.registry.ProcessDistInputs",
          "optimus.observability.AtJobTestNodeInputs");

  public static final Registry TheHugeRegistryOfAllOptimusProperties = new Registry();

  private static void loadClass(Class<?> clazz) {
    for (Field field : clazz.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) continue;
      try {
        NodeInput<?> nodeInput = (NodeInput<?>) field.get(null);
        TheHugeRegistryOfAllOptimusProperties.register(nodeInput);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  private static void loadScalaObject(Predicate<Field> p, Class<?> inputLoaderClass) {
    try {
      Object classInstance = inputLoaderClass.getDeclaredField("MODULE$").get(null);
      for (Field field : inputLoaderClass.getDeclaredFields()) {
        if (p.test(field)) {
          // in case the field was defined as private
          field.setAccessible(true);

          NodeInput<?> nodeInput = (NodeInput<?>) field.get(classInstance);
          TheHugeRegistryOfAllOptimusProperties.register(nodeInput);
        }
      }
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  static {
    for (Class<?> clazz : ClassesToLoad) loadClass(clazz);
    for (String clazzStr : ReflectiveJavaClassesToLoad) {
      try {
        Class<?> clazz = Class.forName(clazzStr);
        loadClass(clazz);
      } catch (ClassNotFoundException cnfe) {
        // ignore if missing
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }

    for (String inputLoaderString : ReflectiveScalaObjectsToLoad) {
      try {
        // reflectively getting the inputs out of scala objects and registering them
        Class<?> inputLoaderClass = Class.forName(inputLoaderString + "$");
        loadScalaObject(p -> !p.getName().contains("MODULE"), inputLoaderClass);
      } catch (ClassNotFoundException ignored) {
        // not in code that depends on core
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  private final Map<String, NodeInput<?>> names = new HashMap<>();
  private final SourceCastableMap sourceCastableMap = new SourceCastableMap();

  private Registry() {}

  <T> void register(NodeInput<T> nodeInput) {
    if (names.containsKey(nodeInput.name())) {
      throw new IllegalStateException(
          "Cannot store different node inputs under same name " + nodeInput.name());
    }
    names.put(nodeInput.name(), nodeInput);
    nodeInput
        .sources()
        .forEach(
            source -> {
              if (lookup(source.key()).isPresent()) {
                throw new IllegalStateException(
                    "Cannot store different node inputs under same lookup key " + source.key());
              }
              sourceCastableMap.put(source, nodeInput);
            });
  }

  public final <V> Optional<SourceCastableMap.Entry<V, ?>> lookup(Source.Key<?, V> key) {
    return sourceCastableMap.get(key);
  }

  public final NodeInput<?> lookupByName(String name) {
    return names.get(name);
  }

  public final boolean contains(String name) {
    return lookupByName(name) != null;
  }
}
