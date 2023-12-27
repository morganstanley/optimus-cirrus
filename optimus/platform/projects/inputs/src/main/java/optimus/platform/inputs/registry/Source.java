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

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import optimus.platform.inputs.NodeInput;
import optimus.platform.inputs.loaders.Loaders;
import optimus.platform.inputs.loaders.NodeInputStorage;

/**
 * Represents a source from where a {@link NodeInput} can be loaded. Examples of sources could be
 * system properties, environment variables, command lines and the sorts.
 *
 * <p>It is a {@link Source.Key} combined with a mapping function that allows converting from {@code
 * V} to {@code T}.
 *
 * @see Loaders#systemProperties(NodeInputStorage)
 * @see Loaders#environmentVariables(NodeInputStorage)
 * @see Loaders#fileProperties(NodeInputStorage, File)
 * @param <K> The type of the key in the original source
 * @param <V> The type of the value in the original source
 * @param <T> The type of the {@link NodeInput} associated with this {@link Source}
 */
public abstract class Source<K, V, T> {

  // Functions to convert from one type to another, used for parsing
  private static final Function<String, String> STR2STR = Function.identity();
  private static final Function<String, Boolean> STR2BOOL = Boolean::parseBoolean;
  private static final Function<String, Integer> STR2INT = Integer::parseInt;
  private static final Function<String, Long> STR2LONG = Long::parseLong;
  private static final Function<String, List<String>> STR2STRLIST = Source::stringToStringList;

  private static final Function<String, Duration> STR2DUR = Duration::parse;

  protected final K name;
  private final Function<V, T> mapper;

  protected Source(K name, Function<V, T> mapper) {
    this.name = name;
    this.mapper = mapper;
  }

  public abstract Key<K, V> key();

  public K name() {
    return name;
  }

  public Function<V, T> mapper() {
    return mapper;
  }

  /** The source is a java property */
  public static class JavaProperty<T> extends Source<String, String, T> {

    JavaProperty(String name, Function<String, T> mapper) {
      super(name, mapper);
    }

    @Override
    public JavaPropertyKey key() {
      return new JavaPropertyKey(name);
    }

    public static Key<String, String> key(String javaProperty) {
      return new JavaPropertyKey(javaProperty);
    }

    private static class JavaPropertyKey extends Key<String, String> {
      protected JavaPropertyKey(String key) {
        super("java-property", key);
      }
    }
  }

  public static <T> List<Source<?, ?, T>> fromJavaProperties(
      Function<String, T> mapper, String... javaProperties) {
    return Arrays.stream(javaProperties)
        .map(jp -> new JavaProperty<>(jp, mapper))
        .collect(Collectors.toList());
  }

  public static JavaProperty<String> fromJavaProperty(String javaProperty) {
    return new JavaProperty<>(javaProperty, STR2STR);
  }

  public static JavaProperty<Duration> fromDurJavaProperty(String javaProperty) {
    return new JavaProperty<>(javaProperty, STR2DUR);
  }

  public static List<Source<?, ?, String>> fromJavaProperties(String... javaProperties) {
    return Arrays.stream(javaProperties).map(Source::fromJavaProperty).collect(Collectors.toList());
  }

  public static List<Source<?, ?, List<String>>> fromListJavaProperties(String... javaProperties) {
    return Arrays.stream(javaProperties)
        .map(jp -> new JavaProperty<>(jp, STR2STRLIST))
        .collect(Collectors.toList());
  }

  public static JavaProperty<Boolean> fromBoolJavaProperty(String javaProperty) {
    return new JavaProperty<>(javaProperty, STR2BOOL);
  }

  public static List<Source<?, ?, Boolean>> fromBoolJavaProperties(String... javaProperties) {
    return Arrays.stream(javaProperties)
        .map(Source::fromBoolJavaProperty)
        .collect(Collectors.toList());
  }

  public static JavaProperty<Integer> fromIntJavaProperty(String javaProperty) {
    return new JavaProperty<>(javaProperty, STR2INT);
  }

  public static List<Source<?, ?, Integer>> fromIntJavaProperties(String... javaProperties) {
    return Arrays.stream(javaProperties)
        .map(Source::fromIntJavaProperty)
        .collect(Collectors.toList());
  }

  public static JavaProperty<Long> fromLongJavaProperty(String javaProperty) {
    return new JavaProperty<>(javaProperty, STR2LONG);
  }

  public static List<Source<?, ?, Long>> fromLongJavaProperties(String... javaProperties) {
    return Arrays.stream(javaProperties)
        .map(Source::fromLongJavaProperty)
        .collect(Collectors.toList());
  }

  /** The source is an environment variable */
  public static class EnvironmentVariable<T> extends Source<String, String, T> {

    private EnvironmentVariable(String name, Function<String, T> mapper) {
      super(name, mapper);
    }

    @Override
    public EnvironmentVariableKey key() {
      return new EnvironmentVariableKey(name);
    }

    public static Key<String, String> key(String environmentVariable) {
      return new EnvironmentVariableKey(environmentVariable);
    }

    private static class EnvironmentVariableKey extends Key<String, String> {
      protected EnvironmentVariableKey(String key) {
        super("environment-variable", key);
      }
    }
  }

  public static EnvironmentVariable<String> fromEnvironmentVariable(String environmentVariable) {
    return new EnvironmentVariable<>(environmentVariable, STR2STR);
  }

  public static <T> EnvironmentVariable<T> fromEnvironmentVariable(
      Function<String, T> mapper, String environmentVariable) {
    return new EnvironmentVariable<>(environmentVariable, mapper);
  }

  public static EnvironmentVariable<Boolean> fromBoolEnvironmentVariable(
      String environmentVariable) {
    return new EnvironmentVariable<>(environmentVariable, STR2BOOL);
  }

  /**
   * Used to lookup the actual {@link Source}, which contains the mapping function needed to convert
   * {@code V1} into the type expected by the corresponding {@link NodeInput}.
   *
   * @param <K1> The type of the key in the original source -- NEEDS TO BE SAME AS K
   * @param <V1> The type of the value in the original source -- NEEDS TO BE SAME AS V
   */
  public abstract static class Key<K1, V1> implements Serializable {

    private final String type;
    private final K1 name;

    protected Key(String type, K1 key) {
      this.type = type;
      this.name = key;
    }

    @Override
    public String toString() {
      return String.format("Source.Key[type=%s, name=%s]", type, name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, name);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || obj.getClass() != getClass()) return false;
      Key key = (Key) obj;
      return Objects.equals(type, key.type) && Objects.equals(name, key.name);
    }
  }

  public static List<String> stringToStringList(String value) {
    List<String> list = new ArrayList<>();
    if (value == null || value.trim().isEmpty()) return list;
    String[] elems = value.replaceAll("\\s+", " ").trim().split(",");
    for (String elem : elems) {
      String trimmed = elem.trim();
      if (!trimmed.isEmpty()) list.add(trimmed);
    }
    return list;
  }
}
