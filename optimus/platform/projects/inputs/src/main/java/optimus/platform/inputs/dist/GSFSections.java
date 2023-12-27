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
package optimus.platform.inputs.dist;

import java.util.Map;
import java.util.function.Function;

import optimus.platform.inputs.NodeInput;

/** Describes the section of the EngineSpec where the {@link NodeInput} should be stored */
public class GSFSections {

  public abstract static class GSFSection<T, R> {

    private final boolean requiresRestart;

    protected GSFSection(boolean requiresRestart) {
      this.requiresRestart = requiresRestart;
    }

    public boolean requiresRestart() {
      return requiresRestart;
    }

    public abstract R invertAndFormat(T value);

    public EngineSpecJavaOpt<T> asEngineSpecJavaOpt() {
      return null;
    }

    public EngineSpecEnvVar<T> asEngineSpecEnvVar() {
      return null;
    }

    public SafeJavaOpt<T> asSafeJavaOpt() {
      return null;
    }

    public boolean isNone() {
      return false;
    }

    public final boolean isSafeJavaOpt() {
      return asSafeJavaOpt() != null;
    }

    public final boolean isEngineSpecJavaOpt() {
      return asEngineSpecJavaOpt() != null;
    }

    public final boolean isEngineSpecEnvVar() {
      return asEngineSpecEnvVar() != null;
    }
  }

  /** Implies that the {@link NodeInput} has no connection to GSF whatsoever */
  private static class None<T> extends GSFSection<T, Void> {

    protected None() {
      super(false);
    }

    @Override
    public Void invertAndFormat(T value) {
      throw new IllegalStateException("Should not be called!");
    }

    @Override
    public boolean isNone() {
      return true;
    }
  }

  public static <T> GSFSection<T, Void> none() {
    return new None<>();
  }

  /**
   * Used to invert a {@link NodeInput} representing java properties into a string representation to
   * be used in engine spec
   */
  public static class EngineSpecJavaOpt<T> extends GSFSection<T, String> {

    private final String javaProperty;
    private final String pattern;
    private final Function<T, String> invert;

    EngineSpecJavaOpt(String javaProperty, String pattern, Function<T, String> invert) {
      super(true);
      this.javaProperty = javaProperty;
      this.pattern = pattern;
      this.invert = invert;
    }

    @Override
    public String invertAndFormat(T value) {
      return format(javaProperty, pattern, invert.apply(value));
    }

    public String invert(T value) {
      return invert.apply(value);
    }

    public String javaProperty() {
      return javaProperty;
    }

    public String pattern() {
      return pattern;
    }

    @Override
    public EngineSpecJavaOpt<T> asEngineSpecJavaOpt() {
      return this;
    }

    public static String format(String javaProperty, String pattern, String value) {
      return javaProperty == null
          ? String.format(pattern, value)
          : String.format(pattern, javaProperty, value);
    }
  }

  /**
   * @see EngineSpecJavaOpt
   */
  public static <T> EngineSpecJavaOpt<T> newEngineSpecJavaOpt(
      String javaProperty, Function<T, String> invert) {
    return new EngineSpecJavaOpt<>(javaProperty, "-D%s=%s", invert);
  }

  public static <T> EngineSpecJavaOpt<T> newEngineSpecJvmOpt(
      String pattern, Function<T, String> invert) {
    return new EngineSpecJavaOpt<>(null, pattern, invert);
  }

  /**
   * @see EngineSpecJavaOpt
   */
  public static EngineSpecJavaOpt<String> newEngineSpecJavaFlag() {
    return new EngineSpecJavaOpt<>(null, "%s", Function.identity());
  }

  /**
   * Used to invert a {@link NodeInput} representing environment variables into a string
   * representation to be used in engine spec
   */
  public static class EngineSpecEnvVar<T> extends GSFSection<T, Map.Entry<String, String>> {

    private final Function<T, Map.Entry<String, String>> invertAndFormat;

    EngineSpecEnvVar(Function<T, Map.Entry<String, String>> invertAndFormat) {
      super(true);
      this.invertAndFormat = invertAndFormat;
    }

    @Override
    public Map.Entry<String, String> invertAndFormat(T value) {
      return invertAndFormat.apply(value);
    }

    @Override
    public EngineSpecEnvVar<T> asEngineSpecEnvVar() {
      return this;
    }

    static <T> Function<T, Map.Entry<String, String>> invertAndFormatEnvVar(
        String environmentVariable, Function<T, String> invert) {
      return value ->
          new Map.Entry<String, String>() {
            @Override
            public String getKey() {
              return environmentVariable;
            }

            @Override
            public String getValue() {
              return invert.apply(value);
            }

            @Override
            public String setValue(String value) {
              throw new IllegalStateException("Should never be called");
            }
          };
    }
  }

  /**
   * @see EngineSpecEnvVar
   */
  public static <T> EngineSpecEnvVar<T> newEngineSpecEnvVar(
      String environmentVariable, Function<T, String> invert) {
    return new EngineSpecEnvVar<>(
        EngineSpecEnvVar.invertAndFormatEnvVar(environmentVariable, invert));
  }

  /**
   * Used to invert a {@link NodeInput} representing Optimus safe system properties into a string
   * representation (needed for interop)
   */
  public static class SafeJavaOpt<T> extends GSFSection<T, String> {

    private final Function<T, String> invertAndFormat;

    protected SafeJavaOpt(Function<T, String> invertAndFormat) {
      super(false);
      this.invertAndFormat = invertAndFormat;
    }

    @Override
    public String invertAndFormat(T value) {
      return invertAndFormat.apply(value);
    }

    @Override
    public SafeJavaOpt<T> asSafeJavaOpt() {
      return this;
    }

    static <T> Function<T, String> invertAndFormatSafeJavaOpt(
        String javaProperty, Function<T, String> invert) {
      return value -> String.format("-D%s=%s", javaProperty, invert.apply(value));
    }
  }

  /**
   * @see SafeJavaOpt
   */
  public static <T> SafeJavaOpt<T> newSafeJavaOpt(String javaProperty, Function<T, String> invert) {
    return new SafeJavaOpt<>(SafeJavaOpt.invertAndFormatSafeJavaOpt(javaProperty, invert));
  }
}
