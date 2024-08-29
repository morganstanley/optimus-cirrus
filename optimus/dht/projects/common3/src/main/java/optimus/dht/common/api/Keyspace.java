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
package optimus.dht.common.api;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

/**
 * Class representing a DHT keyspace. Keyspaces are used to divide server memory into independent
 * segments. Each segment can have a different value for the same key.
 *
 * <p>Keyspace is essentially a map with string keys and string values. Two keys have special
 * meaning:
 *
 * <ul>
 *   <li>"name" - short and descriptive primary name of the keyspace. The only <b>mandatory</b> key.
 *   <li>"allocation" - used for allocating space on the server side
 * </ul>
 *
 * Other keys can have arbitrary names. Keys and values cannot use following characters: { } , =
 * Keys cannot start with $ character.
 *
 * <p>Use of other special characters is discouraged.
 *
 * <p>Keyspaces are considered equal only if all key/values pairs match.
 */
public class Keyspace implements Serializable {

  public static final String FORBIDDEN_CHARACTERS = "{},=";
  public static final String SPECIAL_PREFIX = "$";
  public static final String TRANSIENT_PREFIX = "#";

  private final String name;
  private final String allocation;
  private final Map<String, String> extraAttrs;
  private final int hashCode;

  private Keyspace(String name, String allocation, Map<String, String> extraAttrs) {
    Preconditions.checkNotNull(name, "name cannot be null");
    this.name = name;
    this.allocation = allocation;
    this.extraAttrs = extraAttrs;
    this.hashCode = Objects.hash(name, allocation, extraAttrs);
  }

  private static String verifyString(String string) {
    if (string != null && StringUtils.containsAny(string, FORBIDDEN_CHARACTERS)) {
      throw new IllegalArgumentException(
          "String " + string + " contains illegal characters [" + FORBIDDEN_CHARACTERS + "]");
    }
    return string;
  }

  /**
   * Creates a keyspace from the primary name.
   *
   * @param name keyspace's primary name
   * @return object representing keyspace with given primary name
   */
  public static Keyspace of(String name) {
    return new Keyspace(verifyString(name), null, Collections.emptyMap());
  }

  /**
   * Creates a keyspace from the primary name and allocation.
   *
   * @param name keyspace's primary name
   * @param allocation keyspace's allocation
   * @return object representing keyspace with given primary name and allocation
   */
  public static Keyspace of(String name, String allocation) {
    return new Keyspace(verifyString(name), verifyString(allocation), Collections.emptyMap());
  }

  public static KeyspaceBuilder builder() {
    return new KeyspaceBuilder();
  }

  public static KeyspaceBuilder from(Keyspace keyspace) {
    return new KeyspaceBuilder(keyspace);
  }

  public String name() {
    return name;
  }

  public String allocation() {
    return allocation;
  }

  public boolean isTransient() {
    for (String attrKey : extraAttrs.keySet()) {
      if (attrKey.startsWith(TRANSIENT_PREFIX)) {
        return true;
      }
    }
    return false;
  }

  public Map<String, String> extraAttrs() {
    return extraAttrs;
  }

  public String extraAttr(String attributeName) {
    return extraAttrs.get(attributeName);
  }

  public boolean isUnder(Keyspace that) {
    // Sometimes we want to partially match a keyspace. A.isUnder(B) is true iff allocation and name
    // are
    // equal between A and B, and all attributes of B are included in A.
    return Objects.equals(name, that.name)
        && Objects.equals(allocation, that.allocation)
        && extraAttrs.entrySet().containsAll(that.extraAttrs.entrySet());
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Keyspace that = (Keyspace) o;
    return hashCode == that.hashCode
        && Objects.equals(name, that.name)
        && Objects.equals(allocation, that.allocation)
        && Objects.equals(extraAttrs, that.extraAttrs);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{$name=").append(name);
    if (allocation != null) {
      sb.append(",$allocation=").append(allocation);
    }
    if (extraAttrs != null && !extraAttrs.isEmpty()) {
      sb.append(",");
      Joiner.on(",").withKeyValueSeparator("=").appendTo(sb, extraAttrs);
    }
    sb.append("}");
    return sb.toString();
  }

  public static Keyspace parseFromString(String string) {

    if (string.startsWith("{") && string.endsWith("}")) {
      String keyValues = string.substring(1, string.length() - 1);
      Map<String, String> map =
          Splitter.on(",")
              .omitEmptyStrings()
              .trimResults()
              .withKeyValueSeparator("=")
              .split(keyValues);

      Map<String, String> copy = new HashMap<>(map);

      return builder()
          .name(copy.remove("$name"))
          .allocation(copy.remove("$allocation"))
          .attrs(copy)
          .build();

    } else {
      return Keyspace.of(string);
    }
  }

  public static Keyspace fastBuildUnsafe(
      String name, String allocation, Map<String, String> extraAttrs) {
    return new Keyspace(name, allocation, extraAttrs);
  }

  public static class KeyspaceBuilder {
    private String name;
    private String allocation;
    private Map<String, String> extraAttrs;

    public KeyspaceBuilder() {
      this.allocation = null;
      this.extraAttrs = new HashMap<>();
    }

    public KeyspaceBuilder(Keyspace keyspace) {
      this.name = keyspace.name;
      this.allocation = keyspace.allocation;
      this.extraAttrs = new HashMap<>(keyspace.extraAttrs);
    }

    public String name() {
      return name;
    }

    public KeyspaceBuilder name(String name) {
      this.name = verifyString(name);
      return this;
    }

    public String allocation() {
      return allocation;
    }

    public KeyspaceBuilder allocation(String allocation) {
      this.allocation = verifyString(allocation);
      return this;
    }

    public Map<String, String> extraAttrs() {
      return extraAttrs;
    }

    public KeyspaceBuilder attr(String name, String value) {
      Preconditions.checkNotNull(name);
      Preconditions.checkNotNull(value);
      extraAttrs.put(verifyString(name), verifyString(value));
      return this;
    }

    public KeyspaceBuilder attrs(Map<String, String> attrs) {
      for (Map.Entry<String, String> entry : attrs.entrySet()) {
        attr(entry.getKey(), entry.getValue());
      }
      return this;
    }

    public KeyspaceBuilder removeAttr(String attr) {
      extraAttrs.remove(attr);
      return this;
    }

    public KeyspaceBuilder removeAttrs(Collection<String> attrs) {
      for (String attr : attrs) {
        extraAttrs.remove(attr);
      }
      return this;
    }

    public KeyspaceBuilder removeAllAttrs() {
      extraAttrs.clear();
      return this;
    }

    public Keyspace build() {
      return new Keyspace(
          name, allocation, extraAttrs.isEmpty() ? Collections.emptyMap() : extraAttrs);
    }
  }
}
