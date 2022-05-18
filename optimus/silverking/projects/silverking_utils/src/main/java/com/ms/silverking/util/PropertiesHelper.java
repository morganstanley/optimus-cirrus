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
package com.ms.silverking.util;

import java.text.ParseException;
import java.util.Map;
import java.util.Properties;

import com.ms.silverking.text.CoreStringUtil;

/**
 * Simplifies common interaction with java.util.Properties
 */
public class PropertiesHelper {
  private final Properties prop;

  public enum ParseExceptionAction {DefaultOnParseException, RethrowParseException}
  public enum UndefinedAction {DefaultOnUndefined, ExceptionOnUndefined, ZeroOnUndefined}

  public static final PropertiesHelper systemHelper;
  public static final PropertiesHelper envHelper;

  private static final ParseExceptionAction standardParseExceptionAction = ParseExceptionAction.DefaultOnParseException;
  private static final UndefinedAction standardUndefinedAction = UndefinedAction.ExceptionOnUndefined;

  static {
    Properties envProperties;

    envProperties = new Properties();
    for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
      envProperties.setProperty(entry.getKey(), entry.getValue());
    }
    envHelper = new PropertiesHelper(envProperties);
    systemHelper = new PropertiesHelper(System.getProperties());
  }

  public PropertiesHelper(Properties prop) {
    this.prop = prop;
  }

  private void verifyNotDefaultOnUndefined(UndefinedAction undefinedAction) {
    if (undefinedAction == UndefinedAction.DefaultOnUndefined) {
      throw new RuntimeException("Wrong method for DefaultOnUndefined");
    }
  }

  private void throwExceptionOnUndefined(String name) {
    throw new PropertyException("Required property undefined: " + name);
  }

  // String

  String getString(String name, String defaultValue, UndefinedAction undefinedAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      return def;
    } else {
      return getUndefinedValue(name, undefinedAction, null, defaultValue);
    }
  }

  private <T> T getUndefinedValue(String name, UndefinedAction undefinedAction, T zeroValue, T defaultValue) {
    switch (undefinedAction) {
      case ZeroOnUndefined:
        return zeroValue;
      case DefaultOnUndefined:
        return defaultValue;
      case ExceptionOnUndefined:
        throwExceptionOnUndefined(name);
      default:
        throw new RuntimeException("panic");
    }
  }

  public String getString(String name, String defaultValue) {
    return getString(name, defaultValue, UndefinedAction.DefaultOnUndefined);
  }

  public String getString(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getString(name, null, undefinedAction);
  }

  public String getString(String name) {
    return getString(name, standardUndefinedAction);
  }

  // int

  int getInt(String name,
             int defaultValue,
             UndefinedAction undefinedAction,
             ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return Integer.parseInt(def);
      } catch (NumberFormatException nfe) {
        return getExceptionValue(parseExceptionAction, defaultValue, nfe);
      }
    } else {
      return getUndefinedValue(name, undefinedAction, 0, defaultValue);
    }
  }

  private <T> T getExceptionValue(ParseExceptionAction parseExceptionAction,
                                  T defaultValue,
                                  RuntimeException exceptionToThrow) {
    switch (parseExceptionAction) {
      case DefaultOnParseException:
        return defaultValue;
      default:
        throw exceptionToThrow;
    }
  }

  public int getInt(String name, int defaultValue, ParseExceptionAction parseExceptionAction) {
    return getInt(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
  }

  public int getInt(String name, int defaultValue) {
    return getInt(name, defaultValue, UndefinedAction.DefaultOnUndefined, ParseExceptionAction.DefaultOnParseException);
  }

  public int getInt(String name, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getInt(name, 0, undefinedAction, parseExceptionAction);
  }

  public int getInt(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getInt(name, 0, undefinedAction, standardParseExceptionAction);
  }

  public int getInt(String name, ParseExceptionAction parseExceptionAction) {
    return getInt(name, 0, standardUndefinedAction, parseExceptionAction);
  }

  public int getInt(String name) {
    return getInt(name, 0, standardUndefinedAction, standardParseExceptionAction);
  }

  // boolean

  boolean getBoolean(String name,
                     boolean defaultValue,
                     UndefinedAction undefinedAction,
                     ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return CoreStringUtil.parseBoolean(def);
      } catch (ParseException pe) {
        return getExceptionValue(parseExceptionAction, defaultValue, new RuntimeException(pe));
      }
    } else {
      return getUndefinedValue(name, undefinedAction, false, defaultValue);
    }
  }

  public boolean getBoolean(String name, boolean defaultValue, ParseExceptionAction parseExceptionAction) {
    return getBoolean(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
  }

  public boolean getBoolean(String name, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getBoolean(name, false, undefinedAction, parseExceptionAction);
  }

  public boolean getBoolean(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getBoolean(name, false, undefinedAction, standardParseExceptionAction);
  }

  public boolean getBoolean(String name, ParseExceptionAction parseExceptionAction) {
    return getBoolean(name, false, standardUndefinedAction, parseExceptionAction);
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    return getBoolean(name, defaultValue, UndefinedAction.DefaultOnUndefined, standardParseExceptionAction);
  }

  public boolean getBoolean(String name) {
    return getBoolean(name, false, standardUndefinedAction, standardParseExceptionAction);
  }

  // long

  long getLong(String name,
               long defaultValue,
               UndefinedAction undefinedAction,
               ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return Long.parseLong(def);
      } catch (NumberFormatException nfe) {
        return getExceptionValue(parseExceptionAction, defaultValue, nfe);
      }
    } else {
      return getUndefinedValue(name, undefinedAction, 0L, defaultValue);
    }
  }

  public long getLong(String name, long defaultValue, ParseExceptionAction parseExceptionAction) {
    return getLong(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
  }

  public long getLong(String name, UndefinedAction undefinedAction, ParseExceptionAction parseExceptionAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getLong(name, 0, undefinedAction, parseExceptionAction);
  }

  public long getLong(String name, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getLong(name, 0, undefinedAction, standardParseExceptionAction);
  }

  public long getLong(String name, ParseExceptionAction parseExceptionAction) {
    return getLong(name, 0, standardUndefinedAction, parseExceptionAction);
  }

  public long getLong(String name, long defaultValue) {
    return getLong(name,
                   defaultValue,
                   UndefinedAction.DefaultOnUndefined,
                   ParseExceptionAction.DefaultOnParseException);
  }

  public long getLong(String name) {
    return getLong(name, 0, standardUndefinedAction, standardParseExceptionAction);
  }

  // double

  private double getDouble(String name,
                   double defaultValue,
                   UndefinedAction undefinedAction,
                   ParseExceptionAction parseExceptionAction) {
    String def;

    def = prop.getProperty(name);
    if (def != null) {
      try {
        return Double.parseDouble(def);
      } catch (NumberFormatException nfe) {
        return getExceptionValue(parseExceptionAction, defaultValue, nfe);
      }
    } else {
      return getUndefinedValue(name, undefinedAction, 0.0, defaultValue);
    }
  }

  public double getDouble(String name, double defaultValue) {
    return getDouble(name,
                     defaultValue,
                     UndefinedAction.DefaultOnUndefined,
                     ParseExceptionAction.DefaultOnParseException);
  }

  // Enum

  public <T extends Enum> T getEnum(String name,
                                    T defaultValue,
                                    UndefinedAction undefinedAction,
                                    ParseExceptionAction parseExceptionAction) {
    if (defaultValue == null) {
      throw new RuntimeException("defaultValue cannot be null for enum");
    } else {
      String def;

      def = prop.getProperty(name);
      if (def != null) {
        try {
          return (T) defaultValue.valueOf(defaultValue.getClass(), def);
        } catch (IllegalArgumentException iae) {
          return getExceptionValue(parseExceptionAction, defaultValue, iae);
        }
      } else {
        return getUndefinedValue(name,
                                 undefinedAction != null
                                 ? undefinedAction
                                 : UndefinedAction.DefaultOnUndefined,
                                 null,
                                 defaultValue);
      }
    }
  }

  public <T extends Enum> T getEnum(String name, T defaultValue, ParseExceptionAction parseExceptionAction) {
    return getEnum(name, defaultValue, UndefinedAction.DefaultOnUndefined, parseExceptionAction);
  }

  public <T extends Enum> T getEnum(String name, T defaultValue, UndefinedAction undefinedAction) {
    verifyNotDefaultOnUndefined(undefinedAction);
    return getEnum(name, defaultValue, undefinedAction, standardParseExceptionAction);
  }

  public <T extends Enum> T getEnum(String name, T defaultValue) {
    return getEnum(name, defaultValue, UndefinedAction.DefaultOnUndefined, standardParseExceptionAction);
  }
}
