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

package msjava.base.util.internal;
import org.slf4j.Logger;
import org.springframework.util.StringUtils;
import msjava.base.slf4j.ContextLogger;
public class SystemPropertyUtils {
    private static final Logger DEFAULT_LOGGER = ContextLogger.safeLogger();
    private static Logger orDefault(Logger logger) {
        return logger != null ? logger : DEFAULT_LOGGER;
    }
    
    private static final String SYSTEM_PROPERTY_PREFIX = " System property '";
    private static final String SCALE = "kKmMgG";
    private final static String[] platformKeyPrefixes = { "java", "os", "file", "path", "line", "user", "javax", "sun",
            "com.sun", "com.ibm" };
    
    public static String getProperty(String key, Logger logger) {
        return getProperty(key, null, logger);
    }
    
    public static String getProperty(String key, String defaultValue, Logger logger) {
        logger = orDefault(logger);
        final String value = System.getProperty(key);
        final boolean isPlatform = isPlatformProperty(key);
        final String platform = isPlatform ? " platform" : "";
        if (value == null) {
            if (logger.isDebugEnabled()) {
                if (defaultValue != null) {
                    logger.debug("The" + platform + SYSTEM_PROPERTY_PREFIX + key
                            + "' is not set. Using default value: '" + defaultValue + "'.");
                } else {
                    logger.debug("No value set for" + platform + SYSTEM_PROPERTY_PREFIX + key + "'.");
                }
            }
            return defaultValue;
        } else {
            if (!isPlatform) {
                if (logger.isInfoEnabled()) {
                    logger.info("Using" + platform + SYSTEM_PROPERTY_PREFIX + key + "' with value '" + value + "'.");
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Using" + platform + SYSTEM_PROPERTY_PREFIX + key + "' with value '" + value + "'.");
                }
            }
        }
        return value;
    }
    
    public static boolean getBoolean(String key, Logger logger) {
        return getBoolean(key, false, logger);
    }
    
    public static boolean getBoolean(String key, boolean defaultValue, Logger logger) {
        logger = orDefault(logger);
        final String value = System.getProperty(key);
        final boolean isPlatform = isPlatformProperty(key);
        final String platform = isPlatform ? " platform" : "";
        if (value == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("No value set for" + platform + SYSTEM_PROPERTY_PREFIX + key + "'. Returning with '"
                        + defaultValue + "' default boolean value.");
            }
        } else {
            if (value.equalsIgnoreCase("true")) {
                if (logger.isInfoEnabled()) {
                    logger.info("Returning TRUE for" + platform + SYSTEM_PROPERTY_PREFIX + key + "' with value '"
                            + value + "'.");
                }
                return true;
            } else if (value.equalsIgnoreCase("false")) {
                if (logger.isInfoEnabled()) {
                    logger.info("Returning FALSE for" + platform + SYSTEM_PROPERTY_PREFIX + key + "' with value '"
                            + value + "'.");
                }
                return false;
            } else {
                if (logger.isInfoEnabled()) {
                    logger.info("Returning default '" + defaultValue + "' boolean value for" + platform
                            + SYSTEM_PROPERTY_PREFIX + key + "' with value '" + value + "'.");
                }
            }
        }
        return defaultValue;
    }
    
    public static Integer getInteger(String key, int defaultValue, Logger logger) {
        logger = orDefault(logger);
        final Integer value = Integer.getInteger(key);
        final boolean isPlatform = isPlatformProperty(key);
        final String platform = isPlatform ? " platform" : "";
        if (value == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("No or incorrect integer value set for" + platform + SYSTEM_PROPERTY_PREFIX + key
                        + "'. Returning with default integer value: " + defaultValue);
            }
            return defaultValue;
        } else {
            if (logger.isInfoEnabled()) {
                logger.info(
                        "Returning integer value " + value + " for" + platform + SYSTEM_PROPERTY_PREFIX + key + "'.");
            }
        }
        return value;
    }
    
    public static Long getLong(String key, long defaultValue, Logger logger) {
        logger = orDefault(logger);
        final Long value = Long.getLong(key);
        final boolean isPlatform = isPlatformProperty(key);
        final String platform = isPlatform ? " platform" : "";
        if (value == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("No or incorrect long value set for" + platform + SYSTEM_PROPERTY_PREFIX + key
                        + "'. Returning with default long value: " + defaultValue);
            }
            return defaultValue;
        } else {
            if (logger.isInfoEnabled()) {
                logger.info("Returning long value " + value + " for" + platform + SYSTEM_PROPERTY_PREFIX + key + "'.");
            }
        }
        return value;
    }
    
    public static Integer getScaledInteger(String key, int defaultValue, Logger logger) {
        logger = orDefault(logger);
        String value = System.getProperty(key);
        final boolean isPlatform = isPlatformProperty(key);
        final String platform = isPlatform ? " platform" : "";
        if (!StringUtils.hasLength(value)) {
            if (logger.isDebugEnabled()) {
                logger.debug("No scaled integer value set for" + platform + SYSTEM_PROPERTY_PREFIX + key
                        + "'. Returning with default int value: " + defaultValue);
            }
            return defaultValue;
        }
        char lastChar = value.charAt(value.length() - 1);
        int indexOf = SCALE.indexOf(lastChar);
        int scale = 1;
        if (indexOf != -1) {
            
            value = value.substring(0, value.length() - 1);
            scale = 1 << (10 * (indexOf / 2 + 1));
        }
        try {
            int intValue = scale * Integer.decode(value);
            if (logger.isInfoEnabled()) {
                logger.info(
                        "Returning int value " + intValue + " for" + platform + SYSTEM_PROPERTY_PREFIX + key + "'.");
            }
            return intValue;
        } catch (NumberFormatException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Invalid scaled integer value set for" + platform + SYSTEM_PROPERTY_PREFIX + key
                        + "'. Returning with default int value: " + defaultValue);
            }
            return defaultValue;
        }
    }
    
    public static Long getScaledLong(String key, long defaultValue, Logger logger) {
        logger = orDefault(logger);
        String value = System.getProperty(key);
        final boolean isPlatform = isPlatformProperty(key);
        final String platform = isPlatform ? " platform" : "";
        if (!StringUtils.hasLength(value)) {
            if (logger.isDebugEnabled()) {
                logger.debug("No scaled integer value set for" + platform + SYSTEM_PROPERTY_PREFIX + key
                        + "'. Returning with default long value: " + defaultValue);
            }
            return defaultValue;
        }
        char lastChar = value.charAt(value.length() - 1);
        int indexOf = SCALE.indexOf(lastChar);
        int scale = 1;
        if (indexOf != -1) {
            
            value = value.substring(0, value.length() - 1);
            scale = 1 << (10 * (indexOf / 2 + 1));
        }
        try {
            long longValue = scale * Long.decode(value);
            if (logger.isInfoEnabled()) {
                logger.info(
                        "Returning long value " + longValue + " for" + platform + SYSTEM_PROPERTY_PREFIX + key + "'.");
            }
            return longValue;
        } catch (NumberFormatException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Invalid scaled integer value set for" + platform + SYSTEM_PROPERTY_PREFIX + key
                        + "'. Returning with default long value: " + defaultValue);
            }
            return defaultValue;
        }
    }
    
    public static void warnIfDepricatedPropertySet(String key, String msg, Logger logger) {
        logger = orDefault(logger);
        String val = System.getProperty(key);
        final boolean isPlatform = isPlatformProperty(key);
        final String platform = isPlatform ? " platform" : "";
        if (val != null && logger.isWarnEnabled()) {
            logger.warn("The " + platform + SYSTEM_PROPERTY_PREFIX + key + " is deprecated : " + msg);
        }
    }
    
    public static String setProperty(String key, String value, Logger logger) {
        logger = orDefault(logger);
        String oldValue = System.setProperty(key, value);
        String platform = isPlatformProperty(key) ? " platform" : "";
        if (logger.isInfoEnabled()) {
            logger.info("The" + platform + SYSTEM_PROPERTY_PREFIX + key + "' was set to '" + value + "'.");
        }
        return oldValue;
    }
    
    private static boolean isPlatformProperty(String key) {
        for (String prefix : platformKeyPrefixes) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
