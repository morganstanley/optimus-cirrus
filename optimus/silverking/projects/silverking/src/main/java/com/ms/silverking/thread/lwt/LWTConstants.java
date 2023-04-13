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
package com.ms.silverking.thread.lwt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common LWT constants.
 */
public class LWTConstants {
  public static final String LWTEnvPrefix = "LWT_";
  public static final String numProcessorsEnvVar = LWTEnvPrefix + "NUM_PROCESSORS";
  public static final int numProcessors;

  public static final String propertyBase = LWTConstants.class.getPackage().getName();

  private static Logger log = LoggerFactory.getLogger(LWTConstants.class);

  private static final int _defaultMaxDirectCallDepth = 100;
  public static final int defaultMaxDirectCallDepth;
  static final String defaultMaxDirectCallDepthProperty = propertyBase + ".DefaultMaxDirectCallDepth";
  private static final int _defaultIdleThreadThreshold = 1;
  public static final int defaultIdleThreadThreshold;
  static final String defaultIdleThreadThresholdProperty = propertyBase + ".DefaultIdleThreadThreshold";

  static final String lwtControllerCheckInterval = propertyBase + ".LWTControllerCheckInterval";

  static final String enableLoggingProperty = propertyBase + ".EnableLogging";
  private static final boolean _enableLogging = false;
  public static final boolean enableLogging;

  static final String verboseProperty = propertyBase + ".Verbose";
  private static final boolean _verbose = false;
  public static final boolean verbose;

  static {
    String val;

    val = System.getenv(numProcessorsEnvVar);
    if (val != null) {
      numProcessors = Integer.parseInt(val);
    } else {
      numProcessors = Runtime.getRuntime().availableProcessors();
    }

    val = System.getProperty(defaultMaxDirectCallDepthProperty);
    if (val != null) {
      defaultMaxDirectCallDepth = Integer.parseInt(val);
    } else {
      defaultMaxDirectCallDepth = _defaultMaxDirectCallDepth;
    }
    if (log.isDebugEnabled()) {
      log.debug("{} : {}",defaultMaxDirectCallDepthProperty , defaultMaxDirectCallDepth);
    }

    val = System.getProperty(defaultIdleThreadThresholdProperty);
    if (val != null) {
      defaultIdleThreadThreshold = Integer.parseInt(val);
    } else {
      defaultIdleThreadThreshold = _defaultIdleThreadThreshold;
    }
    if (log.isDebugEnabled()) {
      log.debug("{} : {}", defaultIdleThreadThresholdProperty , defaultIdleThreadThreshold);
    }

    val = System.getProperty(enableLoggingProperty);
    if (val != null) {
      enableLogging = Boolean.parseBoolean(val);
    } else {
      enableLogging = _enableLogging;
    }
    if (log.isDebugEnabled()) {
      log.debug("{} : {}", enableLoggingProperty , enableLogging);
    }

    val = System.getProperty(verboseProperty);
    if (val != null) {
      verbose = Boolean.parseBoolean(val);
    } else {
      verbose = _verbose;
    }
    if (log.isDebugEnabled()) {
      log.debug("{} : {}", verboseProperty , verbose);
    }
  }
}
