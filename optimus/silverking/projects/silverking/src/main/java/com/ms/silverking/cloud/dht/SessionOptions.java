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
package com.ms.silverking.cloud.dht;

import java.util.HashMap;
import java.util.Map;

import com.ms.silverking.cloud.dht.client.ClientDHTConfiguration;
import com.ms.silverking.cloud.dht.client.ClientDHTConfigurationProvider;
import com.ms.silverking.cloud.dht.client.SessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.client.SimpleSessionEstablishmentTimeoutController;
import com.ms.silverking.cloud.dht.gridconfig.SKGridConfiguration;
import com.ms.silverking.text.FieldsRequirement;
import com.ms.silverking.text.ObjectDefParser2;
import com.ms.silverking.util.PropertiesHelper;
import com.ms.silverking.util.PropertiesHelper.UndefinedAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Options for a DHTSession.
 */
public final class SessionOptions {

  private static Logger log = LoggerFactory.getLogger(SessionOptions.class);

  private final ClientDHTConfiguration dhtConfig;
  private final String preferredServer;
  private final SessionEstablishmentTimeoutController timeoutController;
  private final SessionPolicyOnDisconnect sessionPolicyOnDisconnect;

  private static final String defaultTimeoutControllerProperty =
      SessionEstablishmentTimeoutController.class.getName() + ".DefaultSETimeoutController";
  private static final SessionEstablishmentTimeoutController defaultDefaultTimeoutController =
      new SimpleSessionEstablishmentTimeoutController(
      14, 2 * 60 * 1000, 8 * 60 * 1000);

  private static SessionEstablishmentTimeoutController defaultTimeoutController;
  private static final boolean debugDefaultTimeoutController = false;

  private static final Map<String, String> sampleGCMap;

  static {
    sampleGCMap = new HashMap<>();
    sampleGCMap.put(ClientDHTConfiguration.nameVar, "samplename");
    sampleGCMap.put(ClientDHTConfiguration.portVar, "80");
    sampleGCMap.put(ClientDHTConfiguration.zkLocVar, "localhost:0");
  }

  private static final SessionOptions template = new SessionOptions(new SKGridConfiguration("samplegc", sampleGCMap),
      "localhost", defaultDefaultTimeoutController, SessionPolicyOnDisconnect.DoNothing);

  static {
    ObjectDefParser2.addParser(template, FieldsRequirement.ALLOW_INCOMPLETE);
  }

  static {
    String def;

    def = PropertiesHelper.systemHelper.getString(defaultTimeoutControllerProperty, UndefinedAction.ZeroOnUndefined);
    if (debugDefaultTimeoutController) {
      log.debug("defaultTimeoutControllerProperty {}", defaultTimeoutControllerProperty);
      log.debug("def {}", def);
    }
    if (def != null) {
      //defaultTimeoutController = SimpleConnectionEstablishmentTimeoutController.parse(def);
      defaultTimeoutController = ObjectDefParser2.parse(SessionEstablishmentTimeoutController.class, def);
    } else {
      defaultTimeoutController = defaultDefaultTimeoutController;
    }
    if (debugDefaultTimeoutController) {
      log.debug("defaultTimeoutController {}", defaultTimeoutController);
    }
  }

  public void setDefaultTimeoutController(SessionEstablishmentTimeoutController newDefaultTimeoutController) {
    defaultTimeoutController = newDefaultTimeoutController;
  }

  public static SessionEstablishmentTimeoutController getDefaultTimeoutController() {
    return defaultTimeoutController;
  }

  public SessionOptions(ClientDHTConfigurationProvider dhtConfigProvider, String preferredServer,
      SessionEstablishmentTimeoutController timeoutController) {
    this(dhtConfigProvider, preferredServer, timeoutController, SessionPolicyOnDisconnect.DoNothing);
  }

  /**
   * Create a fully-specified SessionOptions instance
   */
  public SessionOptions(ClientDHTConfigurationProvider dhtConfigProvider, String preferredServer,
      SessionEstablishmentTimeoutController timeoutController, SessionPolicyOnDisconnect onDisconnect) {
    if (dhtConfigProvider == null) {
      this.preferredServer = preferredServer;
      this.dhtConfig = null;
    } else {
      this.dhtConfig = dhtConfigProvider.getClientDHTConfiguration();
      this.preferredServer = preferredServer;
    }
    this.timeoutController = timeoutController;
    this.sessionPolicyOnDisconnect = onDisconnect;
  }

  /**
   * Create a SessionOptions instance with a default timeout controller and disconnect policy.
   *
   * @param dhtConfigProvider TODO
   * @param preferredServer   TODO
   */
  public SessionOptions(ClientDHTConfigurationProvider dhtConfigProvider, String preferredServer) {
    this(dhtConfigProvider, preferredServer, getDefaultTimeoutController(), SessionPolicyOnDisconnect.DoNothing);
  }

  /**
   * Create a SessionOptions instance with the default preferredServer and timeout controller
   *
   * @param dhtConfigProvider TODO
   */
  public SessionOptions(ClientDHTConfigurationProvider dhtConfigProvider) {
    this(dhtConfigProvider, null);
  }

  /**
   * Return a new SessionOptions object with the specified dhtConfig
   *
   * @param dhtConfig the new dhtConfig
   * @return a modified SessionOptions object
   */
  public SessionOptions dhtConfig(ClientDHTConfiguration dhtConfig) {
    return new SessionOptions(dhtConfig, preferredServer, timeoutController, sessionPolicyOnDisconnect);
  }

  /**
   * Return a new SessionOptions object with the specified preferredServer
   *
   * @param preferredServer the new preferredServer
   * @return a modified SessionOptions object
   */
  public SessionOptions preferredServer(String preferredServer) {
    return new SessionOptions(dhtConfig, preferredServer, timeoutController, sessionPolicyOnDisconnect);
  }

  /**
   * Return a new SessionOptions object with the specified timeoutController
   *
   * @param timeoutController the new timeoutController
   * @return a modified SessionOptions object
   */
  public SessionOptions timeoutController(SessionEstablishmentTimeoutController timeoutController) {
    return new SessionOptions(dhtConfig, preferredServer, timeoutController, sessionPolicyOnDisconnect);
  }

  /**
   * Return a new SessionOptions object with the specified sessionPolicyOnDisconnect
   *
   * @param sessionPolicyOnDisconnect the new sessionPolicyOnDisconnect
   * @return a modified SessionOptions object
   */
  public SessionOptions sessionPolicyOnDisconnect(SessionPolicyOnDisconnect sessionPolicyOnDisconnect) {
    return new SessionOptions(dhtConfig, preferredServer, timeoutController, sessionPolicyOnDisconnect);
  }

  /**
   * Return the ClientDHTConfiguration
   *
   * @return the ClientDHTConfiguration
   */
  public ClientDHTConfiguration getDHTConfig() {
    return dhtConfig;
  }

  /**
   * Return the preferrsedServer
   *
   * @return the preferrsedServer
   */
  public String getPreferredServer() {
    return preferredServer;
  }

  /**
   * Return the timeoutController
   *
   * @return the timeoutController
   */
  public SessionEstablishmentTimeoutController getTimeoutController() {
    return timeoutController;
  }

  /**
   * Return the sessionPolicyOnDisconnect
   *
   * @return the sessionPolicyOnDisconnect
   */
  public SessionPolicyOnDisconnect getSessionPolicyOnDisconnect() {
    return sessionPolicyOnDisconnect;
  }

  @Override
  public int hashCode() {
    return dhtConfig.hashCode() ^ preferredServer.hashCode() ^ timeoutController.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    SessionOptions o;

    o = (SessionOptions) obj;
    return this.dhtConfig.equals(o.dhtConfig) && this.preferredServer.equals(
        o.preferredServer) && this.timeoutController.equals(o.timeoutController);
  }

  @Override
  public String toString() {
    return ObjectDefParser2.objectToString(this);
  }

  public static SessionOptions parse(String def) {
    return ObjectDefParser2.parse(SessionOptions.class, def);
  }
}
