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
package com.ms.silverking.alert;

import com.google.common.base.Preconditions;

public class Alert {
  private final String context;
  private final int level;
  private final String key;
  private final String message;
  private final String data;

  public Alert(String context, int level, String key, String message, String data) {
    Preconditions.checkNotNull(context);
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(message);
    this.context = context;
    this.level = level;
    this.key = key;
    this.message = message;
    this.data = data != null ? data : "";
  }

  public String getContext() {
    return context;
  }

  public int getLevel() {
    return level;
  }

  public String getKey() {
    return key;
  }

  public String getMessage() {
    return message;
  }

  public String getData() {
    return data;
  }

  @Override
  public int hashCode() {
    return context.hashCode() ^ Integer.hashCode(level) ^ key.hashCode() ^ message.hashCode() ^ data.hashCode();
  }

  // if .equals() is called with an object that is not `Alert`, ClassCastException will be thrown
  @Override
  public boolean equals(Object obj) {
    Alert alert = (Alert) obj;
    return context.equals(alert.context) &&
           level == alert.level &&
           key.equals(alert.key) &&
           message.equals(alert.message) &&
           data.equals(alert.data);
  }

  @Override
  public String toString() {
    return context + ":" + level + ":" + key + ":" + message;
  }
}
