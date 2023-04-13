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
package com.ms.silverking.cloud.dht.meta;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

public class Util {

  static final int portMin = 4_000;
  static final int portMax = 10_000;

  public static int getFreePort() {
    return getFreePort(portMin, portMax);
  }

  static int getFreePort(int min, int max) {
    int numAttempts = 5;
    for (int i = 0; i < numAttempts; i++) {
      int port = getRandomPort(min, max);
      if (isFree(port)) {
        return port;
      }
    }

    return StaticDHTCreatorOptions.defaultPort;
  }

  static int getRandomPort(int min, int max) {
    Random r = new Random();
    int port = r.nextInt(max - min) + min;
    return port;
  }

  static boolean isFree(int port) {
    try {
      new ServerSocket(port).close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }
}
