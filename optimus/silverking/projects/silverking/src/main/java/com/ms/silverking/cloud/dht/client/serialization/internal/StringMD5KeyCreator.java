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
package com.ms.silverking.cloud.dht.client.serialization.internal;

import java.nio.charset.Charset;
import java.util.Map;

import com.ms.silverking.cloud.dht.common.DHTKey;

public class StringMD5KeyCreator extends BaseKeyCreator<String> {
  private static final boolean use8BitEncoding;
  private static final int noArrayThreshold = 32;

  private final Map<String, DHTKey> cachedKeys;

  private static final int cacheCapacity = 1024;
  private static final int cacheConcurrencyLevel = 8;

  // FUTURE think about allowing users to override

  static {
    use8BitEncoding = Charset.defaultCharset().name().equals("UTF-8");
  }

  public StringMD5KeyCreator() {
    super();
    //cachedKeys = new MapMaker().concurrencyLevel(cacheConcurrencyLevel).initialCapacity(cacheCapacity).makeMap();
    cachedKeys = null;
    // FUTURE - could consider using cached keys
  }

  @Override
  public DHTKey createKey(String key) {
    DHTKey dhtKey;

    //dhtKey = cachedKeys.get(key);
    //if (dhtKey == null) {
    if (use8BitEncoding && key.length() < noArrayThreshold) {
      dhtKey = md5KeyDigest.computeKey(key);
    } else {
      dhtKey = md5KeyDigest.computeKey(key.getBytes());
    }
    //cachedKeys.put(key, dhtKey);
    //}
    return dhtKey;
  }

}
