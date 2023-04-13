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
package com.ms.silverking.cloud.dht.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHA1Digest {
  private static ThreadLocal<MessageDigest> tl = new ThreadLocal<MessageDigest>();

  public static final int BYTES = 20;

  public static MessageDigest getLocalMessageDigest() {
    MessageDigest md;

    // FUTURE - ADD CODE FOR LWT THREADS TO GET THIS WITHOUT THE TL LOOKUP
    // possibly a factory that we pass to LWT to generate our threads
    md = tl.get();
    if (md == null) {
      try {
        md = MessageDigest.getInstance("SHA-1");
        tl.set(md);
      } catch (NoSuchAlgorithmException nsae) {
        throw new RuntimeException("panic");
      }
    }
    md.reset();
    return md;
  }
}
