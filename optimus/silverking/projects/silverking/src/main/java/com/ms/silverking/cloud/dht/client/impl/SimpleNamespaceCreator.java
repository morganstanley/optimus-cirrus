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
package com.ms.silverking.cloud.dht.client.impl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.ms.silverking.cloud.dht.common.Namespace;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class SimpleNamespaceCreator implements NamespaceCreator {

  private static Logger log = LoggerFactory.getLogger(SimpleNamespaceCreator.class);

  public SimpleNamespaceCreator() {}

  @Override
  public Namespace createNamespace(String namespace) {
    try {
      MessageDigest md;
      byte[] bytes;

      bytes = namespace.getBytes();
      // FUTURE - think about speeding this up
      // by using a thread-local digest
      // like MD5KeyDigest
      md = MessageDigest.getInstance("MD5");
      md.update(bytes, 0, bytes.length);
      return new SimpleNamespace(md.digest());
    } catch (NoSuchAlgorithmException nsae) {
      throw new RuntimeException("panic");
    }
  }

  public static void main(String[] args) {
    for (String arg : args) {
      Namespace ns;

      ns = new SimpleNamespaceCreator().createNamespace(arg);
      log.info("{}  {}  {}", arg, ns.contextAsLong(), ns.contextAsLong());
    }
  }
}
