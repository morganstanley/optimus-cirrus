/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */
/* This is a path of the same file in apache's httpcomponents library from httpcore-4.4.9.jar.
 * Modifications to this file were made by Morgan Stanley to alter the exception handling.
 * Changes are delimited by <+ms> / <-ms>.
 *
 * For those changes only:
 *
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

package org.apache.http.ssl;

import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;

/**
 * {@link SSLContext} factory methods.
 *
 * @since 4.4
 */
public class SSLContexts {

  /**
   * Creates default factory based on the standard JSSE trust material ({@code cacerts} file in the
   * security properties directory). System properties are not taken into consideration.
   *
   * @return the default SSL socket factory
   */
  public static SSLContext createDefault() throws SSLInitializationException {
    // <+ms>
    // Here we are working around JIB, which uses this method (createDefault)
    // rather than createSystemDefault, which heeds -Djavax.ssl.net.keyStore* properties.
    // (https://github.com/GoogleContainerTools/jib/issues/2585)
    // When this is fixed and the fix onboarded, we can remove this patch.
    return createSystemDefault();
    // <-ms>
  }

  /**
   * Creates default SSL context based on system properties. This method obtains default SSL context
   * by calling {@code SSLContext.getInstance("Default")}. Please note that {@code Default}
   * algorithm is supported as of Java 6. This method will fall back onto {@link #createDefault()}
   * when {@code Default} algorithm is not available.
   *
   * @return default system SSL context
   */
  public static SSLContext createSystemDefault() throws SSLInitializationException {
    try {
      return SSLContext.getDefault();
    } catch (final NoSuchAlgorithmException ex) {
      // <+ms>
      throw new SSLInitializationException(ex.getMessage(), ex);
      // <-ms>
    }
  }

  /**
   * Creates custom SSL context.
   *
   * @return default system SSL context
   */
  public static SSLContextBuilder custom() {
    return SSLContextBuilder.create();
  }
}
