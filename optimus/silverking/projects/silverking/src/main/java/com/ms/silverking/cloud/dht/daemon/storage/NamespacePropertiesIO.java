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
package com.ms.silverking.cloud.dht.daemon.storage;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.google.common.base.Preconditions;
import com.ms.silverking.cloud.dht.common.NamespaceProperties;
import com.ms.silverking.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespacePropertiesIO {
  private static final String propertiesFileName = "properties";

  private static Logger log = LoggerFactory.getLogger(NamespacePropertiesIO.class);

  /*
   * Note that creationTime is not serialized by default for NamespaceProperties in order
   * to ensure that DHT-stored namespace properties do not use this field which can vary by observer.
   * Instead, the DHT metadata is consulted for creation times. Here, however, we need to store the
   * creation time, so it is done explicitly.
   */

  private static File propertiesFile(File nsDir) {
    return new File(nsDir, propertiesFileName);
  }

  public static NamespaceProperties read(File nsDir) throws IOException {
    try {
      if (!nsDir.isDirectory()) {
        throw new IOException("NamespacePropertiesIO.read() passed non-directory: " + nsDir);
      }
      return _read(propertiesFile(nsDir));
    } catch (IOException ioe) {
      log.warn("NamespacePropertiesIO.read() failed for: {}", nsDir, ioe);
      throw ioe;
    }
  }

  private static NamespaceProperties _read(File propertiesFile) throws IOException {
    String def;
    int index;

    def = FileUtil.readFileAsString(propertiesFile).trim();
    index = def.lastIndexOf(',');
    if (index < 0) {
      throw new IOException("Failed to parse trailing creationTime from " + def);
    } else {
      long creationTime;

      Preconditions.checkArgument(index <= def.length() - 2);
      creationTime = Long.parseLong(def.substring(index + 1));
      return NamespaceProperties.parse(def.substring(0, index), creationTime);
    }
  }

  public static void rewrite(File nsDir, NamespaceProperties nsProperties) throws IOException {
    if (!nsDir.isDirectory()) {
      throw new IOException("NamespacePropertiesIO.rewrite() passed non-directory: " + nsDir);
    }
    if (propertiesFileExists(nsDir)) {
      _rewrite(propertiesFile(nsDir), nsProperties);
    } else {
      _write(propertiesFile(nsDir), nsProperties);
    }
  }

  public static void write(File nsDir, NamespaceProperties nsProperties) throws IOException {
    if (!nsDir.isDirectory()) {
      throw new IOException("NamespacePropertiesIO.write() passed non-directory: " + nsDir);
    }
    if (propertiesFileExists(nsDir)) {
      NamespaceProperties existingProperties;

      existingProperties = read(nsDir);
      if (!nsProperties.equals(existingProperties)) {
        System.err.println(nsProperties);
        System.err.println(existingProperties);
        System.err.println();
        System.err.flush();
        System.out.println(nsProperties);
        System.out.println(existingProperties);
        System.out.println();
        System.out.flush();
        nsProperties.debugEquals(existingProperties);
        throw new RuntimeException("Existing properties != nsProperties");
      }
    } else {
      _write(propertiesFile(nsDir), nsProperties);
    }
  }

  private static void _write(File propertiesFile, NamespaceProperties nsProperties) throws IOException {
    // For backward compatibility, the legacy format of nsProperties is used
    FileUtil.writeToFile(propertiesFile, nsProperties.toLegacySKDef() + "," + nsProperties.getCreationTime());
  }

  private static void _rewrite(File propertiesFile, NamespaceProperties nsProperties) throws IOException {
    File tempFile;

    // Try not to pollute the old properties, we firstly write new properties into a temp file and atomic move and
    // replace the old one
    tempFile = Files.createTempFile(propertiesFile.getParentFile().toPath(), propertiesFile.getName(),
        "_pre_overwrite").toFile();
    // For backward compatibility, the legacy format of nsProperties is used
    FileUtil.writeToFile(tempFile, nsProperties.toLegacySKDef() + "," + nsProperties.getCreationTime());
    Files.move(tempFile.toPath(), propertiesFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING);
  }

  public static boolean propertiesFileExists(File nsDir) throws IOException {
    return propertiesFile(nsDir).exists();
  }
}
