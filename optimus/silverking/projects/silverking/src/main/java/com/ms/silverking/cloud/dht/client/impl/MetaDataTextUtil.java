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

import com.ms.silverking.cloud.dht.client.Compression;
import com.ms.silverking.cloud.dht.client.MetaData;
import com.ms.silverking.cloud.dht.common.MetaDataUtil;
import com.ms.silverking.text.StringUtil;

public class MetaDataTextUtil {
  public static String toMetaDataString(MetaData metaData, boolean labeled) {
    StringBuilder sb;
    byte[] userData;

    assert metaData != null;
    sb = new StringBuilder();
    format(sb, "StoredLength", Integer.toString(metaData.getStoredLength()), labeled);
    format(sb, "UncompressedLength", Integer.toString(metaData.getUncompressedLength()), labeled);
    format(sb, "Compression", metaData.getCompression().toString(), labeled);
    format(sb, "Version", Long.toString(metaData.getVersion()), labeled);
    format(
        sb,
        "CreationTime",
        metaData.getCreationTime() + "\t[" + metaData.getCreationTime().toDateString() + "]",
        labeled);
    format(sb, "ValueCreator", metaData.getCreator().toString(), labeled);
    format(sb, "LockSeconds", Integer.toString(metaData.getLockSeconds()), labeled);
    format(sb, "ChecksumType", metaData.getChecksumType().toString(), labeled);
    format(sb, "Checksum", StringUtil.byteArrayToHexString(metaData.getChecksum()), labeled);
    /*
    if (labeled) {
        sb.append("UserData: ");
    }
    userData = metaData.getUserData();
    if (userData == null) {
        sb.append("null");
    } else {
        sb.append(StringUtil.byteArrayToHexString(userData));
    }
    //TODO (OPTIMUS-0000): complete user data
    */
    return sb.toString();
  }

  public static String toMetaDataString(byte[] sv, int baseOffset, boolean labeled) {
    StringBuilder sb;
    byte[] userData;

    sb = new StringBuilder();
    format(
        sb,
        "StoredLength",
        Integer.toString(MetaDataUtil.getStoredLength(sv, baseOffset)),
        labeled);
    format(
        sb,
        "UncompressedLength",
        Integer.toString(MetaDataUtil.getUncompressedLength(sv, baseOffset)),
        labeled);
    format(
        sb,
        "Compression",
        Compression.values()[MetaDataUtil.getCompression(sv, baseOffset)].toString(),
        labeled);
    format(sb, "Version", Long.toString(MetaDataUtil.getVersion(sv, baseOffset)), labeled);
    format(
        sb,
        "CreationTime",
        MetaDataUtil.getCreationTime(sv, baseOffset)
            + "\t["
            + MetaDataUtil.getCreationTime(sv, baseOffset)
            + "]",
        labeled);
    format(sb, "ValueCreator", MetaDataUtil.getCreator(sv, baseOffset).toString(), labeled);
    format(
        sb, "LockSeconds", Integer.toString(MetaDataUtil.getLockSeconds(sv, baseOffset)), labeled);
    format(sb, "ChecksumType", MetaDataUtil.getChecksumType(sv, baseOffset).toString(), labeled);
    format(
        sb,
        "Checksum",
        StringUtil.byteArrayToHexString(MetaDataUtil.getChecksum(sv, baseOffset)),
        labeled);
    /*
    if (labeled) {
        sb.append("UserData: ");
    }
    userData = metaData.getUserData();
    if (userData == null) {
        sb.append("null");
    } else {
        sb.append(StringUtil.byteArrayToHexString(userData));
    }
    //TODO (OPTIMUS-0000): complete user data
    */
    return sb.toString();
  }

  private static void format(StringBuilder sb, String string, String value, boolean labeled) {
    if (labeled) {
      sb.append(String.format("\n%-20s %s", string + ":", value));
    } else {
      sb.append(String.format("\n%s", value));
    }
  }
}
