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
package com.ms.silverking.pssh;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.ms.silverking.text.StringUtil;
import com.ms.silverking.util.ArrayUtil;

public class HostAndCommand implements Serializable, Comparable<HostAndCommand> {
  private final String host;
  private final String[] command;

  private static final long serialVersionUID = 6436678339067519421L;

  public HostAndCommand(String host, String[] command) {
    assert command != null;
    this.host = host;
    this.command = command;
  }

  public String getHost() {
    return host;
  }

  public String[] getCommand() {
    return command;
  }

  public static Set<String> getHosts(Collection<HostAndCommand> hostCommands) {
    ImmutableSet.Builder<String> hosts;

    hosts = ImmutableSet.builder();
    for (HostAndCommand hostAndCommand : hostCommands) {
      hosts.add(hostAndCommand.getHost());
    }
    return hosts.build();
  }

  @Override
  public int hashCode() {
    int hash;

    hash = host.hashCode();
    if (command != null) {
      for (String c : command) {
        hash = hash ^ c.hashCode();
      }
    }
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    HostAndCommand o;

    o = (HostAndCommand) obj;
    if (!this.host.equals(o.host)) {
      return false;
    } else {
      // presume that command != null
      if (this.command.length != o.command.length) {
        return false;
      } else {
        for (int i = 0; i < this.command.length; i++) {
          if (!this.command[i].equals(o.command[i])) {
            return false;
          }
        }
        return true;
      }
    }
  }

  @Override
  public int compareTo(HostAndCommand o) {
    int result;

    result = this.host.compareTo(o.host);
    if (result == 0) {
      String thisCommand;
      String oCommand;

      thisCommand = ArrayUtil.toString(this.command, ' ');
      oCommand = ArrayUtil.toString(o.command, ' ');
      result = thisCommand.compareTo(oCommand);
    }
    return result;
  }

  @Override
  public String toString() {
    return host + ":" + StringUtil.arrayToString(command);
  }
}
