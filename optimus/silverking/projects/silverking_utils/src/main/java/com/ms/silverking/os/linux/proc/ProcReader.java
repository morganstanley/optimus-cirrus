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
package com.ms.silverking.os.linux.proc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.UserPrincipal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ms.silverking.io.StreamParser;

/** Provides Linux /proc information. */
public class ProcReader {
  private final StructReader<ProcessStat> reader;

  private static final List<String> emptyList = new ArrayList<String>(0);
  private static final String proc = "/proc";
  private static final File procDir = new File(proc);
  private static final String memFileName = proc + "/meminfo";

  public enum FilterType {
    INCLUSIVE,
    EXCLUSIVE
  }

  private static final String swapFreeToken = "SwapFree:";

  /**
   * Calculation of TotalFreeMem : SwapUsed = SwapTotal - SwapFree total free mem = MemFree + Cached
   * - SwapUsed which is equivalent to: total free mem = MemFree + Cached + SwapFree - SwapTotal
   * Boolean in the map represents if the value needs to be added or subtracted
   */
  private static final Map<String, Boolean> memoryTokens =
      new HashMap<String, Boolean>() {
        {
          put("MemFree:", true); // add to free memory total
          put("Cached:", true); // add to free memory total
          put("SwapFree:", true); // add to free memory total
          put("SwapTotal:", false); // subtract from free memory total
        }
      };

  private static final String rawRAMToken = "MemFree:";

  public ProcReader() {
    reader = new StructReader<ProcessStat>(ProcessStat.class);
  }

  public List<ProcessStat> activeProcessStats() {
    List<ProcessStat> stats;

    stats = new ArrayList<>();
    for (int pid : activePIDList()) {
      ProcessStat stat;

      stat = readStat(pid);
      if (stat != null) {
        stats.add(stat);
      }
    }
    return stats;
  }

  public List<Integer> activePIDList() {
    List<Integer> pidList;
    String[] files;

    pidList = new ArrayList<>();
    files = procDir.list();
    for (String file : files) {
      if (file.matches("\\d+")) {
        pidList.add(Integer.valueOf(file));
      }
    }
    return pidList;
  }

  public List<Integer> filteredActivePIDList(List<String> exceptions) {
    return filteredActivePIDList(activePIDList(), FilterType.EXCLUSIVE, exceptions);
  }

  public List<Integer> filteredActivePIDList(
      List<Integer> pidList, FilterType filterType, List<String> patterns) {
    List<Integer> filteredPIDs;

    filteredPIDs = new ArrayList<>();
    for (int pid : pidList) {
      if (filterType == FilterType.INCLUSIVE) {
        if (cmdLineMatchesPattern(pid, patterns)) {
          filteredPIDs.add(pid);
        }
      } else {
        if (!cmdLineMatchesPattern(pid, patterns)) {
          filteredPIDs.add(pid);
        }
      }
    }
    return filteredPIDs;
  }

  public boolean cmdLineMatchesPattern(int pid, List<String> patterns) {
    String cmdLine;

    cmdLine = readCommandLine(pid);
    if (cmdLine != null) {
      for (String pattern : patterns) {
        if (cmdLine.matches(pattern)) {
          return true;
        }
      }
      return false;
    } else {
      return false;
    }
  }

  public String readCommandLine(int pid) {
    try {
      return StreamParser.parseLine(new File(proc + "/" + pid + "/cmdline"));
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Raw free RAM
   *
   * @return
   * @throws IOException
   */
  public long freeRamBytes() throws IOException {
    List<String> lines;
    String[] tokens;

    lines = StreamParser.parseFileLines(memFileName);
    for (String line : lines) {
      tokens = line.split("\\s+");
      if (tokens[0].startsWith(rawRAMToken)) {
        return Long.parseLong(tokens[1]) * 1024;
      }
    }
    return 0;
  }

  /**
   * Adjusted free memory
   *
   * @return
   * @throws IOException
   */
  public long freeMemBytes() throws IOException {
    List<String> lines;
    String[] tokens;
    long totalFreeKB;

    totalFreeKB = 0;
    lines = StreamParser.parseFileLines(memFileName);
    for (String line : lines) {
      long freeKB;

      freeKB = 0;
      tokens = line.split("\\s+");
      for (String memTok : memoryTokens.keySet()) {
        if (tokens[0].startsWith(memTok)) {
          // if a Token key in the map is associated with "false" value,
          // then we negate the parsed value:
          freeKB =
              memoryTokens.get(memTok) ? Long.parseLong(tokens[1]) : -Long.parseLong(tokens[1]);
          break;
        }
      }
      // sum outside of inner loop to avoid future maintenance
      // coding errors
      totalFreeKB += freeKB;
    }
    return totalFreeKB * 1024;
  }

  private long parseMem(String token) throws IOException {
    List<String> lines;
    String[] tokens;

    lines = StreamParser.parseFileLines(memFileName);
    for (String line : lines) {
      tokens = line.split("\\s+");
      if (tokens[0].startsWith(token)) {
        long freeKB;

        freeKB = Long.parseLong(tokens[1]);
        return freeKB * 1024;
      }
    }
    throw new IOException("Couldn't find token: " + token);
  }

  public long freeSwapBytes() throws IOException {
    return parseMem(swapFreeToken);
  }

  public ProcessStat readStat(int pid) {
    try {
      return reader.read(new File(proc + "/" + pid + "/stat"));
    } catch (IOException ioe) {
      return null;
    }
  }

  public ProcessStatAndOwner readStatAndOwner(int pid) {
    try {
      ProcessStat processStat;
      File statFile;
      UserPrincipal owner;

      statFile = new File(proc + "/" + pid + "/stat");
      processStat = reader.read(statFile);
      owner = Files.getOwner(Paths.get(statFile.getAbsolutePath()));
      return new ProcessStatAndOwner(processStat, owner.getName());
    } catch (IOException ioe) {
      return null;
    }
  }

  public List<String> openFD(int pid) {
    String[] fileNames;

    fileNames = new File(proc + "/" + pid + "/fd").list();
    if (fileNames != null) {
      return Arrays.asList(fileNames);
    } else {
      return emptyList;
    }
  }
}
