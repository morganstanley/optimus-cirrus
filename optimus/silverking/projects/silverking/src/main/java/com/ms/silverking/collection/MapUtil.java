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
package com.ms.silverking.collection;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapUtil {
  public enum NoDelimiterAction {Ignore, Warn, Exception}

  ;

  private static Logger log = LoggerFactory.getLogger(MapUtil.class);

  public static Map<String, String> parseStringMap(InputStream in, char delimiter, NoDelimiterAction noDelimiterAction)
      throws IOException {
    return parseMap(in, delimiter, noDelimiterAction, new StringParser(), new StringParser());
  }

  public static <K, V> Map<K, V> parseMap(InputStream in, char delimiter, NoDelimiterAction noDelimiterAction,
      Function<String, K> keyParser, Function<String, V> valueParser) throws IOException {
    BufferedReader reader;
    String line;
    int lineNumber;
    Map<K, V> map;

    map = new HashMap();
    lineNumber = 0;
    reader = new BufferedReader(new InputStreamReader(in));
    do {
      ++lineNumber;
      line = reader.readLine();
      if (line != null) {
        int dIndex;

        dIndex = line.indexOf(delimiter);
        if (dIndex >= 0) {
          String kDef;
          String vDef;

          kDef = line.substring(0, dIndex);
          vDef = line.substring(dIndex + 1);
          map.put(keyParser.apply(kDef), valueParser.apply(vDef));
        } else {
          switch (noDelimiterAction) {
          case Ignore:
            break;
          case Warn:
            log.info("No delimiter found on line {}", lineNumber);
            break;
          case Exception:
            throw new RuntimeException("No delimiter found on line: " + lineNumber);
          default:
            throw new RuntimeException("Panic");
          }
        }
      }
    } while (line != null);
    return map;
  }

  public static <K, V> Pair<K, V> mapEntryToPair(Map.Entry<K, V> entry) {
    return new Pair<>(entry.getKey(), entry.getValue());
  }

  private static class StringParser implements Function<String, String> {
    @Override
    public String apply(String s) {
      return s;
    }
  }

  public static void main(String[] args) {
    try {
      Map<String, String> m;

      m = MapUtil.parseStringMap(new FileInputStream(args[0]), '\t', MapUtil.NoDelimiterAction.Warn);
      m.put("1", "1");
      log.info("{}",m);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
