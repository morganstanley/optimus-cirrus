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
package com.ms.silverking.numeric;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Histogram {
  private final double min;
  private final double max;
  private final double binSize;
  private final int numBins;
  private final Bin[] bins;
  private final double logBase;
  private boolean frozen;
  private final LogBaseN log;

  private static Logger logger = LoggerFactory.getLogger(Histogram.class);

  private static final double NOT_LOG_SCALE = 0.0;

  public Histogram(double min, double max, int numBins) {
    this(min, max, numBins, NOT_LOG_SCALE);
  }

  public Histogram(double min, double max, int numBins, double logBase) {
    this.min = min;
    this.max = max;
    this.numBins = numBins;
    bins = new Bin[numBins];
    this.logBase = logBase;
    if (logBase == NOT_LOG_SCALE) {
      binSize = (max - min) / (double) numBins;
      log = null;
      initBins();
    } else {
      binSize = -1;
      log = initLogBins();
    }
  }

  private void initBins() {
    double binMin;

    binMin = 0.0;
    for (int i = 0; i < numBins; i++) {
      double binMax;

      if (i < numBins - 1) {
        binMax = binMin + binSize;
      } else {
        binMax = max;
      }
      bins[i] = new Bin(binMin, binMax);
      binMin = binMax;
    }
  }

  private LogBaseN initLogBins() {
    double b;

    b = Math.exp(Math.log(max - min) / (double) numBins);
    for (int i = 0; i < numBins; i++) {
      double binMin;
      double binMax;

      binMin = Math.pow(b, i);
      if (i < numBins - 1) {
        binMax = Math.pow(b, i + 1);
      } else {
        binMax = max;
      }
      bins[i] = new Bin(binMin, binMax);
      binMin = binMax;
    }
    return new LogBaseN(b);
  }

  public boolean logScale() {
    return logBase != NOT_LOG_SCALE;
  }

  public void add(double value, int count) {
    bins[binIndex(value)].add(count);
  }

  public void increment(double value) {
    bins[binIndex(value)].increment();
  }

  private int binIndex(double value) {
    if (value < min || value > max) {
      throw new RuntimeException("value out of range: " + value);
    }
    if (value == max) {
      return numBins - 1;
    } else {
      if (logBase == NOT_LOG_SCALE) {
        return (int) ((value - min) / binSize);
      } else {
        if (value == min) {
          return 0;
        } else {
          //System.out.println(log +" "+ value +" "+ min + log.log(value - min));
          return (int) log.log(value - min);
        }
      }
    }
  }

  public static Histogram parse(String fileName, int numBins) throws IOException {
    return parse(new FileInputStream(fileName), numBins);
  }

  public static Histogram parse(String fileName, int numBins, double logBase) throws IOException {
    return parse(new FileInputStream(fileName), numBins, logBase);
  }

  public static Histogram parse(InputStream inStream, int numBins) throws IOException {
    return parse(inStream, numBins, NOT_LOG_SCALE);
  }

  public static Histogram parse(InputStream inStream, int numBins, double logBase) throws IOException {
    BufferedReader reader;
    String line;
    Histogram histogram;
    List<Double> values;
    List<Integer> counts;
    double min;
    double max;

    values = new ArrayList<Double>();
    counts = new ArrayList<Integer>();
    reader = new BufferedReader(new InputStreamReader(inStream));
    do {
      line = reader.readLine();
      if (line != null && line.length() != 0) {
        String[] tokens;

        line = line.trim();
        tokens = line.split("\\s+");
        if (tokens.length == 2) {
          try {
            values.add(Double.parseDouble(tokens[0]));
            counts.add(Integer.parseInt(tokens[1]));
          } catch (NumberFormatException nfe) {
            logger.info("Ignoring: {}" , line);
          }
        } else {
          logger.info("Ignoring: {}" , line);
        }
      }
    } while (line != null);
    inStream.close();
    min = Double.MAX_VALUE;
    max = Double.MIN_NORMAL;
    for (double value : values) {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }
    histogram = new Histogram(min, max, numBins, logBase);
    for (int i = 0; i < values.size(); i++) {
      histogram.add(values.get(i), counts.get(i));
    }
    histogram.freeze();
    return histogram;
  }

  private void freeze() {
    frozen = true;
  }

  public String toStringCumulative() {
    StringBuilder sb;
    long total;

    total = 0;
    sb = new StringBuilder();
    for (Bin bin : bins) {
      total += bin.getCount();
      sb.append(minMaxCountString(bin.getMin(), bin.getMax(), total));
      sb.append('\n');
    }
    return sb.toString();
  }

  public String toString() {
    StringBuilder sb;

    sb = new StringBuilder();
    for (Bin bin : bins) {
      sb.append(bin.toString());
      sb.append('\n');
    }
    return sb.toString();
  }

  public static String minMaxCountString(double min, double max, long count) {
    StringBuilder sb;

    sb = new StringBuilder();
    sb.append(min);
    sb.append('\t');
    sb.append(max);
    sb.append('\t');
    sb.append(count);
    return sb.toString();
  }

  private class Bin {
    private final double min;
    private final double max;
    private long count;

    public Bin(double min, double max) {
      this.min = min;
      this.max = max;
    }

    public double getMin() {
      return min;
    }

    public double getMax() {
      return max;
    }

    public long getCount() {
      return count;
    }

    public void add(int count) {
      if (frozen) {
        throw new RuntimeException("Can't mutate frozen bin.");
      }
      this.count += count;
    }

    public void increment() {
      add(1);
    }

    public String toString() {
      return minMaxCountString(min, max, count);
    }
  }

  public static void main(String[] args) {
    try {
      Histogram histogram;
      String fileName;
      int numBins;

      if (args.length < 2 || args.length > 3) {
        System.out.println("args: <fileName> <numBins> [logBase]");
        return;
      }
      fileName = args[0];
      numBins = Integer.parseInt(args[1]);
      if (args.length == 3) {
        double logBase;

        logBase = Double.parseDouble(args[2]);
        histogram = Histogram.parse(fileName, numBins, logBase);
      } else {
        histogram = Histogram.parse(fileName, numBins);
      }
      System.out.println("Histogram");
      System.out.println(histogram.toString());
      System.out.println("\nCumulative");
      System.out.println(histogram.toStringCumulative());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
