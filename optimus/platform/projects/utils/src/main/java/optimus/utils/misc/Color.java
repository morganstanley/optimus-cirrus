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
package optimus.utils.misc;

// Color definitions consistent with awt

public class Color {
  public static final Color white = new Color(255, 255, 255);
  public static final Color WHITE = white;
  public static final Color lightGray = new Color(192, 192, 192);
  public static final Color LIGHT_GRAY = lightGray;
  public static final Color gray = new Color(128, 128, 128);
  public static final Color GRAY = gray;
  public static final Color darkGray = new Color(64, 64, 64);
  public static final Color DARK_GRAY = darkGray;
  public static final Color black = new Color(0, 0, 0);
  public static final Color BLACK = black;
  public static final Color red = new Color(255, 0, 0);
  public static final Color RED = red;
  public static final Color pink = new Color(255, 175, 175);
  public static final Color PINK = pink;
  public static final Color orange = new Color(255, 200, 0);
  public static final Color ORANGE = orange;
  public static final Color yellow = new Color(255, 255, 0);
  public static final Color YELLOW = yellow;
  public static final Color green = new Color(0, 255, 0);
  public static final Color GREEN = green;
  public static final Color magenta = new Color(255, 0, 255);
  public static final Color MAGENTA = magenta;
  public static final Color cyan = new Color(0, 255, 255);
  public static final Color CYAN = cyan;
  public static final Color blue = new Color(0, 0, 255);
  public static final Color BLUE = blue;
  private int value;

  private static void testColorValueRange(int r, int g, int b, int a) {
    boolean rangeError = false;
    String badComponentString = "";

    if (a < 0 || a > 255) {
      rangeError = true;
      badComponentString = badComponentString + " Alpha";
    }
    if (r < 0 || r > 255) {
      rangeError = true;
      badComponentString = badComponentString + " Red";
    }
    if (g < 0 || g > 255) {
      rangeError = true;
      badComponentString = badComponentString + " Green";
    }
    if (b < 0 || b > 255) {
      rangeError = true;
      badComponentString = badComponentString + " Blue";
    }
    if (rangeError) {
      throw new IllegalArgumentException(
          "Color parameter outside of expected range:" + badComponentString);
    }
  }

  public Color(int r, int g, int b) {
    this(r, g, b, 255);
  }

  public Color(int r, int g, int b, int a) {
    value = getRGB(r, g, b, a);
    testColorValueRange(r, g, b, a);
  }

  public int getRGB() {
    return value;
  }

  private static int getRGB(int r, int g, int b, int a) {
    testColorValueRange(r, g, b, a);
    return ((a & 0xFF) << 24) | ((r & 0xFF) << 16) | ((g & 0xFF) << 8) | ((b & 0xFF));
  }
}
