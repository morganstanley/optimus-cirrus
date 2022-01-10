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
  public final static Color white = new Color(255, 255, 255);
  public final static Color WHITE = white;
  public final static Color lightGray = new Color(192, 192, 192);
  public final static Color LIGHT_GRAY = lightGray;
  public final static Color gray = new Color(128, 128, 128);
  public final static Color GRAY = gray;
  public final static Color darkGray = new Color(64, 64, 64);
  public final static Color DARK_GRAY = darkGray;
  public final static Color black = new Color(0, 0, 0);
  public final static Color BLACK = black;
  public final static Color red = new Color(255, 0, 0);
  public final static Color RED = red;
  public final static Color pink = new Color(255, 175, 175);
  public final static Color PINK = pink;
  public final static Color orange = new Color(255, 200, 0);
  public final static Color ORANGE = orange;
  public final static Color yellow = new Color(255, 255, 0);
  public final static Color YELLOW = yellow;
  public final static Color green = new Color(0, 255, 0);
  public final static Color GREEN = green;
  public final static Color magenta = new Color(255, 0, 255);
  public final static Color MAGENTA = magenta;
  public final static Color cyan = new Color(0, 255, 255);
  public final static Color CYAN = cyan;
  public final static Color blue = new Color(0, 0, 255);
  public final static Color BLUE = blue;
  int value;

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
      throw new IllegalArgumentException("Color parameter outside of expected range:" + badComponentString);
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

  static int getRGB(int r, int g, int b) {
    return getRGB(r, g, b, 255);
  }

  static int getRGB(int r, int g, int b, int a) {
    testColorValueRange(r, g, b, a);
    return ((a & 0xFF) << 24) | ((r & 0xFF) << 16) | ((g & 0xFF) << 8) | ((b & 0xFF) << 0);
  }

}
