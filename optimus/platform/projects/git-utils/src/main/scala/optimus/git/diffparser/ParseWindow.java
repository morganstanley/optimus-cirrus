/*
 * Copyright 2013-2015 Tom Hombergs (tom.hombergs@gmail.com | http://wickedsource.org)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* Modifications to this file were made by Morgan Stanley to write into Groovy and then back into Java.
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
package optimus.git.diffparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedList;

/**
 * A {@link ParseWindow} slides through the lines of a input stream and offers methods to get the
 * currently focused line as well as upcoming lines. It is backed by an automatically resizing
 * {@link LinkedList}
 */
public class ParseWindow {
  public ParseWindow(InputStream input) {
    Reader unbufferedReader = new InputStreamReader(input);
    this.reader = new BufferedReader(unbufferedReader);
  }

  /**
   * Looks ahead from the current line and retrieves a line that will be the focus line after the
   * window has slided forward.
   *
   * @param distance the number of lines to look ahead. Must be greater or equal 0. 0 returns the
   *     focus line. 1 returns the first line after the current focus line and so on. Note that all
   *     lines up to the returned line will be held in memory until the window has slided past them,
   *     so be careful not to look ahead too far!
   * @return the line identified by the distance parameter that lies ahead of the focus line.
   *     Returns null if the line cannot be read because it lies behind the end of the stream.
   */
  public String getFutureLine(int distance) {
    try {
      resizeWindowIfNecessary(distance + 1);
      return lineQueue.get(distance);
    } catch (IndexOutOfBoundsException ignored) {
      return null;
    }
  }

  public void addLine(int pos, String line) {
    lineQueue.add(pos, line);
  }

  /**
   * Resizes the sliding window to the given size, if necessary.
   *
   * @param newSize the new size of the window (i.e. the number of lines in the window).
   */
  private void resizeWindowIfNecessary(int newSize) {
    try {
      int numberOfLinesToLoad = newSize - this.lineQueue.size();
      for (int i = 0; i < numberOfLinesToLoad; i++) {
        String nextLine = getNextLine();
        if (nextLine != null) {
          lineQueue.addLast(nextLine);
        } else {
          throw new IndexOutOfBoundsException("End of stream has been reached!");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Slides the window forward one line.
   *
   * @return the next line that is in the focus of this window or null if the end of the stream has
   *     been reached.
   */
  public String slideForward() {
    try {
      lineQueue.pollFirst();
      lineNumber = lineNumber++;
      if (lineQueue.isEmpty()) {
        String nextLine = getNextLine();
        if (nextLine != null) {
          lineQueue.addLast(nextLine);
        }

        return nextLine;
      } else {
        return lineQueue.peekFirst();
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getNextLine() throws IOException {
    String nextLine = reader.readLine();

    return getNextLineOrVirtualBlankLineAtEndOfStream(nextLine);
  }

  /**
   * Guarantees that a virtual blank line is injected at the end of the input stream to ensure the
   * parser attempts to transition to the {@code END} state, if necessary, when the end of stream is
   * reached.
   */
  private String getNextLineOrVirtualBlankLineAtEndOfStream(String nextLine) {
    if ((nextLine == null) && !isEndOfStream) {
      isEndOfStream = true;
      return "";
    }

    return nextLine;
  }

  /**
   * Returns the line currently focused by this window. This is actually the same line as returned
   * by {@link #slideForward()} but calling this method does not slide the window forward a step.
   *
   * @return the currently focused line.
   */
  public String getFocusLine() {
    return lineQueue.getFirst();
  }

  private BufferedReader reader;
  private LinkedList<String> lineQueue = new LinkedList<String>();
  private int lineNumber = 0;
  private boolean isEndOfStream = false;
}
