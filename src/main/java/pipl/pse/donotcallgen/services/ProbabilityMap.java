package pipl.pse.donotcallgen.services;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Scanner;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

public class ProbabilityMap implements AutoCloseable {
  static final Logger LOGGER = LoggerFactory.getLogger(ProbabilityMap.class);
  static final private StringBuilder SLASH_N = new StringBuilder().append('\\').append('N');
  private UnsafeMap unsafeMap = null;
  private String outputBinaryFilePath;
  private long size = 0;
  private HashFunction hasher;
  public float defaultValue;
  private int numProbs;

  public interface StringMapper extends Function<String, Float> {
  }

  public ProbabilityMap(String outputBinaryFilePath, int numProbs, float defaultValue)
      throws SecurityException, IllegalArgumentException {
    this.outputBinaryFilePath = outputBinaryFilePath;
    this.numProbs = numProbs;
    hasher = Hashing.sipHash24();
    this.defaultValue = defaultValue;
  }

  public void close() {
    if (unsafeMap != null) {
      unsafeMap.close();
      unsafeMap = null;
    }
  }

  public void loadDump(boolean isCollisionTest, String dumpFile, StringMapper... mapping)
      throws NoSuchFieldException, SecurityException,
      IllegalArgumentException, IllegalAccessException, IOException {
    long startTime = System.currentTimeMillis();
    // Count the number of entries
    File file = new File(dumpFile);
    long count = countLines(file);
    unsafeMap = new UnsafeMap(count, numProbs);
    StringBuilder sb = new StringBuilder("(.*)\\t([^\\t]+)");
    for (int i = 1; i < numProbs; i++) {
      sb.append("\\t([^\\t]+)");
    }
    Pattern p = Pattern.compile(sb.toString());
    long lineNumber = 0;
    String key = null;
    Exception ioException = null;
    int skippedLinesCnt = 0;
    HashMap<Long, String> prevKeys = new HashMap<>();
    try (Scanner scanner = new Scanner(new BufferedReader(new InputStreamReader(new FileInputStream(dumpFile), StandardCharsets.UTF_8)))) {
      while (scanner.hasNextLine()) {
        ioException = scanner.ioException();
        lineNumber++;
        scanner.findInLine(p);
        MatchResult matches = null;
        try {
          matches = scanner.match();
        } catch (IllegalStateException ise) {
          LOGGER.error("Failed to match line '" + scanner.nextLine() + "' with regex '" + sb.toString() + "' with numProbs=" + numProbs);
          throw ise;
        }
        key = matches.group(1).replace(SLASH_N, "");
        if (key.indexOf('\uFFFD') > -1) {
          LOGGER.warn("Skipped line {} in {} because of invalid key {}.", lineNumber, dumpFile, key);
          skippedLinesCnt++;
          if (scanner.hasNextLine()) {
            scanner.nextLine();
          }
          continue;
        }
        long keyHash = hasher.hashUnencodedChars(key.toLowerCase(Locale.getDefault())).asLong();
        float[] values = new float[numProbs];
        for (int i = 0; i < numProbs; i++) {
          final String matchedGroup = matches.group(i + 2);
          try {
            values[i] = mapping[i].apply(matchedGroup);
          } catch (NumberFormatException nfe) {
            LOGGER.error("Failed to match float value in line '" + scanner.nextLine() + "' matchGroup '" + matchedGroup + "'");
            throw nfe;
          }
        }
        boolean isRealCollision = unsafeMap.put(keyHash, values);
        if (isRealCollision) {
          final String prevKey = isCollisionTest ? "' of previous key '" + prevKeys.get(keyHash) : "";
          LOGGER.warn("Key '" + key + "' collided with a hash '" + keyHash + prevKey + "'");
        } else {
          // just for collision testing, when applicable
          if (isCollisionTest && key.contains("לוי") && key.contains("דוד")) {
            prevKeys.put(keyHash, key);
          }
        }
        if (scanner.hasNextLine()) {
          scanner.nextLine();
        }
        size++;
      }
    } catch (Throwable t) {
      if (ioException != null) {
        LOGGER.error("There was an error reading the underlying dump file {}.", dumpFile, ioException);
      }
      LOGGER.error("Failed parsing dump {}. Around line {}: {}", dumpFile, lineNumber, key, t);
      close();
      throw t;
    }
    LOGGER.info("Loaded dump {} in {} seconds. Found {} key collisions and {} hashmap collisions in {} keys. Skipped {} lines with bad chars",
        dumpFile, (System.currentTimeMillis() - startTime) / 1000, unsafeMap.getRealCollisions(), unsafeMap.getHashCollisions(), size, skippedLinesCnt);
  }

  private long countLines(File file) throws IOException {
    long count = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      int c;
      int previous = 0;
      while ((c = reader.read()) != -1) {
        if (c == 10) {
          count++;
        }
        previous = c;
      }
      if (previous != 10) {
        // Handle files that have last line not terminated by \n
        count++;
      }
    }
    return count;
  }

  public void loadDump(String inputTextFilePath, boolean isCollisionTest)
      throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, IOException {
    LOGGER.info("Loading {}", inputTextFilePath);
    StringMapper[] mapping = new StringMapper[numProbs];
    StringMapper parseFloat = Float::parseFloat;
    Arrays.fill(mapping, parseFloat);
    loadDump(isCollisionTest, inputTextFilePath, mapping);
  }

  public void save() throws IOException {
    LOGGER.info("Saving {}", outputBinaryFilePath);
    Path path = FileSystems.getDefault().getPath(outputBinaryFilePath);
    long startTime = System.currentTimeMillis();
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(path.toString()), 64000)) {
      long size = unsafeMap.getSizeInBytes();
      byte[] buffer = new byte[4000];
      int j = 0;
      for (long i = 0; i < size; i++) {
        buffer[j++] = unsafeMap.getByte(i);
        if (j == 4000) {
          bos.write(buffer, 0, j);
          j = 0;
        }
      }
      bos.write(buffer, 0, j);
    }
    LOGGER.info("Saved binary {} in {} milliseconds.", outputBinaryFilePath, System.currentTimeMillis() - startTime);
  }
}
