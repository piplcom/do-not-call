package pipl.pse.donotcallgen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pipl.pse.donotcallgen.services.ProbabilityMap;

import java.io.*;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

public class DoNotCallGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(DoNotCallGenerator.class);
  private static final String DO_NOT_CALL = "do_not_call";
  private static final String TEXT_FILE_NAME = DO_NOT_CALL + "_txt";
  private static final String EXTENSION_PREFIX = "._.";
  private static final NumberFormat FORMATTER = java.text.NumberFormat.getIntegerInstance();

  public static final String WORK_FOLDER_ARG = "workFolder";
  public static final String INPUT_FILE_NAME_ARG = "inputFileName";
  public static final String DO_NOT_CALL_DATE_ARG = "doNotCallDate";
  public static final int INTERVAL = 1000000;
  public static final int MILLIS_PER_SECOND = 1000;

  private static String WORK_FOLDER = ".";
  private static String RAW_DO_NOT_CALL_DOWNLOAD_FILE = "NO INPUT FILE"; // for example: "2020-2-6_Global_4B95655D-7BFA-4CCA-8949-E911404284EE.txt";
  private static String DO_NOT_CALL_DATE = "NO_DATE"; // for example: "2018_02_20";
  private static final String USAGE_EXAMPLE =
      "Usage example: java -Xms25G -Xmx25G -jar DoNotCallGenerator.jar workFolder=. inputFileName=2020-2-6_Global_4B95655D-7BFA-4CCA-8949-E911404284EE.txt doNotCallDate=2018_02_20";

  /**
   * Generates a binary map file from raw do-not-call file downloaded from the Do Not Call registry
   * <BR/><b>Usage example:</b><BR/>
   *   java -Xms25G -Xmx25G -jar DoNotCallGenerator.jar workFolder=. inputFileName=2020-2-6_Global_4B95655D-7BFA-4CCA-8949-E911404284EE.txt doNotCallDate=2018_02_20
   * @param args workFolder, inputFileName, doNotCallDate
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Set<String> expectedArgs = new HashSet(Arrays.asList(WORK_FOLDER_ARG, INPUT_FILE_NAME_ARG, DO_NOT_CALL_DATE_ARG));
    if (args.length < expectedArgs.size()) {
      throw new IllegalArgumentException("Number of arguments is " + args.length + " (expecting " + expectedArgs.size() + ")\n" + USAGE_EXAMPLE);
    }
    for (String arg : args) {
      if (arg.startsWith(WORK_FOLDER_ARG)) {
        WORK_FOLDER = getArgValue(arg, WORK_FOLDER_ARG, expectedArgs);
      } else if (arg.startsWith(INPUT_FILE_NAME_ARG)) {
        RAW_DO_NOT_CALL_DOWNLOAD_FILE = getArgValue(arg, INPUT_FILE_NAME_ARG, expectedArgs);
      } else if (arg.startsWith(DO_NOT_CALL_DATE_ARG)) {
        DO_NOT_CALL_DATE = getArgValue(arg, DO_NOT_CALL_DATE_ARG, expectedArgs);
      }
    }
    if (!expectedArgs.isEmpty()) {
      throw new IllegalArgumentException("Argument(s) " + expectedArgs.toString() + " not supplied\n" + USAGE_EXAMPLE);
    }

    long startTime = System.currentTimeMillis();
    
    String rawInputTextFilePath = WORK_FOLDER + File.separator + RAW_DO_NOT_CALL_DOWNLOAD_FILE;
    String textFilePath = WORK_FOLDER + File.separator + TEXT_FILE_NAME;
    String binaryFilePath = WORK_FOLDER + File.separator + DO_NOT_CALL + EXTENSION_PREFIX + DO_NOT_CALL_DATE;

    LOGGER.info("Processing lines...");
    List<String> fileLines = readFileLines(rawInputTextFilePath).stream()
        .sorted()
        .map(line -> line.replace(",", "") + ", 1")
        .collect(Collectors.toList());

    appendLinesToFile(fileLines, textFilePath);

    runCountMap(textFilePath, binaryFilePath);

    long intervalMillis = System.currentTimeMillis() - startTime;
    long fullSeconds = intervalMillis / MILLIS_PER_SECOND;
    long minutes = fullSeconds / 60;
    long seconds = fullSeconds - minutes * 60;
    LOGGER.info("Processed do-not-call data in {} minutes and {}.{} seconds.", minutes, seconds, intervalMillis % MILLIS_PER_SECOND);
  }

  private static String getArgValue(String arg, String argName, Set<String> expectedArgs) {
    expectedArgs.remove(argName);
    return arg.replace(argName + "=", "");
  }

  private static void runCountMap(String inputTextFilePath, String outputBinaryFilePath) {
    try (ProbabilityMap probabilityMap = new ProbabilityMap(outputBinaryFilePath, 1, 0.0f)) {
      probabilityMap.loadDump(inputTextFilePath, false);
      probabilityMap.save();
      LOGGER.info("Done with {}", outputBinaryFilePath);
    } catch (Throwable t) {
      LOGGER.error("Failed to create binary count map", t);
    }
  }

  private static List<String> readFileLines(String filePath) throws IOException {
    List<String> lines = new ArrayList<>();
    LOGGER.info("Reading input file " + filePath);
    try (FileReader fileReader = new FileReader(filePath);
         BufferedReader bufferedReader = new BufferedReader(fileReader)) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        lines.add(line);
        if (lines.size() % INTERVAL == 0) {
          LOGGER.info(FORMATTER.format(lines.size()) + " lines read so far");
        }
      }
    }
    LOGGER.info("Done reading input file " + filePath);

    return lines;
  }

  private static void appendLinesToFile(List<String> lines, String filePath) throws IOException {
    LOGGER.info("Outputting lines to file " + filePath);
    File file = new File(filePath);
    file.createNewFile();
    try (FileWriter fw = new FileWriter(filePath, false);
         BufferedWriter bw = new BufferedWriter(fw);
         PrintWriter pw = new PrintWriter(bw)) {
      int count = 0;
      for (String line : lines) {
        pw.println(line);
        count++;
        if (count % INTERVAL == 0) {
          LOGGER.info(FORMATTER.format(lines.size()) + " lines written so far");
        }
      }
      LOGGER.info("Done writing lines, Now flushing buffers");
      pw.flush();
      bw.flush();
      fw.flush();
    }
    LOGGER.info("Done outputting lines to file " + filePath);
  }
}
