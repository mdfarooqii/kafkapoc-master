package com.ubs.risk.arisk;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class AriskFolderPollerTask extends SourceTask {
  static final Logger log = LoggerFactory.getLogger(AriskFolderPollerTask.class);

  static final String  PROCESSINGFILEEXTENSION = ".process";
  //protected Parser parser;
  private AriskFolderPollerConnectorConfig config;
  private CSVParser csvParser;
  private CSVReader csvReader;
  private File inputFile;
  private InputStream inputStream;
  private InputStreamReader streamReader;
  Stopwatch processingTime = Stopwatch.createStarted();
  private boolean hasRecords = false;
  private Map<String, String> fileMetadata;
  private long inputFileModifiedTime;
  private Map<String, String> metadata;
  protected Map<String, ?> sourcePartition;
  String[] fieldNames;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    //TODO: Do things here that are required to start your task. This could be open a connection to a database, etc.
    config = new AriskFolderPollerConnectorConfig(map);

    checkDirectory(config.getLookupFolderConfig());
    checkDirectory(config.getSuccessFolderConfig());
    checkDirectory(config.getFailureFolderConfig());


    System.out.println("Key Schema \n"+ config.key_Schema);
    System.out.println(" Value schema \n"+ config.value_Schema);

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    //TODO: Create SourceRecord objects that will be sent the kafka cluster.
   // throw new UnsupportedOperationException("This has not been implemented.");
    log.trace("poll()");
    List<SourceRecord> results = read();

    if (results.isEmpty()) {
      log.trace("read() returned empty list. Sleeping {} ms.", 10000);
      Thread.sleep(10000);
    }
    log.trace("read() returning {} result(s)", results.size());
    return results;
  }



  private void closeAndMoveToFinished(File outputDirectory, boolean errored) throws IOException {
    if (null != inputStream) {
      log.info("Closing {}", this.inputFile);

      this.inputStream.close();
      this.inputStream = null;

      File finishedFile = new File(outputDirectory, this.inputFile.getName());

      if (errored) {
        log.error("Error during processing, moving {} to {}.", this.inputFile, outputDirectory);
      } else {
        log.info("Finished processing {} in {} second(s). Moving to {}.", this.inputFile, processingTime.elapsed(TimeUnit.SECONDS), outputDirectory);
      }

      Files.move(this.inputFile, finishedFile);

      File processingFile = processingFile(this.inputFile);
      if (processingFile.exists()) {
        log.info("Removing processing file {}", processingFile);
        processingFile.delete();
      }

    }
  }

  public List<SourceRecord> read() {
    try {
      if (!hasRecords) {
        closeAndMoveToFinished(this.config.getSuccessFolderConfig(), false);

        File nextFile = findNextInputFile();
        if (null == nextFile) {
          return new ArrayList<>();
        }

        this.metadata = ImmutableMap.of();
        this.inputFile = nextFile;
        this.inputFileModifiedTime = this.inputFile.lastModified();
        File processingFile = processingFile(this.inputFile);
        Files.touch(processingFile);

        try {
          this.sourcePartition = ImmutableMap.of(
                  "fileName", this.inputFile.getName()
          );
          log.info("Opening {}", this.inputFile);
          Long lastOffset = null;
          log.trace("looking up offset for {}", this.sourcePartition);
          Map<String, Object> offset = this.context.offsetStorageReader().offset(this.sourcePartition);
          if (null != offset && !offset.isEmpty()) {
            Number number = (Number) offset.get("offset");
            lastOffset = number.longValue();
          }
          this.inputStream = new FileInputStream(this.inputFile);
          initializeConfigurations(this.inputStream, this.metadata, lastOffset);
        } catch (Exception ex) {
          throw new ConnectException(ex);
        }
        processingTime.reset();
        processingTime.start();
      }
      List<SourceRecord> records = process();
      this.hasRecords = !records.isEmpty();
      return records;
    } catch (Exception ex) {
      log.error("Exception encountered processing line {} of {}.", recordOffset(), this.inputFile, ex);

      try {
        closeAndMoveToFinished(this.config.getFailureFolderConfig(), true);
      } catch (IOException ex0) {
        log.error("Exception thrown while moving {} to {}", this.inputFile, this.config.getFailureFolderConfig(), ex0);
      }

      return new ArrayList<>();

    }
  }


  public List<SourceRecord> process() throws IOException {
    List<SourceRecord> records = new ArrayList<SourceRecord>(this.config.getFileBatchSizeConfig());

    while (records.size() < this.config.getFileBatchSizeConfig()) {
      String[] row = this.csvReader.readNext();

      if (row == null) {
        break;
      }
      log.trace("process() - Row on line {} has {} field(s)", recordOffset(), row.length);

      Struct keyStruct = new Struct(this.config.getKey_Schema());
      Struct valueStruct = new Struct(this.config.getValue_Schema());

      for (int i = 0; i < this.fieldNames.length; i++) {
        String fieldName = this.fieldNames[i];
        log.trace("process() - Processing field {}", fieldName);
        String input = row[i];
        log.trace("process() - input = '{}'", input);
        Object fieldValue = null;

        try {
          Field field = this.config.getValue_Schema().field(fieldName);
          if (null != field) {
           // fieldValue = this.parser.parseString(field.schema(), input);
            fieldValue =  input;
            log.trace("process() - output = '{}'", fieldValue);
            valueStruct.put(field, fieldValue);
          } else {
            log.trace("process() - Field {} is not defined in the schema.", fieldName);
          }
        } catch (Exception ex) {
          String message = String.format("Exception thrown while parsing data for '%s'. linenumber=%s", fieldName, this.recordOffset());
          throw new DataException(message, ex);
        }

        Field keyField = this.config.getKey_Schema().field(fieldName);
        if (null != keyField) {
          log.trace("process() - Setting key field '{}' to '{}'", keyField.name(), fieldValue);
          keyStruct.put(keyField, fieldValue);
        }
      }

      if (log.isInfoEnabled() && this.csvReader.getLinesRead() % ((long) this.config.getFileBatchSizeConfig()* 20) == 0) {
        log.info("Processed {} lines of {}", this.csvReader.getLinesRead(), this.fileMetadata);
      }

      addRecord(records, keyStruct, valueStruct);
    }
    return records;
  }



  protected void addRecord(List<SourceRecord> records, Struct keyStruct, Struct valueStruct) {
    Map<String, ?> sourceOffset = ImmutableMap.of(
            "offset",
            recordOffset()
    );
    log.trace("addRecord() - {}", sourceOffset);

    long timestamp = this.inputFileModifiedTime;

    SourceRecord sourceRecord = new SourceRecord(
            this.sourcePartition,
            sourceOffset,
            this.config.getTopicConfig(),
            null,
            this.config.getKey_Schema(),
            keyStruct,
            this.config.getValue_Schema(),
            valueStruct,
            timestamp
    );
    records.add(sourceRecord);
  }


  public long recordOffset() {
    return this.csvReader.getLinesRead();
  }


  private void initializeConfigurations(InputStream inputStream, Map<String, String> metadata, final Long lastOffset) {
    try {
      csvParser = config.createCSVParserBuilder().build();
      streamReader = new InputStreamReader(inputStream, "UTF-8");
      csvReader = config.createCSVReaderBuilder(streamReader,csvParser).build();

      fieldNames = new String[this.config.getValue_Schema().fields().size()];
      int index = 0;
      for (Field field : this.config.getValue_Schema().fields()) {
        fieldNames[index++] = field.name();
      }
      log.info("configure() - field names from schema order. fields = {}", Joiner.on(", ").join(fieldNames));

    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
  }


  @Override
  public void stop() {
    //TODO: Do whatever is required to stop your task.
  }


  private static void checkDirectory(File directoryPath) {
    if (log.isInfoEnabled()) {
      log.info("Checking if directory '{}' exists.",
              directoryPath
      );
    }

    String errorMessage = String.format(
            "Directory for '%s' does not exist ",
            directoryPath
    );

    if (!directoryPath.isDirectory()) {
      throw new ConnectException(
              errorMessage,
              new FileNotFoundException(directoryPath.getAbsolutePath())
      );
    }

    if (log.isInfoEnabled()) {
      log.info("Checking to ensure  '{}' is writable ",  directoryPath);
    }

    errorMessage = String.format(
            "Directory for '%s' it not writable.",
            directoryPath
    );

    File temporaryFile = null;

    try {
      temporaryFile = File.createTempFile(".permission", ".testing", directoryPath);
    } catch (IOException ex) {
      throw new ConnectException(
              errorMessage,
              ex
      );
    } finally {
      try {
        if (null != temporaryFile && temporaryFile.exists()) {
          Preconditions.checkState(temporaryFile.delete(), "Unable to delete temp file in %s", directoryPath);
        }
      } catch (Exception ex) {
        if (log.isWarnEnabled()) {
          log.warn("Exception thrown while deleting {}.", temporaryFile, ex);
        }
      }
    }
  }

  File processingFile(File input) {
    String fileName = input.getName() + PROCESSINGFILEEXTENSION;
    return new File(input.getParentFile(), fileName);
  }

  File findNextInputFile() {
    File[] input = this.config.getLookupFolderConfig().listFiles(this.config.getInputFilenameFilter());
    if (null == input || input.length == 0) {
      log.debug("No files matching {} were found in {}", config.INPUT_FILE_PATTERN_CONF, this.config.getLookupFolderConfig());
      return null;
    }
    List<File> files = new ArrayList<>(input.length);
    for (File f : input) {
      File processingFile = processingFile(f);
      log.trace("Checking for processing file: {}", processingFile);

      if (processingFile.exists()) {
        log.debug("Skipping {} because processing file exists.", f);
        continue;
      }
      files.add(f);
    }

    File result = null;

    for (File file : files) {
      long fileAgeMS = System.currentTimeMillis() - file.lastModified();

      if (fileAgeMS < 0L) {
        log.warn("File {} has a date in the future.", file);
      }

      if (this.config.getFileMinimumAgeMsConf()  > 0L && fileAgeMS < this.config.getFileMinimumAgeMsConf()) {
        log.debug("Skipping {} because it does not meet the minimum age.", file);
        continue;
      }
      result = file;
      break;
    }

    return result;
  }


}