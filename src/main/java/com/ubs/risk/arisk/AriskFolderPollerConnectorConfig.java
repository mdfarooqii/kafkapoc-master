package com.ubs.risk.arisk;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.PatternFilenameFilter;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReaderBuilder;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.regex.Pattern;


public class AriskFolderPollerConnectorConfig extends AbstractConfig {
  public final Schema key_Schema ;
  public final Schema value_Schema;
  public final PatternFilenameFilter inputFilenameFilter;

  public static final String LOOKUP_FOLDER_CONFIG = "lookup.folder";
  private static final String LOOKUP_FOLDER_DOC = "The folder to poll for the files by the connector.";

  public static final String LOOKUP_FILE_FILTER_CONFIG = "lookup.file.filtername";
  private static final String LOOKUP_FILE_FILTER_DOC = "The file name pattern , in the  specified folder by the connector.";

  public static final String FILE_BATCH_SIZE_CONFIG = "file.batchSize";
  private static final String FILE_BATCH_SIZE_DOC = "The batch size that the records will be read from the file.";


  public static final String FILE_FIELD_SEPARATOR_CONFIG = "file.field.separator";
  private static final String FILE_FIELD_SEPARATOR_DOC = "The separator for the files.";

  public static final String KEY_SCHEMA_CONFIG = "key.schema";
  private static final String KEY_SCHEMA_DOC = "The key schema for the file record, in json format.";

  public static final String VALUE_SCHEMA_CONFIG = "value.schema";
  private static final String VALUE_SCHEMA_DOC = "The value schema for the file records, in json format.";


  public static final String SUCCESS_FOLDER_CONFIG = "success.folder";
  private static final String SUCCESS_FOLDER_DOC = "The folder that the file will be pushed after processing successfully.";


  public static final String ERROR_FOLDER_CONFIG = "error.folder";
  private static final String ERROR_FOLDER_DOC = "The folder that the file will be pushed if file error.";

  public static final String IGNORE_HEADER_LINE = "ignore.first.line";
  private static final String IGNORE_HEADER_LINE_DOC = "Ignore the first line as the header file.";

  private static final String  ESCAPE_CHAR_CONFIG = "file.csv.escapeChar";
  private static final String  ESCAPE_CHAR_DOC = "This is the escape character";

  private static final String  SEPARATOR_CHAR_CONFIG = "file.csv.separator";
  private static final String  SEPARATOR_CHAR_DOC = "This is the escape character";

  public static final String CSV_SKIP_LINES_CONFIG = "csv.skip.lines";
  private static final String CSV_SKIP_LINES_DOC = "Number of lines to skip while reading";

  public static final String INPUT_FILE_PATTERN_CONF = "input.file.pattern";
  private static final String INPUT_FILE_PATTERN_DOC = "This is the file name filter";


  public static final String FILE_MINIMUM_AGE_MS_CONF = "file.minimum.age.ms";
  public static final String FILE_MINIMUM_AGE_MS_DOC = "This is the minimun time to wait after file is created";

  public static final String PROCESSING_FILE_EXTENSION_CONF = "processing.file.extension";
    public static final String PROCESSING_FILE_EXTENSION_DOC= "This is the file extension for processing files, by default it's .process";

  public static final String TOPIC_CONFIG = "output.topic";
  public static final String TOPIC_DOC = "This is the output topic that needs to be configured";


    public static ConfigDef conf() {
        return new ConfigDef()
                .define(LOOKUP_FOLDER_CONFIG, Type.STRING, Importance.HIGH, LOOKUP_FOLDER_DOC)
                .define(LOOKUP_FILE_FILTER_CONFIG, Type.STRING, Importance.HIGH, LOOKUP_FILE_FILTER_DOC)
                .define(FILE_BATCH_SIZE_CONFIG, Type.STRING, Importance.HIGH, FILE_BATCH_SIZE_DOC)
                // .define(FILE_FIELD_SEPARATOR_CONFIG, Type.STRING, Importance.HIGH, FILE_FIELD_SEPARATOR_DOC)
                .define(KEY_SCHEMA_CONFIG, Type.STRING, Importance.HIGH, KEY_SCHEMA_DOC)
                .define(VALUE_SCHEMA_CONFIG, Type.STRING, Importance.HIGH, VALUE_SCHEMA_DOC)
                .define(SUCCESS_FOLDER_CONFIG, Type.STRING, Importance.HIGH, SUCCESS_FOLDER_DOC)
                .define(ERROR_FOLDER_CONFIG, Type.STRING, Importance.HIGH, ERROR_FOLDER_DOC)
                .define(IGNORE_HEADER_LINE, Type.STRING, Importance.HIGH, IGNORE_HEADER_LINE_DOC)
                .define(SEPARATOR_CHAR_CONFIG, Type.STRING, Importance.HIGH, SEPARATOR_CHAR_DOC)
                .define(CSV_SKIP_LINES_CONFIG, Type.STRING, Importance.HIGH, CSV_SKIP_LINES_DOC)
                .define(TOPIC_CONFIG, Type.STRING, Importance.HIGH, TOPIC_DOC)
                .define(INPUT_FILE_PATTERN_CONF, Type.STRING, Importance.HIGH, INPUT_FILE_PATTERN_DOC)
                .define(FILE_MINIMUM_AGE_MS_CONF, Type.STRING, Importance.HIGH, FILE_MINIMUM_AGE_MS_DOC)
                .define(PROCESSING_FILE_EXTENSION_CONF, Type.STRING, Importance.HIGH, TOPIC_DOC)
                ;
    }

    /**
     * This is the constructor
     * @param parsedConfig
     */
    public AriskFolderPollerConnectorConfig(Map<String, String> parsedConfig) {
        super(conf(), parsedConfig);

        SchemaObject keySchemaObj = convertReadSchema(getKeySchemaConfig()) ; //readSchema(getKeySchemaConfig());
        SchemaObject valueSchemaObj = convertReadSchema(getValueSchemaConfig()) ; // readSchema(getValueSchemaConfig());

        SchemaBuilder ksb = SchemaBuilder.struct().name(keySchemaObj.getSchemaName());
        SchemaBuilder vsb = SchemaBuilder.struct().name(valueSchemaObj.getSchemaName());

        if(keySchemaObj !=null && !keySchemaObj.getFields().isEmpty()){
            for(AriskField f : keySchemaObj.getFields()){
                Schema s = createSchema(f);
                ksb.field(f.getFieldName(), s);
            }
        }
        key_Schema = ksb;

        if(valueSchemaObj !=null && !valueSchemaObj.getFields().isEmpty()){
            for(AriskField f : valueSchemaObj.getFields()){
                Schema s = createSchema(f);
                vsb.field(f.getFieldName(), s);
            }
        }
        value_Schema = vsb;
        System.out.println("key schema is ....."+key_Schema.toString());
        System.out.println("Value schema is ....."+value_Schema.toString());

        final String inputPatternText = this.getString(INPUT_FILE_PATTERN_CONF);
        final Pattern inputPattern = Pattern.compile(inputPatternText);
        this.inputFilenameFilter = new PatternFilenameFilter(inputPattern);
    }


    /**
     * Utility method to create schema
     * TODO: Move to util class
     * TODO: Add support for the rest of the Schema.Types
     * @param field
     * @return
     */
    private Schema createSchema(AriskField field) {
        Schema fieldSch = null;
        switch (field.getFieldType()){
            case "String"  :  fieldSch = (field.getFieldOptional().equalsIgnoreCase("true") ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);  break;
            case "Integer"  : fieldSch = (field.getFieldOptional().equalsIgnoreCase("true") ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);  break;
        };
        return fieldSch;
    }

    public CSVParserBuilder createCSVParserBuilder() {

        return new CSVParserBuilder()
                /*.withEscapeChar(this.escapeChar)
                .withIgnoreLeadingWhiteSpace(this.ignoreLeadingWhitespace)
                .withIgnoreQuotations(this.ignoreQuotations)
                .withQuoteChar(this.quoteChar)
                .withStrictQuotes(this.strictQuotes)
                .withFieldAsNull(nullFieldIndicator)
                */
                .withSeparator(getFileFieldSeparatorConfig());
    }


    public CSVReaderBuilder createCSVReaderBuilder(Reader reader, CSVParser parser) {
        return new CSVReaderBuilder(reader)
                .withCSVParser(parser)
                .withSkipLines(getCsvSkipLinesConfig())
                /*.withKeepCarriageReturn(this.keepCarriageReturn)

                .withVerifyReader(this.verifyReader)
                .withFieldAsNull(nullFieldIndicator)*/
                ;
    }


  public static SchemaObject convertReadSchema(String jsonString){
    //String jsonString = "{\"schemaName\": \"PositionKeySchema\",\"fields\": [  {\"fieldName\": \"positionId\", \"fieldType\": \"string\", \"fieldOptional\": \"false\" },  {       \"fieldName\": \"positionName\", \"fieldType\": \"string\",\"fieldOptional\": \"false\"   } ]}";
    ObjectMapper objectMapper = new ObjectMapper();
    SchemaObject schemaObject = null;

    //convert json string to object
    try {
      schemaObject = objectMapper.readValue(jsonString, SchemaObject.class);
      if(schemaObject!=null){
        System.out.println("schema object is not null "+schemaObject.getSchemaName() + "\n fields list "+ schemaObject.getFields().size() );
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return schemaObject;
  }



    public  String getTopicConfig() {
        return this.getString(TOPIC_CONFIG);
    }


    public PatternFilenameFilter getInputFilenameFilter() {
        return this.inputFilenameFilter;
    }

    public String getEscapeCharConfig() {
        return this.getString(ESCAPE_CHAR_CONFIG);
    }

    public  String getSeparatorCharConfig() {
        return this.getString(SEPARATOR_CHAR_CONFIG);
    }



    public String getInputFilePatternConf() {
        return this.getString(INPUT_FILE_PATTERN_CONF);
    }

    public int getFileMinimumAgeMsConf() {
        return this.getInt(FILE_MINIMUM_AGE_MS_CONF);
    }

    public String getProcessingFileExtensionConf() {
        return this.getString(PROCESSING_FILE_EXTENSION_CONF);
    }
/*Schema readSchema(final String key) {
    String schema = this.getString(key);
    Schema result;

    if (Strings.isNullOrEmpty(schema)) {
      result = null;
    } else {
      try {
        result = ObjectMapperFactory.INSTANCE.readValue(schema, Schema.class);
      } catch (IOException e) {
        throw new DataException("Could not read schema from '" + key + "'", e);
      }
    }

    return result;
  }*/


    public  char getCsvSkipLinesConfig() {
        return this.getChar( this.getString(CSV_SKIP_LINES_CONFIG));
    }

    public File getLookupFolderConfig() {
        return this.getAbsoluteFile(this.getString(LOOKUP_FOLDER_CONFIG) , LOOKUP_FOLDER_CONFIG);
    }

    public File getSuccessFolderConfig() {
        return this.getAbsoluteFile(this.getString(SUCCESS_FOLDER_CONFIG) , SUCCESS_FOLDER_CONFIG);
    }

    public File getFailureFolderConfig() {
        return this.getAbsoluteFile(this.getString(ERROR_FOLDER_CONFIG) , ERROR_FOLDER_CONFIG);
    }

    public static File getAbsoluteFile(String path, String key) {
        File file = new File(path);
        Preconditions.checkState(file.isAbsolute(), "'%s' must be an absolute path.", new Object[]{key});
        return new File(path);
    }

  public String getLookupFolderDoc() {
    return this.getString(LOOKUP_FOLDER_DOC);
  }

  public String getLookupFileFilterConfig() {
    return this.getString(LOOKUP_FILE_FILTER_CONFIG);
  }

  public String getLookupFileFilterDoc() {
    return this.getString(LOOKUP_FILE_FILTER_DOC);
  }

  public  int getFileBatchSizeConfig() {
    return this.getInt(FILE_BATCH_SIZE_CONFIG);
  }



  public  char getFileFieldSeparatorConfig() {
    return this.getChar( this.getString(FILE_FIELD_SEPARATOR_CONFIG));
  }


    final char getChar(String key) {
        int intValue = this.getInt(key);
        return (char) intValue;
    }

    public Schema getKey_Schema() {
        return key_Schema;
    }

    public Schema getValue_Schema() {
        return value_Schema;
    }

    public String getFileFieldSeparatorDoc() {
    return this.getString(FILE_FIELD_SEPARATOR_DOC);
  }

  public  String getKeySchemaConfig() {
    return this.getString(KEY_SCHEMA_CONFIG);
  }

  public String getKeySchemaDoc() {
    return this.getString(KEY_SCHEMA_DOC);
  }

  public  String getValueSchemaConfig() {
    return this.getString(VALUE_SCHEMA_CONFIG);
  }

  public String getValueSchemaDoc() {
    return this.getString(VALUE_SCHEMA_DOC);
  }



}
