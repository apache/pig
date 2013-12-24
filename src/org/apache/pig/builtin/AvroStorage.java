/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.builtin;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.avro.AvroArrayReader;
import org.apache.pig.impl.util.avro.AvroRecordReader;
import org.apache.pig.impl.util.avro.AvroRecordWriter;
import org.apache.pig.impl.util.avro.AvroStorageSchemaConversionUtilities;
import org.apache.pig.impl.util.avro.AvroTupleWrapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;

/**
 * Pig UDF for reading and writing Avro data.
 *
 */
public class AvroStorage extends LoadFunc
    implements StoreFuncInterface, LoadMetadata, LoadPushDown {

  /**
   *  Creates new instance of Pig Storage function, without specifying
   *  the schema. Useful for just loading in data.
   */
  public AvroStorage() {
    this(null, null);
  }

  /**
   *  Creates new instance of Pig Storage function.
   *  @param sn Specifies the input/output schema or record name.
   */
  public AvroStorage(final String sn) {
    this(sn, null);
  }

  private String schemaName = "pig_output";
  private String schemaNameSpace = null;
  protected boolean allowRecursive = false;
  protected boolean doubleColonsToDoubleUnderscores = false;
  protected Schema schema;
  protected final Log log = LogFactory.getLog(getClass());

  /**
   *  Creates new instance of AvroStorage function, specifying output schema
   *  properties.
   *  @param sn Specifies the input/output schema or record name.
   *  @param opts Options for AvroStorage:
   *  <li><code>-namespace</code> Namespace for an automatically generated
   *    output schema.</li>
   *  <li><code>-schemafile</code> Specifies URL for avro schema file
   *    from which to read the input schema (can be local file, hdfs,
   *    url, etc).</li>
   *  <li><code>-examplefile</code> Specifies URL for avro data file from
   *    which to copy the input schema (can be local file, hdfs, url, etc).</li>
   *  <li><code>-allowrecursive</code> Option to allow recursive schema
   *    definitions (default is false).</li>
   *  <li><code>-doublecolons</code> Option to translate Pig schema names
   *    with double colons to names with double underscores (default is false).</li>
   *
   */
  public AvroStorage(final String sn, final String opts) {
    super();

    if (sn != null && sn.length() > 0) {
      try {
        Schema s = (new Schema.Parser()).parse(sn);
        // must be a valid schema
        setInputAvroSchema(s);
        setOutputAvroSchema(s);
      } catch (SchemaParseException e) {
        // not a valid schema, use as a record name
        schemaName = sn;
      }
    }

    if (opts != null) {
      String[] optsArr = opts.split(" ");
      Options validOptions = new Options();
      try {
        CommandLineParser parser = new GnuParser();
        validOptions.addOption("n", "namespace", true,
            "Namespace for an automatically generated output schema");
        validOptions.addOption("f", "schemafile", true,
            "Specifies URL for avro schema file from which to read "
            + "the input or output schema");
        validOptions.addOption("e", "examplefile", true,
            "Specifies URL for avro data file from which to copy "
            + "the output schema");
        validOptions.addOption("r", "allowrecursive", false,
            "Option to allow recursive schema definitions (default is false)");
        validOptions.addOption("d", "doublecolons", false,
            "Option to translate Pig schema names with double colons "
            + "to names with double underscores (default is false)");
        CommandLine configuredOptions = parser.parse(validOptions, optsArr);
        schemaNameSpace = configuredOptions.getOptionValue("namespace", null);
        allowRecursive = configuredOptions.hasOption('r');
        doubleColonsToDoubleUnderscores = configuredOptions.hasOption('d');

        if (configuredOptions.hasOption('f')) {
          try {
            Path p = new Path(configuredOptions.getOptionValue('f'));
            Schema s = new Schema.Parser()
              .parse((FileSystem.get(p.toUri(), new Configuration()).open(p)));  
            setInputAvroSchema(s);
            setOutputAvroSchema(s);
          } catch (FileNotFoundException fnfe) {
            System.err.printf("file not found exception\n");
            log.warn("Schema file not found when instantiating AvroStorage. (If the " + 
                "schema was described in a local file on the front end, and this message " + 
                "is in the back end log, you can ignore this mesasge.)", fnfe);
          }
        } else if (configuredOptions.hasOption('e')) {
          setOutputAvroSchema(
              getAvroSchema(configuredOptions.getOptionValue('e'),
                  new Job(new Configuration())));
        }

      } catch (ParseException e) {
        log.error("Exception in AvroStorage", e);
        log.error("AvroStorage called with arguments " + sn + ", " + opts);
        warn("ParseException in AvroStorage", PigWarning.UDF_WARNING_1);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("AvroStorage(',', '[options]')", validOptions);
        throw new RuntimeException(e);
      } catch (IOException e) {
        log.warn("Exception in AvroStorage", e);
        log.warn("AvroStorage called with arguments " + sn + ", " + opts);
        warn("IOException in AvroStorage", PigWarning.UDF_WARNING_1);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Context signature for this UDF instance.
   */
  protected String udfContextSignature = null;

  @Override
  public final void setUDFContextSignature(final String signature) {
    udfContextSignature = signature;
    super.setUDFContextSignature(signature);
  }

  /**
   * Internal function for getting the Properties object associated with
   * this UDF instance.
   * @return The Properties object associated with this UDF instance
   */
  protected final Properties getProperties() {
    if (udfContextSignature == null) {
      return getProperties(AvroStorage.class, null);
    } else {
      return getProperties(AvroStorage.class, udfContextSignature);
    }
  }

  /**
   * Internal function for getting the Properties object associated with
   * this UDF instance.
   * @param c Class of this UDF
   * @param signature Signature string
   * @return The Properties object associated with this UDF instance
   */
  @SuppressWarnings("rawtypes")
  protected final Properties getProperties(final Class c,
      final String signature) {
    UDFContext context = UDFContext.getUDFContext();
    if (signature == null) {
      return context.getUDFProperties(c);
    } else {
      return context.getUDFProperties(c, new String[] {signature});
    }

  }

  /*
   * @see org.apache.pig.LoadMetadata#getSchema(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public final ResourceSchema getSchema(final String location,
      final Job job) throws IOException {
    if (schema == null) {
      Schema s = getAvroSchema(location, job);
      setInputAvroSchema(s);
    }

    ResourceSchema rs = AvroStorageSchemaConversionUtilities
        .avroSchemaToResourceSchema(schema, allowRecursive);

    return rs;
  }

  /**
   * Reads the avro schema at the specified location.
   * @param location Location of file
   * @param job Hadoop job object
   * @return an Avro Schema object derived from the specified file
   * @throws IOException
   *
   */
  protected final Schema getAvroSchema(final String location, final Job job)
      throws IOException {
    String[] locations = getPathStrings(location);
    Path[] paths = new Path[locations.length];
    for (int i = 0; i < paths.length; ++i) {
      paths[i] = new Path(locations[i]);
    }

    return getAvroSchema(paths, job);
  }

  /**
   * A PathFilter that filters out invisible files.
   */
  protected static final PathFilter VISIBLE_FILES = new PathFilter() {
    @Override
    public boolean accept(final Path p) {
      return (!(p.getName().startsWith("_") || p.getName().startsWith(".")));
    }
  };

  /**
   * Reads the avro schemas at the specified location.
   * @param p Location of file
   * @param job Hadoop job object
   * @return an Avro Schema object derived from the specified file
   * @throws IOException
   *
   */
  public Schema getAvroSchema(final Path[] p, final Job job) throws IOException {
    GenericDatumReader<Object> avroReader = new GenericDatumReader<Object>();
    ArrayList<FileStatus> statusList = new ArrayList<FileStatus>();
    FileSystem fs = FileSystem.get(p[0].toUri(), job.getConfiguration());
    for (Path temp : p) {
      for (FileStatus tempf : fs.globStatus(temp)) {
        statusList.add(tempf);
      }
    }
    FileStatus[] statusArray = (FileStatus[]) statusList
        .toArray(new FileStatus[statusList.size()]);

    if (statusArray == null) {
      throw new IOException("Path " + p.toString() + " does not exist.");
    }

    if (statusArray.length == 0) {
      throw new IOException("No path matches pattern " + p.toString());
    }

    Path filePath = depthFirstSearchForFile(statusArray, fs);

    if (filePath == null) {
      throw new IOException("No path matches pattern " + p.toString());
    }

    InputStream hdfsInputStream = fs.open(filePath);
    DataFileStream<Object> avroDataStream = new DataFileStream<Object>(
        hdfsInputStream, avroReader);
    Schema s = avroDataStream.getSchema();
    avroDataStream.close();
    return s;
  }

  /**
   * Finds a valid path for a file from a FileStatus object.
   * @param fileStatus FileStatus object corresponding to a file,
   * or a directory.
   * @param fileSystem FileSystem in with the file should be found
   * @return The first file found
   * @throws IOException
   */

  private Path depthFirstSearchForFile(final FileStatus fileStatus,
      final FileSystem fileSystem) throws IOException {
    if (fileSystem.isFile(fileStatus.getPath())) {
      return fileStatus.getPath();
    } else {
      return depthFirstSearchForFile(
          fileSystem.listStatus(fileStatus.getPath(), VISIBLE_FILES),
          fileSystem);
    }

  }

  /**
   * Finds a valid path for a file from an array of FileStatus objects.
   * @param statusArray Array of FileStatus objects in which to search
   * for the file.
   * @param fileSystem FileSystem in which to search for the first file.
   * @return The first file found.
   * @throws IOException
   */
  protected Path depthFirstSearchForFile(final FileStatus[] statusArray,
      final FileSystem fileSystem) throws IOException {

    // Most recent files first
    Arrays.sort(statusArray,
        new Comparator<FileStatus>() {
          @Override
          public int compare(final FileStatus fs1, final FileStatus fs2) {
              return Longs.compare(fs2.getModificationTime(),fs1.getModificationTime());
            }
          }
    );

    for (FileStatus f : statusArray) {
      Path p = depthFirstSearchForFile(f, fileSystem);
      if (p != null) {
        return p;
      }
    }

    return null;

  }

  /*
   * @see org.apache.pig.LoadMetadata#getStatistics(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public final ResourceStatistics getStatistics(final String location,
      final Job job) throws IOException {
    return null;
  }

  /*
   * @see org.apache.pig.LoadMetadata#getPartitionKeys(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public final String[] getPartitionKeys(final String location,
      final Job job) throws IOException {
    return null;
  }

  /*
   * @see
   * org.apache.pig.LoadMetadata#setPartitionFilter(org.apache.pig.Expression)
   */
  @Override
  public void setPartitionFilter(final Expression partitionFilter)
      throws IOException {
  }

  /*
   * @see
   * org.apache.pig.StoreFuncInterface#relToAbsPathForStoreLocation(java.lang
   * .String, org.apache.hadoop.fs.Path)
   */
  @Override
  public final String relToAbsPathForStoreLocation(final String location,
      final Path curDir) throws IOException {
    return LoadFunc.getAbsolutePath(location, curDir);
  }

  /*
   * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
   */
  @Override
  public OutputFormat<NullWritable, Object> getOutputFormat()
      throws IOException {

    /**
     * Hadoop output format for AvroStorage.
     */
    class AvroStorageOutputFormat extends
      FileOutputFormat<NullWritable, Object> {

      @Override
      public RecordWriter<NullWritable, Object> getRecordWriter(
          final TaskAttemptContext tc) throws IOException,
      InterruptedException {

        return new AvroRecordWriter(
            // avroStorageOutputFormatSchema,
            getDefaultWorkFile(tc, AvroOutputFormat.EXT),
            tc.getConfiguration());

      }
    }

    return new AvroStorageOutputFormat();

  }

  /*
   * @see org.apache.pig.StoreFuncInterface#setStoreLocation(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public final void setStoreLocation(final String location,
      final Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
  }

  /**
   * Pig property name for the output avro schema.
   */
  public static final String OUTPUT_AVRO_SCHEMA =
      "org.apache.pig.builtin.AvroStorage.output.schema";

  /*
   * @see
   * org.apache.pig.StoreFuncInterface#checkSchema(org.apache.pig.ResourceSchema
   * )
   */
  @Override
  public final void checkSchema(final ResourceSchema rs) throws IOException {
    if (rs == null) {
      throw new IOException("checkSchema: called with null ResourceSchema");
    }
    Schema avroSchema = AvroStorageSchemaConversionUtilities
        .resourceSchemaToAvroSchema(rs,
            (schemaName == null || schemaName.length() == 0)
                ? "pig_output" : schemaName,
                schemaNameSpace,
                Maps.<String, List<Schema>> newHashMap(),
                doubleColonsToDoubleUnderscores);
    if (avroSchema == null) {
      throw new IOException("checkSchema: could not translate ResourceSchema to Avro Schema");
    }
    setOutputAvroSchema(avroSchema);
  }

  /**
   * Sets the output avro schema to {@s}.
   * @param s An Avro schema
   */
  protected final void setOutputAvroSchema(final Schema s) {
    schema = s;
    getProperties()
      .setProperty(OUTPUT_AVRO_SCHEMA, s.toString());
  }

  /**
   * Utility function that gets the output schema from the udf
   * properties for this instance of the store function.
   * @return the output schema associated with this UDF
   */
  protected final Schema getOutputAvroSchema() {
    if (schema == null) {
      String schemaString = 
              getProperties()
          .getProperty(OUTPUT_AVRO_SCHEMA);
      if (schemaString != null) {
        schema = (new Schema.Parser()).parse(schemaString);
      }
    }
    return schema;

  }
  
  /**
   * RecordWriter used by this UDF instance.
   */
  private RecordWriter<NullWritable, Object> writer;

  /*
   * @see
   * org.apache.pig.StoreFuncInterface#prepareToWrite(org.apache.hadoop.mapreduce
   * .RecordWriter)
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public final void prepareToWrite(final RecordWriter w) throws IOException {
    if (this.udfContextSignature == null)
      throw new IOException(this.getClass().toString() + ".prepareToWrite called without setting udf context signature");
    writer = (RecordWriter<NullWritable, Object>) w;
    ((AvroRecordWriter) writer).prepareToWrite(getOutputAvroSchema());
  }

  /*
   * @see org.apache.pig.StoreFuncInterface#putNext(org.apache.pig.data.Tuple)
   */
  @Override
  public final void putNext(final Tuple t) throws IOException {
    try {
      writer.write(null, t);
    } catch (InterruptedException e) {
      log.error("InterruptedException in putNext");
      throw new IOException(e);
    }
  }

  /*
   * @see
   * org.apache.pig.StoreFuncInterface#setStoreFuncUDFContextSignature(java.
   * lang.String)
   */
  @Override
  public final void setStoreFuncUDFContextSignature(final String signature) {
    udfContextSignature = signature;
    super.setUDFContextSignature(signature);
  }

  /*
   * @see org.apache.pig.StoreFuncInterface#cleanupOnFailure(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */
  @Override
  public final void cleanupOnFailure(final String location,
      final Job job) throws IOException {
    StoreFunc.cleanupOnFailureImpl(location, job);
  }

  /**
   * Pig property name for the input avro schema.
   */
  public static final String INPUT_AVRO_SCHEMA =
      "org.apache.pig.builtin.AvroStorage.input.schema";

  /*
   * @see org.apache.pig.LoadFunc#setLocation(java.lang.String,
   * org.apache.hadoop.mapreduce.Job)
   */

  @Override
  public void setLocation(final String location, final Job job)
      throws IOException {
    FileInputFormat.setInputPaths(job, location);
    if (schema == null) {
      schema = getInputAvroSchema();
      if (schema == null) {
        schema = getAvroSchema(location, job);
        if (schema == null) {
          throw new IOException(
              "Could not determine avro schema for location " + location);
        }
        setInputAvroSchema(schema);
      }
    }
  }

  /**
   * Sets the input avro schema to {@s}.
   * @param s The specified schema
   */
  protected final void setInputAvroSchema(final Schema s) {
    schema = s;
    getProperties().setProperty(INPUT_AVRO_SCHEMA, s.toString());
  }

  /**
   * Helper function reads the input avro schema from the UDF
   * Properties.
   * @return The input avro schema
   */
  public final Schema getInputAvroSchema() {
    if (schema == null) {
      String schemaString = getProperties().getProperty(INPUT_AVRO_SCHEMA);
      if (schemaString != null) {
        Schema s = new Schema.Parser().parse(schemaString);
        schema = s;
      }
    }
    return schema;
  }

  /*
   * @see org.apache.pig.LoadFunc#getInputFormat()
   */
  @Override
  public InputFormat<NullWritable, GenericData.Record> getInputFormat()
      throws IOException {

    return new org.apache.pig.backend.hadoop.executionengine.mapReduceLayer
        .PigFileInputFormat<NullWritable, GenericData.Record>() {

      @Override
      public RecordReader<NullWritable, GenericData.Record>
        createRecordReader(final InputSplit is, final TaskAttemptContext tc)
          throws IOException, InterruptedException {
        Schema s = getInputAvroSchema();
        RecordReader<NullWritable, GenericData.Record> rr = null;
        if (s.getType() == Type.ARRAY) {
          rr = new AvroArrayReader(s);
        } else {
          rr = new AvroRecordReader(s);
        }
        rr.initialize(is, tc);
        tc.setStatus(is.toString());
        return rr;
      }
    };

  }

  @SuppressWarnings("rawtypes") private RecordReader reader;
  PigSplit split;

  /*
   * @see
   * org.apache.pig.LoadFunc#prepareToRead(org.apache.hadoop.mapreduce.RecordReader
   * , org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit)
   */
  @SuppressWarnings("rawtypes")
  @Override
  public final void prepareToRead(final RecordReader r, final PigSplit s)
      throws IOException {
    reader = r;
    split = s;
  }

  /*
   * @see org.apache.pig.LoadFunc#getNext()
   */

  @Override
  public final Tuple getNext() throws IOException {
    try {
      if (reader.nextKeyValue()) {
        return new AvroTupleWrapper<GenericData.Record>(
            (GenericData.Record) reader.getCurrentValue());
      } else {
        return null;
      }
    } catch (InterruptedException e) {
      throw new IOException("Wrapped Interrupted Exception", e);
    }
  }

  @Override
  public void cleanupOnSuccess(final String location, final Job job)
      throws IOException {
  }

  @Override
  public List<OperatorSet> getFeatures() {
      return Lists.newArrayList(LoadPushDown.OperatorSet.PROJECTION);
  }

  /**
   * List of required fields passed by pig in a push down projection.
   */
  protected RequiredFieldList requiredFieldList;

  /*
   * @see
   * org.apache.pig.LoadPushDown#pushProjection(org.apache.pig.LoadPushDown.
   * RequiredFieldList)
   */
  @Override
  public RequiredFieldResponse pushProjection(final RequiredFieldList rfl)
          throws FrontendException {
      requiredFieldList = rfl;
      
      Schema newSchema = AvroStorageSchemaConversionUtilities
              .newSchemaFromRequiredFieldList(schema, rfl);
      if (newSchema != null) {
          schema = newSchema;
          setInputAvroSchema(schema);
          return new RequiredFieldResponse(true);
      } else {
          log.warn("could not select fields subset " + rfl + "\n");
          warn("could not select fields subset", PigWarning.UDF_WARNING_2);
          return new RequiredFieldResponse(false);
      }

  }

}
