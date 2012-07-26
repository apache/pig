package org.apache.pig.builtin.mock;

import static org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.getUniqueFile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.PigServer;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreMetadata;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * A convenient mock Storage for unit tests
 *
 * <pre>
 *  PigServer pigServer = new PigServer(ExecType.LOCAL);
 *  Data data = resetData(pigServer);
 *      data.set("foo",
 *      tuple("a"),
 *      tuple("b"),
 *      tuple("c")
 *      );
 *
 *  pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
 *  pigServer.registerQuery("STORE A INTO 'bar' USING mock.Storage();");
 *
 *  List<Tuple> out = data.get("bar");
 *
 *  assertEquals(tuple("a"), out.get(0));
 *  assertEquals(tuple("b"), out.get(1));
 *  assertEquals(tuple("c"), out.get(2));
 * </pre>
 * With Schema:
 *  <pre>
 *  PigServer pigServer = new PigServer(ExecType.LOCAL);
 *  Data data = resetData(pigServer);
 *
 *  data.set("foo", "blah:chararray",
 *      tuple("a"),
 *      tuple("b"),
 *      tuple("c")
 *      );
 *
 *  pigServer.registerQuery("A = LOAD 'foo' USING mock.Storage();");
 *  pigServer.registerQuery("B = FOREACH A GENERATE blah as a, blah as b;");
 *  pigServer.registerQuery("STORE B INTO 'bar' USING mock.Storage();");
 *
 *  assertEquals(schema("a:chararray,b:chararray"), data.getSchema("bar"));
 *  
 *  List<Tuple> out = data.get("bar");
 *  assertEquals(tuple("a", "a"), out.get(0));
 *  assertEquals(tuple("b", "b"), out.get(1));
 *  assertEquals(tuple("c", "c"), out.get(2));
 * </pre>
 */
public class Storage extends LoadFunc implements StoreFuncInterface, LoadMetadata, StoreMetadata {
  private static final String PIG_CONTEXT_KEY = "pig.mock.storage.id";
  private static final Logger LOG = Logger.getLogger(Storage.class);
  private static Map<Integer, Data> idToData = new HashMap<Integer, Data>();
  private static TupleFactory TF = TupleFactory.getInstance();
  private static BagFactory BF = BagFactory.getInstance();

  private static int nextId;

  /**
   * @param objects
   * @return a tuple containing the provided objects
   */
  public static Tuple tuple(Object... objects) {
    return TF.newTuple(Arrays.asList(objects));
  }

  /**
   * @param tuples
   * @return a bag containing the provided objects
   */
  public static DataBag bag(Tuple... tuples) {
    return BF.newDefaultBag(Arrays.asList(tuples));
  }
  
  /**
   * @param schema
   * @return the schema represented by the string
   * @throws ParserException if the schema is invalid
   */
  public static Schema schema(String schema) throws ParserException {
	  return Utils.getSchemaFromString(schema);
  }

  /**
   * reset the store and get the Data object to access it
   * @param pigServer
   * @return
   */
  public static Data resetData(PigServer pigServer) {
    return resetData(pigServer.getPigContext());
  }

  /**
   * reset the store and get the Data object to access it
   * @param context
   * @return
   */
  public static Data resetData(PigContext context) {
    Properties properties = context.getProperties();

    // cleaning up previous data
    try {
      if (properties.contains(PIG_CONTEXT_KEY)) {
        Integer previousId = new Integer(properties.getProperty(PIG_CONTEXT_KEY));
        idToData.remove(previousId);
      }
    } catch (RuntimeException e) {
      LOG.warn("invalid id in context properties for "+PIG_CONTEXT_KEY, e);
    }

    // setting new Store
    int id = nextId++;
    properties.setProperty(PIG_CONTEXT_KEY, String.valueOf(id));
    Data data = new Data();
    idToData.put(id, data);
    return data;
  }

  private Data getData(Job job) throws IOException {
    String stringId = job.getConfiguration().get(PIG_CONTEXT_KEY);
    if (stringId == null) {
      throw new IOException("no Data prepared for this Script. " +
      		"You need to call Storage.resetData(pigServer.getPigContext()) first");
    }
    Data data = idToData.get(new Integer(stringId));
    if (data == null) {
      throw new IOException("no Data anymore for this Script. " +
          "Has data been reset by another Storage.resetData(pigServer.getPigContext()) ?");
    }
    return data;
  }

  private static class Parts {
    final String location;
    final Map<String, Collection<Tuple>> parts = new HashMap<String, Collection<Tuple>>();

    public Parts(String location) {
      super();
      this.location = location;
    }

    public void set(String partFile, Collection<Tuple> data) {
      if (parts.put(partFile, data) != null) {
        throw new RuntimeException("the part " + partFile + " for location " + location + " already exists");
      }
    }

    public List<Tuple> getAll() {
        List<Tuple> all = new ArrayList<Tuple>();
        Set<Entry<String, Collection<Tuple>>> entrySet = parts.entrySet();
        for (Entry<String, Collection<Tuple>> entry : entrySet) {
            all.addAll(entry.getValue());
        }
        return all;
    }

  }
  
  /**
   * An isolated data store to avoid side effects
   *
   */
  public static class Data implements Serializable {
    private static final long serialVersionUID = 1L;
    private Map<String, Parts> locationToData = new HashMap<String, Parts>();
    private Map<String, Schema> locationToSchema = new HashMap<String, Schema>();

    /**
     * to set the data in a location with a known schema
     *
     * @param location "where" to store the tuples
     * @param schema the schema of the data
     * @param data the tuples to store
     * @throws ParserException if schema is invalid
     */
    public void set(String location, String schema, Collection<Tuple> data) throws ParserException {
      set(location, Utils.getSchemaFromString(schema), data);
    }

    /**
     * to set the data in a location with a known schema
     *
     * @param location "where" to store the tuples
     * @param schema
     * @param data the tuples to store
     * @throws ParserException if schema is invalid
     */
    public void set(String location, String schema, Tuple... data) throws ParserException {
      set(location, Utils.getSchemaFromString(schema), Arrays.asList(data));
    }
    
    /**
     * to set the data in a location with a known schema
     *
     * @param location "where" to store the tuples
     * @param schema
     * @param data the tuples to store
     */
    public void set(String location, Schema schema, Collection<Tuple> data) {
      set(location, data);
      if (locationToSchema.put(location, schema) != null) {
          throw new RuntimeException("schema already set for location "+location);
      }
    }

    /**
     * to set the data in a location with a known schema
     *
     * @param location "where" to store the tuples
     * @param schema
     * @param data the tuples to store
     */
    public void set(String location, Schema schema, Tuple... data) {
      set(location, schema, Arrays.asList(data));
    }

    /**
     * to set the data in a location
     *
     * @param location "where" to store the tuples
     * @param data the tuples to store
     */
    private void setInternal(String location, String partID, Collection<Tuple> data) {
        Parts parts = locationToData.get(location);
        if (partID == null) {
            if (parts == null) {
                partID = "mock";
            } else {
                throw new RuntimeException("Can not set location " + location + " twice");
            }
        }
        if (parts == null) {
            parts = new Parts(location);
            locationToData.put(location, parts);
        }
        parts.set(partID, data);
    }

    /**
     * to set the data in a location
     *
     * @param location "where" to store the tuples
     * @param data the tuples to store
     */
    public void set(String location, Collection<Tuple> data) {
      setInternal(location, null, data);
    }

    /**
     * to set the data in a location
     *
     * @param location "where" to store the tuples
     * @param data the tuples to store
     */
    public void set(String location, Tuple... data) {
        set(location, Arrays.asList(data));
    }
    
    /**
     *
     * @param location
     * @return the data in this location
     */
    public List<Tuple> get(String location) {
      if (!locationToData.containsKey(location)) {
        throw new RuntimeException("No data for location '" + location + "'");
      }
      return locationToData.get(location).getAll();
    }

    /**
     * 
     * @param location
     * @return the schema stored in this location
     */
	public Schema getSchema(String location) {
		return locationToSchema.get(location);
	}

	/**
	 * to set the schema for a given location
	 * @param location
	 * @param schema
	 */
	public void setSchema(String location, Schema schema) {
		locationToSchema.put(location, schema);
	}

  }

  private String location;

  private Data data;
  
  private Schema schema;

  private Iterator<Tuple> dataBeingRead;
private MockRecordWriter mockRecordWriter;

  private void init(String location, Job job) throws IOException {
	  this.data = getData(job);
	  this.location = location;
	  this.schema = data.getSchema(location);
  }
  // LoadFunc

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
	this.location = location;
    return location;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    init(location, job);
    this.dataBeingRead = data.get(location).iterator();
  }

  @Override
  public InputFormat<?, ?> getInputFormat() throws IOException {
    return new MockInputFormat(location);
  }

  @Override
  public LoadCaster getLoadCaster() throws IOException {
    return super.getLoadCaster();
  }

  @Override
  public void prepareToRead(@SuppressWarnings("rawtypes") RecordReader reader, PigSplit split) throws IOException {
  }

  @Override
  public Tuple getNext() throws IOException {
    if (dataBeingRead == null) {
      throw new IOException("data was not correctly initialized in MockLoader");
    }
    return dataBeingRead.hasNext() ? dataBeingRead.next() : null;
  }

  @Override
  public void setUDFContextSignature(String signature) {
    super.setUDFContextSignature(signature);
  }
  
  // LoadMetaData
  
  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
	init(location, job);
  	return schema == null ? null : new ResourceSchema(schema);
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job)
  		throws IOException {
	init(location, job);
  	return null;
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException {
	init(location, job);
  	return null;
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
  }

  // StoreFunc

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
    this.location = location;
    return location;
  }

  @Override
  public OutputFormat<?, ?> getOutputFormat() throws IOException {
    return new MockOutputFormat();
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
	init(location, job);
  }

  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
  }

  @Override
  public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
      mockRecordWriter = (MockRecordWriter) writer;
      this.data.setInternal(location, mockRecordWriter.partID, mockRecordWriter.dataBeingWritten);
  }

  @Override
  public void putNext(Tuple t) throws IOException {
      mockRecordWriter.dataBeingWritten.add(t);
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
	init(location, job);
  }

  // StoreMetaData
  
  @Override
  public void storeStatistics(ResourceStatistics stats, String location, Job job)
  		throws IOException {
	init(location, job);
  }

  @Override
  public void storeSchema(ResourceSchema schema, String location, Job job)
  		throws IOException {
	init(location, job);
	data.setSchema(location, Schema.getPigSchema(schema));
  }
  
  // Mocks for LoadFunc

  private static class MockRecordReader extends RecordReader<Object, Object> {
    @Override
    public void close() throws IOException {
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
      return "mockKey";
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
      return "mockValue";
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0.5f;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext arg1) throws IOException,
        InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return true;
    }
  }

  private static class MockInputSplit extends InputSplit implements Writable  {
    private String location;

    // used through reflection by Hadoop
    @SuppressWarnings("unused")
    public MockInputSplit() {
    }

    public MockInputSplit(String location) {
      this.location = location;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[] { location };
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 10000000;
    }

    @Override
    public boolean equals(Object arg0) {
      return arg0==this;
    }

    @Override
    public int hashCode() {
      return location.hashCode();
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
      location = arg0.readUTF();
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
      arg0.writeUTF(location);
    }
  }

  private static class MockInputFormat extends InputFormat<Object, Object> {

    private final String location;

    public MockInputFormat(String location) {
      this.location = location;
    }

    @Override
    public RecordReader<Object, Object> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
        throws IOException, InterruptedException {
      return new MockRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
      return Arrays.<InputSplit>asList(new MockInputSplit(location));
    }
  }

  // mocks for StoreFunc
  private static final class MockRecordWriter extends RecordWriter<Object, Object> {

    private final List<Tuple> dataBeingWritten = new ArrayList<Tuple>();
    private final String partID;

    public MockRecordWriter(String partID) {
        super();
        this.partID = partID;
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    }

    @Override
    public void write(Object arg0, Object arg1) throws IOException, InterruptedException {
    }

  }

  private static class MockOutputCommitter extends OutputCommitter {

    @Override
    public void abortTask(TaskAttemptContext arg0) throws IOException {
    }

    @Override
    public void commitTask(TaskAttemptContext arg0) throws IOException {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
      return true;
    }

    @Override
    public void setupJob(JobContext arg0) throws IOException {
    }

    @Override
    public void setupTask(TaskAttemptContext arg0) throws IOException {
    }

  }

  private static final class MockOutputFormat extends OutputFormat<Object, Object> {

    @Override
    public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException,
    InterruptedException {
      return new MockOutputCommitter();
    }

    @Override
    public RecordWriter<Object, Object> getRecordWriter(TaskAttemptContext arg0) throws IOException,
    InterruptedException {
      return new MockRecordWriter(getUniqueFile(arg0, "part", ".mock"));
    }

  }

}
