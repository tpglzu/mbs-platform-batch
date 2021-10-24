/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.pipe.assembly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class AggregateBy is a {@link SubAssembly} that serves two roles for handling aggregate operations.
 * <p/>
 * The first role is as a base class for composable aggregate operations that have a MapReduce Map side optimization for the
 * Reduce side aggregation. For example 'summing' a value within a grouping can be performed partially Map side and
 * completed Reduce side. Summing is associative and commutative.
 * <p/>
 * AggregateBy also supports operations that are not associative/commutative like 'counting'. Counting
 * would result in 'counting' value occurrences Map side but summing those counts Reduce side. (Yes, counting can be
 * transposed to summing Map and Reduce sides by emitting 1's before the first sum, but that's three operations over
 * two, and a hack)
 * <p/>
 * Think of this mechanism as a MapReduce Combiner, but more efficient as no values are serialized,
 * deserialized, saved to disk, and multi-pass sorted in the process, which consume cpu resources in trade of
 * memory and a little or no IO.
 * <p/>
 * Further, Combiners are limited to only associative/commutative operations.
 * <p/>
 * Additionally the Cascading planner can move the Map side optimization
 * to the previous Reduce operation further increasing IO performance (between the preceding Reduce and Map phase which
 * is over HDFS).
 * <p/>
 * The second role of the AggregateBy class is to allow for composition of AggregateBy
 * sub-classes. That is, {@link SumBy} and {@link CountBy} AggregateBy sub-classes can be performed
 * in parallel on the same grouping keys.
 * </p>
 * Custom AggregateBy classes can be created by sub-classing this class and implementing a special
 * {@link Functor} for use on the Map side. Multiple Functor instances are managed by the {@link CompositeFunction}
 * class allowing them all to share the same LRU value map for more efficiency.
 * <p/>
 * To tune the LRU, set the {@code threshold} value to a high enough value to utilize available memory. Or set a
 * default value via the {@link #AGGREGATE_BY_THRESHOLD} property. The current default ({@link CompositeFunction#DEFAULT_THRESHOLD})
 * is {@code 10, 000} unique keys. Note "flushes" from the LRU will be logged in threshold increments along with memory
 * information.
 * <p/>
 * Note using a AggregateBy instance automatically inserts a {@link GroupBy} into the resulting {@link cascading.flow.Flow}.
 * And passing multiple AggregateBy instances to a parent AggregateBy instance still results in one GroupBy.
 * <p/>
 * Also note that {@link Unique} is not a CompositeAggregator as it makes no sense to combine it with other aggregators,
 * and so is slightly more optimized internally.
 *
 * @see SumBy
 * @see CountBy
 * @see Unique
 */
public class AggregateBy extends SubAssembly
  {
  private static final Logger LOG = LoggerFactory.getLogger( AggregateBy.class );

  public static final String AGGREGATE_BY_THRESHOLD = "cascading.aggregateby.threshold";

  private String name;
  private int threshold;
  private Fields[] argumentFields;
  private Functor[] functors;
  private Aggregator[] aggregators;
  private transient GroupBy groupBy;

  public enum Flush
    {
      Num_Keys_Flushed
    }

  /**
   * Interface Functor provides a means to create a simple function for use with the {@link CompositeFunction} class.
   * <p/>
   * Note the {@link FlowProcess} argument provides access to the underlying properties and counter APIs.
   */
  public interface Functor extends Serializable
    {
    /**
     * Method getDeclaredFields returns the declaredFields of this Functor object.
     *
     * @return the declaredFields (type Fields) of this Functor object.
     */
    Fields getDeclaredFields();

    /**
     * Method aggregate operates on the given args in tandem (optionally) with the given context values.
     * <p/>
     * The context argument is the result of the previous call to this method. Use it to store values between aggregate
     * calls (the current count, or sum of the args).
     * <p/>
     * On the very first invocation of aggregate for a given grouping key, context will be {@code null}. All subsequent
     * invocations context will be the value returned on the previous invocation.
     *
     * @param flowProcess of type FlowProcess
     * @param args        of type TupleEntry
     * @param context     of type Tuple   @return Tuple
     */
    Tuple aggregate( FlowProcess flowProcess, TupleEntry args, Tuple context );

    /**
     * Method complete allows the final aggregate computation to be performed before the return value is collected.
     * <p/>
     * The number of values in the returned {@link Tuple} instance must match the number of declaredFields.
     * <p/>
     * It is safe to return the context object as the result value.
     *
     * @param flowProcess of type FlowProcess
     * @param context     of type Tuple  @return Tuple
     */
    Tuple complete( FlowProcess flowProcess, Tuple context );
    }

  /**
   * Class CompositeFunction takes multiple Functor instances and manages them as a single {@link Function}.
   *
   * @see Functor
   */
  public static class CompositeFunction extends BaseOperation<LinkedHashMap<Tuple, Tuple[]>> implements Function<LinkedHashMap<Tuple, Tuple[]>>
    {
    public static final int DEFAULT_THRESHOLD = 10000;

    private int threshold = 0;
    private final Fields groupingFields;
    private final Fields[] argumentFields;
    private final Fields[] functorFields;
    private final Functor[] functors;

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields
     * @param functor        of type Functor
     * @param threshold      of type int
     */
    public CompositeFunction( Fields groupingFields, Fields argumentFields, Functor functor, int threshold )
      {
      this( groupingFields, Fields.fields( argumentFields ), new Functor[]{functor}, threshold );
      }

    /**
     * Constructor CompositeFunction creates a new CompositeFunction instance.
     *
     * @param groupingFields of type Fields
     * @param argumentFields of type Fields[]
     * @param functors       of type Functor[]
     * @param threshold      of type int
     */
    public CompositeFunction( Fields groupingFields, Fields[] argumentFields, Functor[] functors, int threshold )
      {
      super( getFields( groupingFields, functors ) );
      this.groupingFields = groupingFields;
      this.argumentFields = argumentFields;
      this.functors = functors;
      this.threshold = threshold;

      functorFields = new Fields[ functors.length ];

      for( int i = 0; i < functors.length; i++ )
        functorFields[ i ] = functors[ i ].getDeclaredFields();
      }

    private static Fields getFields( Fields groupingFields, Functor[] functors )
      {
      Fields fields = groupingFields;

      for( int i = 0; i < functors.length; i++ )
        fields = fields.append( functors[ i ].getDeclaredFields() );

      return fields;
      }

    @Override
    public void prepare( final FlowProcess flowProcess, final OperationCall<LinkedHashMap<Tuple, Tuple[]>> operationCall )
      {
      if( threshold == 0 )
        {
        Object value = flowProcess.getProperty( AGGREGATE_BY_THRESHOLD );

        if( value != null && !value.toString().isEmpty() )
          threshold = Integer.valueOf( (String) flowProcess.getProperty( AGGREGATE_BY_THRESHOLD ) );
        else
          threshold = DEFAULT_THRESHOLD;
        }

      LOG.info( "using threshold value: {}", threshold );

      operationCall.setContext( new LinkedHashMap<Tuple, Tuple[]>( threshold, 0.75f, true )
      {
      long flushes = 0;

      @Override
      protected boolean removeEldestEntry( Map.Entry<Tuple, Tuple[]> eldest )
        {
        boolean doRemove = size() > threshold;

        if( doRemove )
          {
          completeFunctors( flowProcess, ( (FunctionCall) operationCall ).getOutputCollector(), eldest );
          flowProcess.increment( Flush.Num_Keys_Flushed, 1 );

          if( flushes % threshold == 0 ) // every multiple, write out data
            {
            Runtime runtime = Runtime.getRuntime();
            long freeMem = runtime.freeMemory() / 1024 / 1024;
            long maxMem = runtime.maxMemory() / 1024 / 1024;
            long totalMem = runtime.totalMemory() / 1024 / 1024;

            LOG.info( "flushed keys num times: {}, with threshold: {}", flushes + 1, threshold );
            LOG.info( "mem on flush (mb), free: " + freeMem + ", total: " + totalMem + ", max: " + maxMem );
            }

          flushes++;
          }

        return doRemove;
        }
      } );
      }

    @Override
    public void operate( FlowProcess flowProcess, FunctionCall<LinkedHashMap<Tuple, Tuple[]>> functionCall )
      {
      TupleEntry args = functionCall.getArguments();
      Tuple key = args.selectTuple( groupingFields );
      Tuple[] context = functionCall.getContext().get( key );

      if( context == null )
        {
        context = new Tuple[ functors.length ];
        functionCall.getContext().put( key, context );
        }

      for( int i = 0; i < functors.length; i++ )
        context[ i ] = functors[ i ].aggregate( flowProcess, args.selectEntry( argumentFields[ i ] ), context[ i ] );
      }

    @Override
    public void flush( FlowProcess flowProcess, OperationCall<LinkedHashMap<Tuple, Tuple[]>> operationCall )
      {
      // need to drain context
      TupleEntryCollector collector = ( (FunctionCall) operationCall ).getOutputCollector();

      for( Map.Entry<Tuple, Tuple[]> entry : operationCall.getContext().entrySet() )
        completeFunctors( flowProcess, collector, entry );

      operationCall.setContext( null );
      }

    private void completeFunctors( FlowProcess flowProcess, TupleEntryCollector outputCollector, Map.Entry<Tuple, Tuple[]> entry )
      {
      Tuple result = new Tuple( entry.getKey() );
      Tuple[] values = entry.getValue();

      for( int i = 0; i < functors.length; i++ )
        result.addAll( functors[ i ].complete( flowProcess, values[ i ] ) );

      outputCollector.add( result );
      }

    @Override
    public boolean equals( Object object )
      {
      if( this == object )
        return true;
      if( !( object instanceof CompositeFunction ) )
        return false;
      if( !super.equals( object ) )
        return false;

      CompositeFunction that = (CompositeFunction) object;

      if( threshold != that.threshold )
        return false;
      if( !Arrays.equals( argumentFields, that.argumentFields ) )
        return false;
      if( !Arrays.equals( functorFields, that.functorFields ) )
        return false;
      if( !Arrays.equals( functors, that.functors ) )
        return false;
      if( groupingFields != null ? !groupingFields.equals( that.groupingFields ) : that.groupingFields != null )
        return false;

      return true;
      }

    @Override
    public int hashCode()
      {
      int result = super.hashCode();
      result = 31 * result + threshold;
      result = 31 * result + ( groupingFields != null ? groupingFields.hashCode() : 0 );
      result = 31 * result + ( argumentFields != null ? Arrays.hashCode( argumentFields ) : 0 );
      result = 31 * result + ( functorFields != null ? Arrays.hashCode( functorFields ) : 0 );
      result = 31 * result + ( functors != null ? Arrays.hashCode( functors ) : 0 );
      return result;
      }
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param name      of type String
   * @param threshold of type int
   */
  protected AggregateBy( String name, int threshold )
    {
    this.name = name;
    this.threshold = threshold;
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param argumentFields of type Fields
   * @param functor        of type Functor
   * @param aggregator     of type Aggregator
   */
  protected AggregateBy( Fields argumentFields, Functor functor, Aggregator aggregator )
    {
    this.argumentFields = Fields.fields( argumentFields );
    this.functors = new Functor[]{functor};
    this.aggregators = new Aggregator[]{aggregator};
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param assemblies     of type CompositeAggregator...
   */
  public AggregateBy( Pipe pipe, Fields groupingFields, AggregateBy... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, CompositeFunction.DEFAULT_THRESHOLD, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param threshold      of type int
   * @param assemblies     of type CompositeAggregator...
   */
  public AggregateBy( Pipe pipe, Fields groupingFields, int threshold, AggregateBy... assemblies )
    {
    this( null, Pipe.pipes( pipe ), groupingFields, threshold, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param pipe           of type Pipe
   * @param groupingFields of type Fields
   * @param threshold      of type int
   * @param assemblies     of type CompositeAggregator...
   */
  public AggregateBy( String name, Pipe pipe, Fields groupingFields, int threshold, AggregateBy... assemblies )
    {
    this( name, Pipe.pipes( pipe ), groupingFields, threshold, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param assemblies     of type CompositeAggregator...
   */
  public AggregateBy( String name, Pipe[] pipes, Fields groupingFields, AggregateBy... assemblies )
    {
    this( name, pipes, groupingFields, CompositeFunction.DEFAULT_THRESHOLD, assemblies );
    }

  /**
   * Constructor CompositeAggregator creates a new CompositeAggregator instance.
   *
   * @param name           of type String
   * @param pipes          of type Pipe[]
   * @param groupingFields of type Fields
   * @param threshold      of type int
   * @param assemblies     of type CompositeAggregator...
   */
  public AggregateBy( String name, Pipe[] pipes, Fields groupingFields, int threshold, AggregateBy... assemblies )
    {
    this( name, threshold );

    List<Fields> arguments = new ArrayList<Fields>();
    List<Functor> functors = new ArrayList<Functor>();
    List<Aggregator> aggregators = new ArrayList<Aggregator>();

    for( int i = 0; i < assemblies.length; i++ )
      {
      AggregateBy assembly = assemblies[ i ];

      Collections.addAll( arguments, assembly.getArgumentFields() );
      Collections.addAll( functors, assembly.getFunctors() );
      Collections.addAll( aggregators, assembly.getAggregators() );
      }

    initialize( groupingFields, pipes, arguments.toArray( new Fields[ arguments.size() ] ), functors.toArray( new Functor[ functors.size() ] ), aggregators.toArray( new Aggregator[ aggregators.size() ] ) );
    }

  protected AggregateBy( String name, Pipe[] pipes, Fields groupingFields, Fields argumentFields, Functor functor, Aggregator aggregator, int threshold )
    {
    this( name, threshold );
    initialize( groupingFields, pipes, argumentFields, functor, aggregator );
    }

  protected void initialize( Fields groupingFields, Pipe[] pipes, Fields argumentFields, Functor functor, Aggregator aggregator )
    {
    initialize( groupingFields, pipes, Fields.fields( argumentFields ),
      new Functor[]{functor},
      new Aggregator[]{aggregator} );
    }

  protected void initialize( Fields groupingFields, Pipe[] pipes, Fields[] argumentFields, Functor[] functors, Aggregator[] aggregators )
    {
    this.argumentFields = argumentFields;
    this.functors = functors;
    this.aggregators = aggregators;

    verify();

    Fields argumentSelector = Fields.merge( groupingFields, Fields.merge( argumentFields ) );

    Pipe[] functions = new Pipe[ pipes.length ];

    CompositeFunction function = new CompositeFunction( groupingFields, argumentFields, functors, threshold );

    for( int i = 0; i < functions.length; i++ )
      functions[ i ] = new Each( pipes[ i ], argumentSelector, function, Fields.RESULTS );

    groupBy = new GroupBy( name, functions, groupingFields );

    Pipe pipe = groupBy;

    for( int i = 0; i < aggregators.length; i++ )
      pipe = new Every( pipe, functors[ i ].getDeclaredFields(), aggregators[ i ], Fields.ALL );

    setTails( pipe );
    }

  /** Method verify should be overridden by sub-classes if any values must be tested before the calling constructor returns. */
  protected void verify()
    {

    }

  protected Fields[] getArgumentFields()
    {
    return argumentFields;
    }

  protected Functor[] getFunctors()
    {
    return functors;
    }

  protected Aggregator[] getAggregators()
    {
    return aggregators;
    }

  /**
   * Method getGroupBy returns the internal {@link GroupBy} instance so that any custom properties
   * can be set on it via {@link cascading.pipe.Pipe#getStepConfigDef()}.
   *
   * @return GroupBy type
   */
  public GroupBy getGroupBy()
    {
    return groupBy;
    }
  }
