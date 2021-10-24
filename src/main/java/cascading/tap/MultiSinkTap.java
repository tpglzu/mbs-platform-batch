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

package cascading.tap;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.scheme.NullScheme;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class MultiSinkTap is both a {@link cascading.tap.CompositeTap} and {@link cascading.tap.SinkTap} that can write to
 * multiple child {@link cascading.tap.Tap} instances simultaneously.
 * <p/>
 * It is the counterpart to {@link cascading.tap.MultiSourceTap}.
 */
public class MultiSinkTap<Child extends Tap, Config, Output> extends SinkTap<Config, Output> implements CompositeTap<Child>
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( MultiSinkTap.class );

  /** Field taps */
  private final Child[] taps;
  /** Field tempPath */
  private final String tempPath = "__multisink_placeholder_" + Util.createUniqueID();
  /** Field childConfigs */
  private List<Map<String, String>> childConfigs;

  private class MultiSinkCollector extends TupleEntryCollector
    {
    TupleEntryCollector[] collectors;

    public MultiSinkCollector( FlowProcess<Config> flowProcess, Tap... taps ) throws IOException
      {
      super( Fields.asDeclaration( getSinkFields() ) );

      collectors = new TupleEntryCollector[ taps.length ];

      Config conf = flowProcess.getConfigCopy();

      for( int i = 0; i < taps.length; i++ )
        {
        Config mergedConf = childConfigs == null ? conf : flowProcess.mergeMapIntoConfig( conf, childConfigs.get( i ) );
        Tap tap = taps[ i ];
        LOG.info( "opening for write: {}", tap.toString() );

        collectors[ i ] = tap.openForWrite( flowProcess.copyWith( mergedConf ) );
        }
      }

    protected void collect( TupleEntry tupleEntry ) throws IOException
      {
      for( int i = 0; i < taps.length; i++ )
        collectors[ i ].add( tupleEntry );
      }

    @Override
    public void close()
      {
      super.close();

      try
        {
        for( TupleEntryCollector collector : collectors )
          {
          try
            {
            collector.close();
            }
          catch( Exception exception )
            {
            LOG.warn( "exception closing TupleEntryCollector", exception );
            }
          }
        }
      finally
        {
        collectors = null;
        }
      }
    }

  /**
   * Constructor MultiSinkTap creates a new MultiSinkTap instance.
   *
   * @param taps of type Tap...
   */
  @ConstructorProperties({"taps"})
  public MultiSinkTap( Child... taps )
    {
    this.taps = taps;
    }

  protected Child[] getTaps()
    {
    return taps;
    }

  @Override
  public Iterator<Child> getChildTaps()
    {
    return Arrays.asList( getTaps() ).iterator();
    }

  @Override
  public long getNumChildTaps()
    {
    return getTaps().length;
    }

  @Override
  public String getIdentifier()
    {
    return tempPath;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<Config> flowProcess, Output output ) throws IOException
    {
    return new MultiSinkCollector( flowProcess, getTaps() );
    }

  @Override
  public void sinkConfInit( FlowProcess<Config> process, Config conf )
    {
    childConfigs = new ArrayList<Map<String, String>>();

    for( int i = 0; i < getTaps().length; i++ )
      {
      Tap tap = getTaps()[ i ];
      Config jobConf = process.copyConfig( conf );

      tap.sinkConfInit( process, jobConf );

      childConfigs.add( process.diffConfigIntoMap( conf, jobConf ) );
      }
    }

  @Override
  public boolean createResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.createResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean deleteResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.deleteResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean commitResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.commitResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean rollbackResource( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.rollbackResource( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public boolean resourceExists( Config conf ) throws IOException
    {
    for( Tap tap : getTaps() )
      {
      if( !tap.resourceExists( conf ) )
        return false;
      }

    return true;
    }

  @Override
  public long getModifiedTime( Config conf ) throws IOException
    {
    long modified = getTaps()[ 0 ].getModifiedTime( conf );

    for( int i = 1; i < getTaps().length; i++ )
      modified = Math.max( getTaps()[ i ].getModifiedTime( conf ), modified );

    return modified;
    }

  @Override
  public Scheme getScheme()
    {
    if( super.getScheme() != null )
      return super.getScheme();

    Set<Comparable> fieldNames = new LinkedHashSet<Comparable>();

    for( int i = 0; i < getTaps().length; i++ )
      {
      for( Object o : getTaps()[ i ].getSinkFields() )
        fieldNames.add( (Comparable) o );
      }

    Fields allFields = new Fields( fieldNames.toArray( new Comparable[ fieldNames.size() ] ) );

    setScheme( new NullScheme( allFields, allFields ) );

    return super.getScheme();
    }

  @Override
  public String toString()
    {
    return "MultiSinkTap[" + ( taps == null ? "none" : Arrays.asList( taps ) ) + ']';
    }

  @Override
  public boolean equals( Object o )
    {
    if( this == o )
      return true;
    if( !( o instanceof MultiSinkTap ) )
      return false;
    if( !super.equals( o ) )
      return false;

    MultiSinkTap that = (MultiSinkTap) o;

    if( !Arrays.equals( taps, that.taps ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( taps != null ? Arrays.hashCode( taps ) : 0 );
    return result;
    }
  }
