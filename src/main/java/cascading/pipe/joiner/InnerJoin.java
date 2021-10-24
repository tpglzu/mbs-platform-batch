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

package cascading.pipe.joiner;

import java.util.Iterator;

import cascading.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class InnerJoin will return an {@link Iterator} that will iterate over a given {@link Joiner} and return tuples that represent
 * and inner join of the CoGrouper internal grouped tuple collections.
 */
public class InnerJoin implements Joiner
  {
  /** Field LOG */
  private static final Logger LOG = LoggerFactory.getLogger( InnerJoin.class );

  public Iterator<Tuple> getIterator( JoinerClosure closure )
    {
    return new JoinIterator( closure );
    }

  public int numJoins()
    {
    return -1;
    }

  public static class JoinIterator implements Iterator<Tuple>
    {
    final JoinerClosure closure;
    Iterator[] iterators;
    Comparable[] lastValues;

    public JoinIterator( JoinerClosure closure )
      {
      this.closure = closure;

      LOG.debug( "cogrouped size: {}", closure.size() );

      init();
      }

    protected void init()
      {
      iterators = new Iterator[ closure.size() ];

      for( int i = 0; i < closure.size(); i++ )
        iterators[ i ] = getIterator( i );
      }

    protected Iterator getIterator( int i )
      {
      return closure.getIterator( i );
      }

    private Comparable[] initLastValues()
      {
      lastValues = new Comparable[ iterators.length ];

      for( int i = 0; i < iterators.length; i++ )
        lastValues[ i ] = (Comparable) iterators[ i ].next();

      return lastValues;
      }

    public final boolean hasNext()
      {
      // if this is the first pass, and there is an iterator without a next value,
      // then we have no next element
      if( lastValues == null )
        {
        for( Iterator iterator : iterators )
          {
          if( !iterator.hasNext() )
            return false;
          }

        return true;
        }

      for( Iterator iterator : iterators )
        {
        if( iterator.hasNext() )
          return true;
        }

      return false;
      }

    public Tuple next()
      {
      if( lastValues == null )
        return makeResult( initLastValues() );

      for( int i = iterators.length - 1; i >= 0; i-- )
        {
        if( iterators[ i ].hasNext() )
          {
          lastValues[ i ] = (Comparable) iterators[ i ].next();
          break;
          }

        // reset to first
        iterators[ i ] = getIterator( i );
        lastValues[ i ] = (Comparable) iterators[ i ].next();
        }

      return makeResult( lastValues );
      }

    private Tuple makeResult( Comparable[] lastValues )
      {
      Tuple result = new Tuple();

      // flatten the results into one Tuple
      for( Comparable lastValue : lastValues )
        result.addAll( lastValue );

      if( LOG.isTraceEnabled() )
        LOG.trace( "tuple: {}", result.print() );

      return result;
      }

    public void remove()
      {
      // unsupported
      }
    }
  }
