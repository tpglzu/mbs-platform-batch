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

package cascading.flow.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowElement;
import cascading.flow.FlowProcess;
import cascading.flow.planner.Scope;
import cascading.pipe.Splice;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryChainIterator;
import cascading.tuple.TupleEntryIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class SpliceGate extends Gate<TupleEntry, Grouping<TupleEntry, TupleEntryIterator>> implements ElementDuct, Collapsing
  {
  private static final Logger LOG = LoggerFactory.getLogger( SpliceGate.class );

  protected Duct[] orderedPrevious;

  public enum Role
    {
      sink, source, both
    }

  protected final FlowProcess flowProcess;
  protected Role role = Role.both;

  private TrapHandler trapHandler;
  private Set<String> branchNames;

  protected final Splice splice;
  protected final List<Scope> incomingScopes = new ArrayList<Scope>();
  protected final List<Scope> outgoingScopes = new ArrayList<Scope>();
  protected Fields[] keyFields;
  protected Fields[] sortFields;
  protected Fields[] valuesFields;

  protected Grouping<TupleEntry, TupleEntryIterator> grouping;
  protected TupleEntryChainIterator tupleEntryIterator;
  protected TupleEntry keyEntry;

  public SpliceGate( FlowProcess flowProcess, Splice splice )
    {
    this.splice = splice;

    if( splice.hasConfigDef() )
      flowProcess = new ElementFlowProcess( flowProcess, splice.getConfigDef() );

    this.flowProcess = flowProcess;
    }

  public SpliceGate( FlowProcess flowProcess, Splice splice, Role role )
    {
    this.splice = splice;
    this.flowProcess = flowProcess;
    this.role = role;
    }

  public void setBranchNames( Set<String> branchNames )
    {
    this.branchNames = branchNames;
    }

  public Set<String> getBranchNames()
    {
    return branchNames;
    }

  @Override
  public void setTrapHandler( TrapHandler trapHandler )
    {
    this.trapHandler = trapHandler;
    }

  @Override
  public boolean hasTrapHandler()
    {
    return trapHandler != null;
    }

  protected void handleReThrowableException( String message, Throwable throwable )
    {
    trapHandler.handleReThrowableException( message, throwable );
    }

  protected void handleException( Throwable exception, TupleEntry tupleEntry )
    {
    trapHandler.handleException( exception, tupleEntry );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    if( incomingScopes.size() == 0 )
      throw new IllegalStateException( "incoming scopes may not be empty" );

    if( outgoingScopes.size() == 0 )
      throw new IllegalStateException( "outgoing scope may not be empty" );

    keyFields = new Fields[ incomingScopes.size() ];
    valuesFields = new Fields[ incomingScopes.size() ];

    if( splice.isSorted() )
      sortFields = new Fields[ incomingScopes.size() ];

    for( Scope incomingScope : incomingScopes )
      {
      // for GroupBy, incoming may have same name, but guaranteed to have same key/value/sort fields for merge
      int pos = splice.isGroupBy() ? 0 : splice.getPipePos().get( incomingScope.getName() );

      keyFields[ pos ] = outgoingScopes.get( 0 ).getKeySelectors().get( incomingScope.getName() );
      valuesFields[ pos ] = incomingScope.isEvery() ? incomingScope.getOutGroupingFields() : incomingScope.getOutValuesFields();

      if( sortFields != null )
        sortFields[ pos ] = outgoingScopes.get( 0 ).getSortingSelectors().get( incomingScope.getName() );

      if( LOG.isDebugEnabled() )
        {
        LOG.debug( "incomingScope: {}, in pos: {}", incomingScope.getName(), pos );
        LOG.debug( "keyFields: {}", printSafe( keyFields[ pos ] ) );
        LOG.debug( "valueFields: {}", printSafe( valuesFields[ pos ] ) );

        if( sortFields != null )
          LOG.debug( "sortFields: {}", printSafe( sortFields[ pos ] ) );
        }
      }

    if( role == Role.sink )
      return;

    keyEntry = new TupleEntry( outgoingScopes.get( 0 ).getOutGroupingFields(), true );
    tupleEntryIterator = new TupleEntryChainIterator( outgoingScopes.get( 0 ).getOutValuesFields() );

    grouping = new Grouping<TupleEntry, TupleEntryIterator>();
    grouping.key = keyEntry;
    grouping.iterator = tupleEntryIterator;
    }

  @Override
  public FlowElement getFlowElement()
    {
    return splice;
    }

  @Override
  public List<Scope> getOutgoingScopes()
    {
    return outgoingScopes;
    }

  @Override
  public List<Scope> getIncomingScopes()
    {
    return incomingScopes;
    }

  public void addIncomingScope( Scope incomingScope )
    {
    incomingScopes.add( incomingScope );
    }

  public void addOutgoingScope( Scope outgoingScope )
    {
    outgoingScopes.add( outgoingScope );
    }

  @Override
  public void cleanup()
    {
    super.cleanup();

    // close if top of stack
    if( next == null )
      TrapHandler.closeTraps();
    }

  protected synchronized void orderDucts( StreamGraph streamGraph )
    {
    orderedPrevious = new Duct[ incomingScopes.size() ];

    if( incomingScopes.size() == 1 && splice.getPrevious().length == 1 )
      {
      orderedPrevious[ 0 ] = allPrevious[ 0 ];
      return;
      }

    for( Duct previous : allPrevious )
      orderedPrevious[ streamGraph.ordinalBetween( previous, this ) ] = previous;
    }

  protected void makePosMap( Map<Duct, Integer> posMap )
    {
    for( int i = 0; i < orderedPrevious.length; i++ )
      {
      if( orderedPrevious[ i ] != null )
        posMap.put( orderedPrevious[ i ], i );
      }
    }

  private String printSafe( Fields fields )
    {
    if( fields != null )
      return fields.printVerbose();

    return "";
    }

  @Override
  public final boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof SpliceGate ) )
      return false;

    SpliceGate spliceGate = (SpliceGate) object;

    if( splice != null ? splice != spliceGate.splice : spliceGate.splice != null )
      return false;

    return true;
    }

  @Override
  public final int hashCode()
    {
    return splice != null ? System.identityHashCode( splice ) : 0;
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( getClass().getSimpleName() );
    sb.append( "{splice=" ).append( splice );
    sb.append( '}' );
    return sb.toString();
    }
  }
