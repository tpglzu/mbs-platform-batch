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

package cascading.tuple.hadoop.util;

import java.util.Comparator;

import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.util.TupleHasher;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

/**
 *
 */
public class HasherPartitioner extends TupleHasher implements JobConfigurable
  {
  private static Comparator defaultComparator;
  private Comparator[] comparators;

  public void configure( JobConf jobConf )
    {
    if( defaultComparator == null )
      defaultComparator = TupleSerialization.getDefaultComparator( jobConf );

    comparators = DeserializerComparator.getFieldComparatorsFrom( jobConf, "cascading.group.comparator" );

    initialize( defaultComparator, comparators );
    }
  }
