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

package cascading.tuple.hadoop.io;

import java.io.IOException;
import java.io.OutputStream;

import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.io.TupleOutputStream;
import org.apache.hadoop.io.serializer.Serializer;

abstract class BaseSerializer<T> implements Serializer<T>
  {
  private final TupleSerialization.SerializationElementWriter elementWriter;
  TupleOutputStream outputStream;

  protected BaseSerializer( TupleSerialization.SerializationElementWriter elementWriter )
    {
    this.elementWriter = elementWriter;
    }

  public void open( OutputStream out )
    {
    if( out instanceof TupleOutputStream )
      outputStream = (TupleOutputStream) out;
    else
      outputStream = new HadoopTupleOutputStream( out, elementWriter );
    }

  public void close() throws IOException
    {
    try
      {
      if( outputStream != null ) // my never be opened
        outputStream.close();
      }
    finally
      {
      outputStream = null;
      }
    }
  }
