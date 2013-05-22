/*
   RAW -- High-performance querying over raw, never-seen-before data.
   
                         Copyright (c) 2013
      Data Intensive Applications and Systems Labaratory (DIAS)
               École Polytechnique Fédérale de Lausanne
   
                         All Rights Reserved.
   
   Permission to use, copy, modify and distribute this software and
   its documentation is hereby granted, provided that both the
   copyright notice and this permission notice appear in all copies of
   the software, derivative works or modified versions, and any
   portions thereof, and that both notices appear in supporting
   documentation.
   
   This code is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. THE AUTHORS
   DISCLAIM ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
   RESULTING FROM THE USE OF THIS SOFTWARE.
*/
package raw.catalog

import raw.code.storage.Storage
import raw.code.storage.SequentialAccessPath
import raw.code.storage.KeyAccessPath

import raw.schema.Field
import raw.schema.Schema
import raw.schema.ANY

abstract class Table {
  
  /** Default implementation uses late binding and therefore assumes all fields exist, have ANY type and are nullable.
   *  Storages may override default implementation if the schema is known. */
  def hasField(fieldName: String): Boolean = true
  def getField(fieldName: String): Field = Field(fieldName, ANY, true)

  def getSequentialAccessPath(fields: Schema): SequentialAccessPath
  def getKeyAccessPath(fields: Schema): KeyAccessPath

}

case class UnknownField(fieldName: String) extends Exception

abstract class KnownSchemaTable(val schema: Schema) extends Table {

  override def hasField(fieldName: String): Boolean =
    schema.hasField(fieldName)
    
  override def getField(fieldName: String): Field = 
    if (schema.hasField(fieldName))
      schema.getField(fieldName)
    else
      throw UnknownField(fieldName)

}