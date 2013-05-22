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
package raw.code.storage.csv

import raw.Util

import raw.catalog.Table
import raw.catalog.KnownSchemaTable
import raw.catalog.TableCatalog

import raw.code.Instruction
import raw.code.Reference
import raw.code.storage.Storage
import raw.code.storage.SequentialAccessPath
import raw.code.storage.KeyAccessPath

import raw.schema.Field
import raw.schema.Schema

class CsvKnownSchemaTable(val ref: Reference, schema: Schema) extends KnownSchemaTable(schema) {

  def getSequentialAccessPath(fields: Schema): SequentialAccessPath =
    new CsvSequentialAccessPath(ref, fields, schema)
  
  def getKeyAccessPath(fields: Schema): KeyAccessPath =
    throw new RuntimeException("CsvKeyAccessPath not implemented")
}

class CsvStorage(val schema: Schema) extends Storage {
  override val toString = "CSV: " + reference.name
  
  private val table = new CsvKnownSchemaTable(reference, schema)
  TableCatalog.registerTable("csv", table)
  
  def init(): List[Instruction] = List()
    
  def done(): List[Instruction] = List()
}