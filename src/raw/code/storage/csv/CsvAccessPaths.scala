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

import scala.collection.immutable.SortedMap

import raw.Util

import raw.code.Instruction
import raw.code.Reference

import raw.code.Reference
import raw.code.storage.SequentialAccessPath

import raw.schema.Field
import raw.schema.Schema
import raw.schema.INT32
import raw.schema.UINT32

/* CsvSequentialAccessPath assumes the schema is already known (passed in as 'schema').
 * FIXME: Add access path that does not assume the schema is known.
 * That access path would e.g. parse the 1st line of the file to collect field names and build the mapping column name -> number.
 * (Or alternatively receive directly the column number.)
 * It would return "ANY" as the field type.
 * When building the physical query plan, the optimizer would add an operator to convert "ANY" to the specific type needed by the plan.
 * (Handling errors would be tricky here.)
 */
class CsvSequentialAccessPath(ref: Reference, fields: Schema, val schema: Schema) extends SequentialAccessPath(ref, fields) {
  private val uniqueId = Util.getUniqueId()
  
  def init(): List[Instruction] =
    List(Instruction("CSV_INIT", uniqueId, ref.name))    
  
  def open(): List[Instruction] =
    List(Instruction("CSV_OUTER_LOOP_BEGIN", uniqueId))
      
  def close(): List[Instruction] =
    List(Instruction("CSV_OUTER_LOOP_END"))
    
  def done(): List[Instruction] = List()

  def getNextTuple(): List[Instruction] = {
    val fieldsPos = SortedMap[Int, Field]() ++ fields.fields.map(field => (schema.fields.indexOf(field), field))
    val lastFieldPos = fieldsPos.last
    (for (i <- 0 until schema.fields.length)
      yield
        if (fieldsPos.contains(i))
          fieldsPos(i).dataType match {
            case INT32 => Instruction("CSV_FIELD_READ_AS_INT", uniqueId, fieldsPos(i).name)
            case UINT32 => Instruction("CSV_FIELD_READ_AS_INT", uniqueId, fieldsPos(i).name)
        }
        else Instruction("CSV_FIELD_SKIP", uniqueId)).toList
  }
  
  def getField(field: Field): Reference =
    Reference(field.name)
}
