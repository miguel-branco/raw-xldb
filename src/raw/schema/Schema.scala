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
package raw.schema

import scala.collection.mutable.ListBuffer

sealed abstract class DataType
case object ANY extends DataType
case object ROWID extends DataType
case object UINT64 extends DataType
case object INT64 extends DataType
case object UINT32 extends DataType
case object INT32 extends DataType
case object FLOAT extends DataType
case object BOOL extends DataType

case class Field(val name: String, val dataType: DataType, val nullable: Boolean) 

class Schema(val fields: List[Field]) {
  def hasField(fieldName: String): Boolean =
    fields.filter(_.name == fieldName).length != 0
  
  def getField(fieldName: String): Field =
    fields.filter(_.name == fieldName)(0)
  
  def names: List[String] =
    fields.map(_.name)
    
  def ++(rhs: Schema): Schema =
    new Schema(fields ++ rhs.fields)
}
