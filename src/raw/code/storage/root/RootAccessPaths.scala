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
package raw.code.storage.root

import raw.Util

import raw.code.Instruction
import raw.code.Reference

import raw.code.storage.SequentialAccessPath
import raw.code.storage.KeyAccessPath

import raw.schema.DataType
import raw.schema.ROWID
import raw.schema.UINT32
import raw.schema.INT32
import raw.schema.FLOAT
import raw.schema.BOOL
import raw.schema.Field
import raw.schema.Schema

private object RootUtil {
  def getRootType(dataType: DataType) =
    dataType match {
      case ROWID => "Long64_t"
      case UINT32 => "UInt_t"
      case INT32 => "Int_t"
      case FLOAT => "Float_t"
      case BOOL => "Bool_t"
    }
}

class RootOuterSequentialAccessPath(ref: Reference, fields: Schema) extends SequentialAccessPath(ref, fields) {
  private val rootId = Util.getUniqueId()  
  // FIXME: uniqueId is actually the for-loop variable, so rename it accordingly
  private val uniqueId = Util.getUniqueId()
  
  def init(): List[Instruction] =
    List(Instruction("ROOT_FILE_ADD", rootId, ref.name)) ++
    (for (field <- fields.fields; if !field.name.contains("ID"))
      yield Instruction("ROOT_FIELD_SET", rootId, field.name, RootUtil.getRootType(field.dataType)))
  
  def open(): List[Instruction] =
    List(Instruction("ROOT_OUTER_LOOP_BEGIN", rootId, uniqueId))    
      
  def close(): List[Instruction] =
    List(Instruction("ROOT_OUTER_LOOP_END"))
    
  def done(): List[Instruction] = List()

  def getNextTuple(): List[Instruction] =
    List(Instruction("ROOT_ROW_GET", rootId, uniqueId)) ++
    (for (fieldName <- fields.names; if !fieldName.contains("ID"))
      yield Instruction("ROOT_FIELD_GET", rootId, uniqueId, fieldName))
    
  def getField(field: Field): Reference =
    if (field.name == "EventID") Reference(uniqueId)
    else Reference(rootId + field.name)
}


class RootInnerKeyAccessPath(ref: Reference, fields: Schema) extends KeyAccessPath(ref, fields) {
  private val rootId = Util.getUniqueId()  
  // FIXME: Rename uniqueId to smtg more meaningful
  private val uniqueId = Util.getUniqueId()
  // FIXME: Use of mutable state is ugly...
  private var keyId: String = ""

  def init(): List[Instruction] =
    List(Instruction("ROOT_FILE_ADD", rootId, ref.name)) ++
    (for (field <- fields.fields; if !field.name.contains("ID"))
      yield Instruction("ROOT_VECTOR_SET", rootId, field.name, RootUtil.getRootType(field.dataType)))
  
  def open(key: Reference): List[Instruction] =  {
    keyId = key.name
    List(Instruction("ROOT_ROW_GET", rootId, key.name)) ++
    (for (fieldName <- fields.names; if !fieldName.contains("ID"))
      yield Instruction("ROOT_FIELD_GET", rootId, key.name, fieldName)) ++
    List(Instruction("ROOT_VECTOR_LOOP_BEGIN", rootId, uniqueId, fields.names.filterNot(_.contains("ID"))(0)))
  }
  
  def close(key: Reference): List[Instruction] =
    List(Instruction("ROOT_VECTOR_LOOP_END"))
  
  def done(): List[Instruction] = List()
    
  def getNextTuple(): List[Instruction] =
    for (field <- fields.fields; if !field.name.contains("ID"))
      yield Instruction("ROOT_VECTOR_GET", rootId, "vec_" + field.name, RootUtil.getRootType(field.dataType), field.name, uniqueId)
    
  def getField(field: Field): Reference =
    if (field.name == "EventID") Reference(keyId)
    else if (field.name.contains("ID")) Reference(uniqueId)
    else Reference(rootId + "vec_" + field.name)
}