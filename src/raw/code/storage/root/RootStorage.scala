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
import raw.schema.DataType
import raw.schema.ROWID
import raw.schema.BOOL
import raw.schema.FLOAT
import raw.schema.INT32
import raw.schema.UINT32

class RootOuterKnownSchemaTable(val ref: Reference, schema: Schema) extends KnownSchemaTable(schema) {
  
  def getSequentialAccessPath(fields: Schema): SequentialAccessPath =
    new RootOuterSequentialAccessPath(ref, fields)
  
  def getKeyAccessPath(fields: Schema): KeyAccessPath =
    throw new RuntimeException("RootOuterKnownSchemaTable.getKeyAccessPath not implemented")
  
}

class RootInnerKnownSchemaTable(val ref: Reference, schema: Schema) extends KnownSchemaTable(schema) {
  
  def getSequentialAccessPath(fields: Schema): SequentialAccessPath =
    throw new RuntimeException("RootInnerKnownSchemaTable.getSequentialAccessPath not implemented")
  
  def getKeyAccessPath(fields: Schema): KeyAccessPath =
    new RootInnerKeyAccessPath(ref, fields)
  
}

class RootStorage extends Storage {
  override val toString = "ROOT: " + reference.name
  
  private val eventSchema = new Schema(List(Field("ID", ROWID, false),
                                            Field("RunNumber", UINT32, false),
                                            Field("lbn", UINT32, false),
                                            Field("EF_e24vhi_medium1", BOOL, false),
                                            Field("EF_e60_medium1", BOOL, false),
                                            Field("EF_2e12Tvh_loose1", BOOL, false),
                                            Field("EF_mu24i_tight", BOOL, false),
                                            Field("EF_mu36_tight", BOOL, false),
                                            Field("EF_2mu13", BOOL, false)))
  private val eventTable = new RootOuterKnownSchemaTable(reference, eventSchema)
  TableCatalog.registerTable("events", eventTable)
  
  private val muonSchema = new Schema(List(Field("ID", ROWID, false),
                                           Field("mu_ptcone20", FLOAT, false),
                                           Field("mu_pt", FLOAT, false),
                                           Field("mu_eta", FLOAT, false),
                                           Field("mu_isCombinedMuon", INT32, false),
                                           Field("mu_isLowPtReconstructedMuon", INT32, false),
                                           Field("mu_tight", INT32, false),
                                           Field("mu_expectBLayerHit", INT32, false),
                                           Field("mu_nBLHits", INT32, false),
                                           Field("mu_nPixHits", INT32, false),
                                           Field("mu_nPixelDeadSensors", INT32, false),
                                           Field("mu_nSCTHits", INT32, false),
                                           Field("mu_nSCTDeadSensors", INT32, false),
                                           Field("mu_nPixHoles", INT32, false),
                                           Field("mu_nSCTHoles", INT32, false)))
  private val muonTable = new RootInnerKnownSchemaTable(reference, muonSchema)
  TableCatalog.registerTable("muons", muonTable)

  private val electronSchema = new Schema(List(Field("ID", ROWID, false),
                                               Field("el_ptcone20", FLOAT, false),
                                               Field("el_pt", FLOAT, false),
                                               Field("el_eta", FLOAT, false),
                                               Field("el_author", INT32, false),
                                               Field("el_medium", INT32, false)))
  private val electronTable = new RootInnerKnownSchemaTable(reference, electronSchema)
  TableCatalog.registerTable("electrons", electronTable)

  private val jetSchema = new Schema(List(Field("ID", ROWID, false),
                                          Field("jet_E", FLOAT, false),
                                          Field("jet_pt", FLOAT, false),
                                          Field("jet_phi", FLOAT, false),
                                          Field("jet_eta", FLOAT, false),
                                          Field("jet_flavor_weight_JetFitterCOMBNN", FLOAT, false),
                                          Field("jet_flavor_weight_SV1", FLOAT, false),
                                          Field("jet_flavor_weight_IP3D", FLOAT, false)))
  private val jetTable = new RootInnerKnownSchemaTable(reference, jetSchema)
  TableCatalog.registerTable("jets", jetTable)
  
  def init(): List[Instruction] =
    List(Instruction("ROOT_INIT"))      

  def done(): List[Instruction] = List()
}
