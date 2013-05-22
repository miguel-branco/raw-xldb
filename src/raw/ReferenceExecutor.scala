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
package raw

import raw.catalog.TableCatalog

import raw.code.CodeGeneration
import raw.code.DefaultEmitter 

import raw.code.executor.supersonic.SupersonicExecutor

import raw.code.storage.csv.CsvStorage
import raw.code.storage.root.RootStorage

import raw.expression.ConstFloat
import raw.expression.ConstInt32
import raw.expression.Plus
import raw.expression.Multiply
import raw.expression.Divide
import raw.expression.Abs
import raw.expression.And
import raw.expression.Or
import raw.expression.Equal
import raw.expression.NotEqual
import raw.expression.Less
import raw.expression.LessOrEqual
import raw.expression.Greater
import raw.expression.GreaterOrEqual
import raw.expression.FieldReference

import raw.operators.Load
import raw.operators.Scan
import raw.operators.ScanByKey
import raw.operators.Filter
import raw.operators.InnerJoin
import raw.operators.AntiJoin
import raw.operators.Aggregate
import raw.operators.UnionAll
import raw.operators.Count
import raw.operators.Project
import raw.operators.Store
import raw.operators.Reuse
import raw.operators.Benchmark
import raw.operators.PrintBenchmark
import raw.operators.Print

import raw.schema.Field
import raw.schema.Schema
import raw.schema.UINT32


object ReferenceExecutor extends App {
  println("Hi from ReferenceExecutor")
  
  val csv = new CsvStorage(new Schema(List(Field("RunNumber", UINT32, false), 
                                           Field("lbn", UINT32, false))))
  
  //val rootFiles = scala.io.Source.fromFile("/home/miguel/test/diassrv8.full").getLines().toList
  val rootFiles = List("/home/miguel/NTUP_TOPEL.00872780._000012.root.1")
  
  val root = new RootStorage()

  // FIXME: Rename Load to 'Using'?
  // FIXME: Fix usage of getReference() within the executor.
  // FIXME: Add Alias support after adding way to use column number (starting from 0)
  // FIXME: Add ability to "introspect" ROOT files and derive event/muon/... tables dynamically 
  // FIXME: ScanByKey should only apply to Storages with capability to read by key. Ensure that with OO hierarchy.
  // FIXME: Add pretty-print of logical and physical operator trees
  // FIXME: Define embedded DSL to write query plan (incl rich support for expressions & UDFs)
  // FIXME: Define various separate programs. One that generates C macros; one that implements an intermediary logical operator-ish DSL language, etc
  // FIXME: Add xdot-like pretty print.
  // FIXME: Add operators to store (mmap) and read (mmap again) to disk.
  // FIXME: Add positional map to 2nd run of CSV scan
  // FIXME: Add higher-level SQL/BigQuery-like language
  // FIXME: Turn this into client/server process (protobuf as protocol). Is Scala server-side?
  // FIXME: "Register" queries in a server process to amortize query compilation time.

  // FIXME: Benchmark code is broken because we can only benchmark on read. Do PrintAndBenchmark instead and use Supersonic Benchmark operators.
  // FIXME: Create rule to check that the DAG is valid (e.g. there is a Store for every Reuse). Could also be handled upstream...
  // FIXME: Validate query plan, e.g. check that there is a top-level Project otherwise the Supersonic code is invalid.
  // FIXME: Add the usual set of rules, e.g. filter pushdowns
  // FIXME: Subdivide logical operators into those that take no children, one child, two children, or more. This would facilitate writing navigation classes.

/** Optimizer is not the most appropriate name for the class.
 *  All it does is generate the physical query plan from the logical query plan.
 *  A LogicalOptimizer instead would perform typical transformations (e.g. filter pushdowns).
 *  There's however an interesting aspect to how the physical query plan is generated.
 *  The ScanAndProject physical operator only loads the fields that are actually used, but it loads them all "early",
 *  i.e. it pushs downs the loads. An alternative would be to introduce a new physical operator that loads columns as needed, performing late materialization. 
 */

  val query =
    List(
      Store(
        "GoodRuns",
        Load(
          csv,
          List("/home/miguel/good_runs.csv"),
          List(
            Project(
              Scan(TableCatalog.table("csv")),
              List("RunNumber", "lbn"))))),
      Print(
        Load(
          root,
          rootFiles,
          List(
            Store(
              "GoodEvents",
              Project(
                InnerJoin(
                  Filter(
                    Scan(TableCatalog.table("events")),
                    Or(
                      FieldReference("EF_e24vhi_medium1"),
                      Or(
                        FieldReference("EF_e60_medium1"),
                        Or(
                          FieldReference("EF_2e12Tvh_loose1"),
                          Or(
                            FieldReference("EF_mu24i_tight"),
                            Or(
                              FieldReference("EF_mu36_tight"),
                              FieldReference("EF_2mu13"))))))),
          	      Reuse("GoodRuns"),
          	      List("RunNumber", "lbn"),
          	      List("RunNumber", "lbn")),
          	    List("ID", "RunNumber", "lbn"))),
            Store(
              "GoodMuons",
              Aggregate(
                Filter(
                  ScanByKey(
                    TableCatalog.table("muons"),
                    Reuse("GoodEvents"),
                    List("ID")),
                  And(
                    Less(FieldReference("mu_ptcone20"), Multiply(ConstFloat(0.1), FieldReference("mu_pt"))),
                    And(
                      Greater(FieldReference("mu_pt"), ConstFloat(20000.0)),
                      And(
                        Less(Abs(FieldReference("mu_eta")), ConstFloat(2.4)),
                        And(
                          NotEqual(FieldReference("mu_isCombinedMuon"), ConstInt32(0)),
                          And(
                            NotEqual(FieldReference("mu_tight"), ConstInt32(0)),
                            And(
                              Or(
                                Equal(FieldReference("mu_expectBLayerHit"), ConstInt32(0)),
                                NotEqual(FieldReference("mu_nBLHits"), ConstInt32(0))),
                              And(
                                Greater(Plus(FieldReference("mu_nPixHits"), FieldReference("mu_nPixelDeadSensors")), ConstInt32(1)),
                                And(
                                  GreaterOrEqual(Plus(FieldReference("mu_nSCTHits"), FieldReference("mu_nSCTDeadSensors")), ConstInt32(6)),
                                  Less(Plus(FieldReference("mu_nPixHoles"), FieldReference("mu_nSCTHoles")), ConstInt32(3))))))))))),
                List("ID"),
                List(Count("mu_COUNT")))),
            Store(
              "GoodElectrons",
              Aggregate(
                Filter(
                  ScanByKey(
                    TableCatalog.table("electrons"),
                    Reuse("GoodEvents"),
                    List("ID")),
                  And(
                    Less(FieldReference("el_ptcone20"), Multiply(ConstFloat(0.1), FieldReference("el_pt"))),
                    And(
                      Greater(FieldReference("el_pt"), ConstFloat(20000.0)),
                      And(
                        Less(Abs(FieldReference("el_eta")), ConstFloat(2.5)),
                        And(
                          Or(
                            Equal(FieldReference("el_author"), ConstInt32(1)),
                            Equal(FieldReference("el_author"), ConstInt32(3))),
                         NotEqual(FieldReference("el_medium"), ConstInt32(0))))))),
                List("ID"),
                List(Count("el_COUNT")))),
            Store(
              "GoodJets",
              Project(
                Filter(
                  ScanByKey(
                    TableCatalog.table("jets"),
                    UnionAll(
                      List(
                        Filter(
                          AntiJoin(
                            Reuse("GoodMuons"),
                            Reuse("GoodElectrons"),
                            List("ID"),
                            List("ID")),
                          Equal(FieldReference("mu_COUNT"), ConstInt32(2))),
                        Filter(
                          AntiJoin(
                            Reuse("GoodElectrons"),
                            Reuse("GoodMuons"),
                            List("ID"),
                            List("ID")),
                          Equal(FieldReference("el_COUNT"), ConstInt32(2))))),
                      List("ID")),
                    And(
                      LessOrEqual(Abs(FieldReference("jet_eta")), ConstFloat(2.5)),
                      GreaterOrEqual(Divide(FieldReference("jet_pt"), ConstFloat(1000)), ConstFloat(25)))),
                  List("ID", "jet_E", "jet_pt", "jet_phi", "jet_eta", "jet_flavor_weight_JetFitterCOMBNN", "jet_flavor_weight_SV1", "jet_flavor_weight_IP3D"))),
            Reuse("GoodJets")))))
        
  CodeGeneration.generate(Optimizer.optimize(query),
                          new DefaultEmitter(),
                          new SupersonicExecutor())
  
  println("Done at " + new java.util.Date)
}
