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

import raw.code.CodeGeneration
import raw.code.DefaultEmitter 

import raw.code.executor.supersonic.SupersonicExecutor

import raw.code.storage.MockStorage
import raw.code.storage.csv.CsvStorage
import raw.code.storage.root.RootStorage

import raw.expression.ConstFloat
import raw.expression.ConstInt32
import raw.expression.Plus
import raw.expression.Minus
import raw.expression.Multiply
import raw.expression.Divide
import raw.expression.Abs
import raw.expression.Cos
import raw.expression.Sin
import raw.expression.Sinh
import raw.expression.Sqrt
import raw.expression.Negate
import raw.expression.And
import raw.expression.Or
import raw.expression.Equal
import raw.expression.NotEqual
import raw.expression.Less
import raw.expression.LessOrEqual
import raw.expression.Greater
import raw.expression.GreaterOrEqual
import raw.expression.FieldReference
import raw.expression.If

import raw.operators.Load
import raw.operators.Scan
import raw.operators.ScanByKey
import raw.operators.Filter
import raw.operators.InnerJoin
import raw.operators.AntiJoin
import raw.operators.Aggregate
import raw.operators.UnionAll
import raw.operators.Project
import raw.operators.Compute
import raw.operators.With
import raw.operators.Reuse
import raw.operators.Benchmark
import raw.operators.PrintBenchmark
import raw.operators.Print
import raw.operators.Histogram
import raw.operators.HistogramFill
import raw.operators.UDFFilter
import raw.operators.QueueInit
import raw.operators.QueuePush
import raw.operators.QueuePop

import raw.operators.Count
import raw.operators.Sum
import raw.operators.First
import raw.operators.Last

import raw.schema.Field
import raw.schema.Schema
import raw.schema.UINT32


object ReferenceExecutor extends App {
  println("Hi from ReferenceExecutor")
  
  val csvStorage = new CsvStorage(new Schema(List(Field("RunNumber", UINT32, false), 
                                                  Field("lbn", UINT32, false))))
  
  val rootFiles = scala.io.Source.fromFile("/home/miguel/test/diassrv8.full").getLines().toList
  //val rootFiles = List("/home/miguel/NTUP_TOPEL.00872780._000012.root.1")
  /*
  val rootFiles = List(
    "/data1/mbranco/ATLAS/data12_8TeV.00203779.physics_Muons.merge.NTUP_TOPMU.f446_m1153_p1104_p1105_tid00872879_00/NTUP_TOPMU.00872879._000015.root.1",
    "/data1/mbranco/ATLAS/data12_8TeV.00205017.physics_Egamma.merge.NTUP_TOPEL.f449_m1163_p1104_p1105_tid00872817_00/NTUP_TOPEL.00872817._000003.root.1",
    "/data1/mbranco/ATLAS/data12_8TeV.00203779.physics_Egamma.merge.NTUP_TOPEL.f446_m1153_p1104_p1105_tid00872780_00/NTUP_TOPEL.00872780._000005.root.1",
    "/data1/mbranco/ATLAS/data12_8TeV.00205017.physics_Egamma.merge.NTUP_TOPEL.f449_m1163_p1104_p1105_tid00872817_00/NTUP_TOPEL.00872817._000002.root.1",
    "/data1/mbranco/ATLAS/data12_8TeV.00203779.physics_Egamma.merge.NTUP_TOPEL.f446_m1153_p1104_p1105_tid00872780_00/NTUP_TOPEL.00872780._000002.root.1",
    "/data1/mbranco/ATLAS/data12_8TeV.00204976.physics_Egamma.merge.NTUP_TOPEL.f449_m1163_p1104_p1105_tid00872813_00/NTUP_TOPEL.00872813._000006.root.1"
  )  
  */
  val rootStorage = new RootStorage()
  val mockStorage = new MockStorage()
  
  // FIXME: generate code as a library that takes file name as input. Then generate code for 2 queries: on-disk and in-memory.
  // FIXME: emitStore and emitRelease should also take identifier as second argument.
  // FIXME: Instead of newReference() as a method in the executor, add that behavior to raw.code.Instructions
  // FIXME: Instead of Compute requiring every field to be projected manually, we could have Optimizer.getPhysicalPlan add those fields as aliased expressions
  //        automatically.
  // FIXME: Merge Project and Compute operators into single one?
  // FIXME: BUG: Since Load always returns "last" reference, there's a seg fault at the end if there's no wrapper operator reusing reference.
  // FIXME: BUG: Store passes reference of child to its parent but releases the child scoped_ptr in the Supersonic executor.
  //             Therefore, it crashes unless it is used as the top-most operator.
  // FIXME: Does it make sense for Load to return last element from DAG or should it return all of them?
  // FIXME: Project should request only the fields in the project and nothing else. How does that cope with IDs being used in-between?
  // FIXME: Add operator to register table schemas in the catalog. Schema is stored in a built-in file format, which is hardcoded to be table name.
  //        All other tables/files/plugins are then loaded dynamically. Therefore, the system must only know the location of a single (metadata) table.
  //        The metadata should support the notion of hierarchies of tables, which would also be used by the SQL parser layer above.
  // FIXME: Propagate back query execution errors to this layer: e.g. if a user queries a field that does not exist (or that exists), that info should
  //        be registered in the metadata catalog, which is then consulted for subsequent queries. 
  // FIXME: Rename Load to 'Using'?
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
  // FIXME: The notion that a "child" node gives back sorted data is not present in the OO-hierarchy and, hence, not enforced for operators that need sorted input. 
  //        Create a trait for specifying it.
  // FIXME: For KeyAccessPath, the Key should be part of the constructor and not at open() time.
  // FIXME: For access paths, rename "ref" of the constructor to a more meaningful name.

/** Optimizer is not the most appropriate name for the class.
 *  All it does is generate the physical query plan from the logical query plan.
 *  A LogicalOptimizer instead would perform typical transformations (e.g. filter pushdowns).
 *  There's however an interesting aspect to how the physical query plan is generated.
 *  The PhysicalScan physical operator only loads the fields that are actually used, but it loads them all "early",
 *  i.e. it pushs downs the loads. An alternative would be to introduce a new physical operator that loads columns as needed, performing late materialization. 
 */

  /*
  SELECT event
  FROM root:...
  WHERE
  (event.EF_e24vhi_medium1 OR event.EF_e60_medium1 OR event.EF_2e12Tvh_loose1 OR event.EF_mu24i_tight OR event.EF_mu36_tight OR event.EF_2mu13) AND
  event.muon.mu_ptcone20 < 0.1 * event.muon.mu_pt AND
  event.muon.mu_pt > 20000. AND
  ABS(event.muon.mu_eta) < 2.4 AND 
  event.muon.mu_isCombinedMuon <> 0 AND
  event.muon.mu_tight <> 0 AND
  (event.muon.mu_expectBLayerHit == 0 OR event.muon.mu_nBLHits <> 0) AND
  event.muon.mu_nPixHits + event.muon.mu_nPixelDeadSensors > 1
  (event.muon.mu_nSCTHits + event.muon.mu_nSCTDeadSensors >= 6) AND
  (event.muon.mu_nPixHoles + event.muon.mu_nSCTHoles) < 3 AND
  event.muon.el_ptcone20 < 0.1 * el_pt AND
  event.electron.el_pt > 20000. AND
  ABS(event.electron.el_eta) < 2.5 AND
  (event.electron.el_author == 1 OR event.electron.el_author == 3) AND 
  event.electron.el_medium <> 0
  ABS(event.jet.jet_eta) <= 2.5 AND
  event.jet.jet_pt / 1000. >= 25 AND
  UDF_IS_TRUE(passBtaggingWeightSelection(event.jet)) AND
  ((COUNT(event.muon) == 2 AND COUNT(event.electron) == 0) OR (COUNT(event.muon) == 0 AND COUNT(event.electron) == 2)) AND
  COUNT(event.jet) == 2 AND  
  */
  
  val queries = List(
    QueueInit("queue_csv", QueueInit("queue_events", QueueInit("queue_muons", QueueInit("queue_electrons", QueueInit("queue_jets", Histogram("histo1",
    With(
      "GoodRuns",
      Load(
        csvStorage,
        List("good_runs.csv"),
        Project(
          QueuePush("queue_csv", Scan("csv")),
          List("RunNumber", "lbn"))),
      Load(
        rootStorage,
        rootFiles,
        With(
          "GoodEvents",
          Project(
            InnerJoin(
              Project(
                Filter(
                  Compute( 
                    Filter(
                      QueuePush(
                        "queue_events",
                        Scan("events")),
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
                    List(
                      ("EventID", FieldReference("EventID")),
                      ("RunNumber", FieldReference("RunNumber")),
                      ("lbn", FieldReference("lbn")),
                      ("met", Sqrt(Plus(Multiply(FieldReference("MET_RefFinal_etx"),FieldReference("MET_RefFinal_etx")),Multiply(FieldReference("MET_RefFinal_ety"),FieldReference("MET_RefFinal_ety"))))))),
                  Less(FieldReference("met"), ConstFloat(50000))),
                List("EventID", "RunNumber", "lbn")),
      	      Reuse("GoodRuns"),
      	      List("RunNumber", "lbn"),
      	      List("RunNumber", "lbn")),
      	    List("EventID", "RunNumber", "lbn")),
          With(
            "AllMuons",
            Project(
              Filter(
                QueuePush(
                  "queue_muons",
                  ScanByKey(
                    "muons",
                    Reuse("GoodEvents"),
                    List("EventID"))),
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
              List("MuonID", "EventID")),
            With(
              "GoodMuons",
              Aggregate(
                Reuse("AllMuons"),
                List("EventID"),
                List(Count("mu_COUNT"))),
              With(
                "AllElectrons",
                Project(                
                  Filter(
                    QueuePush(
                      "queue_electrons",
                      ScanByKey(
                        "electrons",
                        Reuse("GoodEvents"),
                        List("EventID"))),
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
                  List("ElectronID", "EventID")),
                With(
                  "GoodElectrons",
                  Aggregate(
                      Reuse("AllElectrons"),
                      List("EventID"),
                    List(Count("el_COUNT"))),
                  With(
                    "AllJets",
                    Project(
                      Project(
                        UDFFilter(
                          Filter(
                            QueuePush(
                              "queue_jets",
                              ScanByKey(
                                "jets",
                                UnionAll(
                                  List(
                                    Filter(
                                      AntiJoin(
                                        Reuse("GoodMuons"),
                                        Reuse("GoodElectrons"),
                                        List("EventID"),
                                        List("EventID")),
                                      Equal(FieldReference("mu_COUNT"), ConstInt32(2))),
                                    Filter(
                                      AntiJoin(
                                        Reuse("GoodElectrons"),
                                        Reuse("GoodMuons"),
                                        List("EventID"),
                                        List("EventID")),
                                      Equal(FieldReference("el_COUNT"), ConstInt32(2))))),
                                  List("EventID"))),
                            And(
                              LessOrEqual(Abs(FieldReference("jet_eta")), ConstFloat(2.5)),
                              GreaterOrEqual(Divide(FieldReference("jet_pt"), ConstFloat(1000)), ConstFloat(25))))),
                          List("jet_flavor_weight_JetFitterCOMBNN", "jet_flavor_weight_SV1", "jet_flavor_weight_IP3D")),
                      List("EventID", "jet_pt", "jet_phi", "jet_eta", "jet_E")),
                    With(
                      "GoodJets",
                        Project(
                        Filter(
                          Aggregate(            
                            Compute(
                              Reuse("AllJets"),
                              List(
                                ("EventID", FieldReference("EventID")),
                                ("jetpt_OVER_45", If(Greater(Divide(FieldReference("jet_pt"), ConstFloat(1000)), ConstFloat(45)), ConstInt32(1), ConstInt32(0))))),
                            List("EventID"),
                            List(
                              // FIXME: Maybe this is too early to aggregate? Haven't yet filtered jet count == 2? Separate into separate function
                              Count("jet_COUNT"),
                              Sum("jetpt_OVER_45", "jetpt_OVER_45"))),
                          And(
                            Equal(FieldReference("jet_COUNT"), ConstInt32(2)),
                            Greater(FieldReference("jetpt_OVER_45"), ConstInt32(0)))),
                        List("EventID")),
                      //
                      HistogramFill(
                        "histo1",
                        Project(
                          Compute(
                            Filter(
                              Compute(
                                Compute(
                                  Compute(
                                    Aggregate(
                                      Compute(
                                        InnerJoin(
                                          Reuse("AllJets"),
                                          Reuse("GoodJets"),
                                          List("EventID"),
                                          List("EventID")),
                                        List(
                                          ("EventID", FieldReference("EventID")),
                                          ("jet_E", Divide(FieldReference("jet_E"), ConstFloat(1000))),
                                          ("jet_pt", Divide(FieldReference("jet_pt"), ConstFloat(1000))),
                                          ("jet_phi", FieldReference("jet_phi")),
                                          ("jet_eta", FieldReference("jet_eta")))),
                                      List("EventID"),
                                      List(
                                        First("jet_E1", "jet_E"),
                                        First("jet_pt1", "jet_pt"),
                                        First("jet_phi1", "jet_phi"),
                                        First("jet_eta1", "jet_eta"),
                                        Last("jet_E2", "jet_E"),        
                                        Last("jet_pt2", "jet_pt"),
                                        Last("jet_phi2", "jet_phi"),
                                        Last("jet_eta2", "jet_eta"))),
                                    List(
                                      ("jet_E1", FieldReference("jet_E1")),
                                      ("jet_px1", Multiply(FieldReference("jet_pt1"), Cos(FieldReference("jet_phi1")))),
                                      ("jet_py1", Multiply(FieldReference("jet_pt1"), Sin(FieldReference("jet_phi1")))),
                                      ("jet_pz1", Multiply(FieldReference("jet_pt1"), Sinh(FieldReference("jet_eta1")))),
                                      ("jet_E2", FieldReference("jet_E2")),
                                      ("jet_px2", Multiply(FieldReference("jet_pt2"), Cos(FieldReference("jet_phi2")))),
                                      ("jet_py2", Multiply(FieldReference("jet_pt2"), Sin(FieldReference("jet_phi2")))),
                                      ("jet_pz2", Multiply(FieldReference("jet_pt2"), Sinh(FieldReference("jet_eta2")))),
                                      ("deltaEta", Abs(Minus(FieldReference("jet_eta1"), FieldReference("jet_eta2")))),
                                      ("deltaPhi", Abs(Minus(FieldReference("jet_phi1"),FieldReference("jet_phi2")))))),
                                  List(
                                    ("jet_E1", FieldReference("jet_E1")),
                                    ("jet_px1", FieldReference("jet_px1")),
                                    ("jet_py1", FieldReference("jet_py1")),
                                    ("jet_pz1", FieldReference("jet_pz1")),
                                    ("jet_E2", FieldReference("jet_E2")),
                                    ("jet_px2", FieldReference("jet_px2")),
                                    ("jet_py2", FieldReference("jet_py2")),
                                    ("jet_pz2", FieldReference("jet_pz2")),
                                    ("deltaEta", FieldReference("deltaEta")),
                                    ("deltaPhi", If(Greater(FieldReference("deltaPhi"),ConstFloat(math.Pi)),Minus(Multiply(ConstFloat(2),ConstFloat(math.Pi)),FieldReference("deltaPhi")),FieldReference("deltaPhi"))))),
                                List(
                                  ("jet_E1", FieldReference("jet_E1")),
                                  ("jet_px1", FieldReference("jet_px1")),
                                  ("jet_py1", FieldReference("jet_py1")),
                                  ("jet_pz1", FieldReference("jet_pz1")),
                                  ("jet_E2", FieldReference("jet_E2")),
                                  ("jet_px2", FieldReference("jet_px2")),
                                  ("jet_py2", FieldReference("jet_py2")),
                                  ("jet_pz2", FieldReference("jet_pz2")),
                                  ("pt_H", Sqrt(Plus(Multiply(Plus(FieldReference("jet_px1"),FieldReference("jet_py2")),Plus(FieldReference("jet_px1"),FieldReference("jet_py2"))),Multiply(Plus(FieldReference("jet_px1"),FieldReference("jet_py2")),Plus(FieldReference("jet_px1"),FieldReference("jet_py2")))))),
                                  ("deltaRbb", Sqrt(Plus(Multiply(FieldReference("deltaEta"),FieldReference("deltaEta")),Multiply(FieldReference("deltaPhi"),FieldReference("deltaPhi"))))))),
                              Or(
                                Greater(FieldReference("pt_H"),ConstFloat(200)),
                                Greater(FieldReference("deltaRbb"),ConstFloat(0.7)))),
                            List(
                              ("mbb", Sqrt(
                                        Minus(
                                          Multiply(Plus(FieldReference("jet_E1"),FieldReference("jet_E2")),Plus(FieldReference("jet_E1"),FieldReference("jet_E2"))),
                                            Plus(
                                              Plus(
                                                Multiply(Plus(FieldReference("jet_px1"),FieldReference("jet_px2")),Plus(FieldReference("jet_px1"),FieldReference("jet_px2"))),
                                                Multiply(Plus(FieldReference("jet_py1"),FieldReference("jet_py2")),Plus(FieldReference("jet_py1"),FieldReference("jet_py2")))),
                                              Multiply(Plus(FieldReference("jet_pz1"),FieldReference("jet_pz2")),Plus(FieldReference("jet_pz1"),FieldReference("jet_pz2"))))))))),
                          List("mbb")))))))))))))))))),
    Histogram("histo2", With(
      "GoodRuns2",
      Project(
        QueuePop("queue_csv"),
        List("RunNumber", "lbn")),
      Load(
        mockStorage,
        rootFiles,
        With(
          "GoodEvents2",
          Project(
            InnerJoin(
              Project(
                Filter(
                  Compute( 
                    Filter(
                      QueuePop("queue_events"),
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
                    List(
                      ("EventID", FieldReference("EventID")),
                      ("RunNumber", FieldReference("RunNumber")),
                      ("lbn", FieldReference("lbn")),
                      ("met", Sqrt(Plus(Multiply(FieldReference("MET_RefFinal_etx"),FieldReference("MET_RefFinal_etx")),Multiply(FieldReference("MET_RefFinal_ety"),FieldReference("MET_RefFinal_ety"))))))),
                  Less(FieldReference("met"), ConstFloat(50000))),
                List("EventID", "RunNumber", "lbn")),
              Reuse("GoodRuns2"),
              List("RunNumber", "lbn"),
              List("RunNumber", "lbn")),
            List("EventID", "RunNumber", "lbn")),
          With(
            "AllMuons2",
            Project(
              Filter(
                QueuePop("queue_muons"),
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
              List("MuonID", "EventID")),
            With(
              "GoodMuons2",
              Aggregate(
                Reuse("AllMuons2"),
                List("EventID"),
                List(Count("mu_COUNT"))),
              With(
                "AllElectrons2",
                Project(                
                  Filter(
                    QueuePop("queue_electrons"),
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
                  List("ElectronID", "EventID")),
                With(
                  "GoodElectrons2",
                  Aggregate(
                      Reuse("AllElectrons2"),
                      List("EventID"),
                    List(Count("el_COUNT"))),
                  With(
                    "AllJets2",
                    Project(
                      Project(
                        UDFFilter(
                          Filter(
                            QueuePop("queue_jets"),
                            And(
                              LessOrEqual(Abs(FieldReference("jet_eta")), ConstFloat(2.5)),
                              GreaterOrEqual(Divide(FieldReference("jet_pt"), ConstFloat(1000)), ConstFloat(25))))),
                          List("jet_flavor_weight_JetFitterCOMBNN", "jet_flavor_weight_SV1", "jet_flavor_weight_IP3D")),                              
                      List("EventID", "jet_pt", "jet_phi", "jet_eta", "jet_E")),
                    With(
                      "GoodJets2",
                        Project(
                        Filter(
                          Aggregate(            
                            Compute(
                              Reuse("AllJets2"),
                              List(
                                ("EventID", FieldReference("EventID")),
                                ("jetpt_OVER_45", If(Greater(Divide(FieldReference("jet_pt"), ConstFloat(1000)), ConstFloat(45)), ConstInt32(1), ConstInt32(0))))),
                            List("EventID"),
                            List(
                              // FIXME: Maybe this is too early to aggregate? Haven't yet filtered jet count == 2? Separate into separate function
                              Count("jet_COUNT"),
                              Sum("jetpt_OVER_45", "jetpt_OVER_45"))),
                          And(
                            Equal(FieldReference("jet_COUNT"), ConstInt32(2)),
                            Greater(FieldReference("jetpt_OVER_45"), ConstInt32(0)))),
                        List("EventID")),
                      //
                      HistogramFill(
                        "histo2",
                        Project(
                          Compute(
                            Filter(
                              Compute(
                                Compute(
                                  Compute(
                                    Aggregate(
                                      Compute(
                                        InnerJoin(
                                          Reuse("AllJets2"),
                                          Reuse("GoodJets2"),
                                          List("EventID"),
                                          List("EventID")),
                                        List(
                                          ("EventID", FieldReference("EventID")),
                                          ("jet_E", Divide(FieldReference("jet_E"), ConstFloat(1000))),
                                          ("jet_pt", Divide(FieldReference("jet_pt"), ConstFloat(1000))),
                                          ("jet_phi", FieldReference("jet_phi")),
                                          ("jet_eta", FieldReference("jet_eta")))),
                                      List("EventID"),
                                      List(
                                        First("jet_E1", "jet_E"),
                                        First("jet_pt1", "jet_pt"),
                                        First("jet_phi1", "jet_phi"),
                                        First("jet_eta1", "jet_eta"),
                                        Last("jet_E2", "jet_E"),        
                                        Last("jet_pt2", "jet_pt"),
                                        Last("jet_phi2", "jet_phi"),
                                        Last("jet_eta2", "jet_eta"))),
                                    List(
                                      ("jet_E1", FieldReference("jet_E1")),
                                      ("jet_px1", Multiply(FieldReference("jet_pt1"), Cos(FieldReference("jet_phi1")))),
                                      ("jet_py1", Multiply(FieldReference("jet_pt1"), Sin(FieldReference("jet_phi1")))),
                                      ("jet_pz1", Multiply(FieldReference("jet_pt1"), Sinh(FieldReference("jet_eta1")))),
                                      ("jet_E2", FieldReference("jet_E2")),
                                      ("jet_px2", Multiply(FieldReference("jet_pt2"), Cos(FieldReference("jet_phi2")))),
                                      ("jet_py2", Multiply(FieldReference("jet_pt2"), Sin(FieldReference("jet_phi2")))),
                                      ("jet_pz2", Multiply(FieldReference("jet_pt2"), Sinh(FieldReference("jet_eta2")))),
                                      ("deltaEta", Abs(Minus(FieldReference("jet_eta1"), FieldReference("jet_eta2")))),
                                      ("deltaPhi", Abs(Minus(FieldReference("jet_phi1"),FieldReference("jet_phi2")))))),
                                  List(
                                    ("jet_E1", FieldReference("jet_E1")),
                                    ("jet_px1", FieldReference("jet_px1")),
                                    ("jet_py1", FieldReference("jet_py1")),
                                    ("jet_pz1", FieldReference("jet_pz1")),
                                    ("jet_E2", FieldReference("jet_E2")),
                                    ("jet_px2", FieldReference("jet_px2")),
                                    ("jet_py2", FieldReference("jet_py2")),
                                    ("jet_pz2", FieldReference("jet_pz2")),
                                    ("deltaEta", FieldReference("deltaEta")),
                                    ("deltaPhi", If(Greater(FieldReference("deltaPhi"),ConstFloat(math.Pi)),Minus(Multiply(ConstFloat(2),ConstFloat(math.Pi)),FieldReference("deltaPhi")),FieldReference("deltaPhi"))))),
                                List(
                                  ("jet_E1", FieldReference("jet_E1")),
                                  ("jet_px1", FieldReference("jet_px1")),
                                  ("jet_py1", FieldReference("jet_py1")),
                                  ("jet_pz1", FieldReference("jet_pz1")),
                                  ("jet_E2", FieldReference("jet_E2")),
                                  ("jet_px2", FieldReference("jet_px2")),
                                  ("jet_py2", FieldReference("jet_py2")),
                                  ("jet_pz2", FieldReference("jet_pz2")),
                                  ("pt_H", Sqrt(Plus(Multiply(Plus(FieldReference("jet_px1"),FieldReference("jet_py2")),Plus(FieldReference("jet_px1"),FieldReference("jet_py2"))),Multiply(Plus(FieldReference("jet_px1"),FieldReference("jet_py2")),Plus(FieldReference("jet_px1"),FieldReference("jet_py2")))))),
                                  ("deltaRbb", Sqrt(Plus(Multiply(FieldReference("deltaEta"),FieldReference("deltaEta")),Multiply(FieldReference("deltaPhi"),FieldReference("deltaPhi"))))))),
                              Or(
                                Greater(FieldReference("pt_H"),ConstFloat(200)),
                                Greater(FieldReference("deltaRbb"),ConstFloat(0.7)))),
                            List(
                              ("mbb", Sqrt(
                                        Minus(
                                          Multiply(Plus(FieldReference("jet_E1"),FieldReference("jet_E2")),Plus(FieldReference("jet_E1"),FieldReference("jet_E2"))),
                                            Plus(
                                              Plus(
                                                Multiply(Plus(FieldReference("jet_px1"),FieldReference("jet_px2")),Plus(FieldReference("jet_px1"),FieldReference("jet_px2"))),
                                                Multiply(Plus(FieldReference("jet_py1"),FieldReference("jet_py2")),Plus(FieldReference("jet_py1"),FieldReference("jet_py2")))),
                                              Multiply(Plus(FieldReference("jet_pz1"),FieldReference("jet_pz2")),Plus(FieldReference("jet_pz1"),FieldReference("jet_pz2"))))))))),
                          List("mbb"))))))))))))))
                          
  val physQueries = for (query <- queries) yield Optimizer.optimize(query)
  
  CodeGeneration.generate(physQueries,
                          new DefaultEmitter(),
                          new SupersonicExecutor())
  
  println("Done at " + new java.util.Date)
}
