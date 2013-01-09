package tutorial

import scala.virtualization.lms.common._
import java.io.FileReader


/**
 * Trait in which we define our functions.
 * LMS seems to allow only functions with single input, as there is a 1-1 mapping between this input
 * and the generated class representing the function
 */
trait Prog { this: RawInterface with LiftNumeric with LiftString with ScalaOpsPkg with StreamOps with TraversableOps =>

  /**
   * Showcasing ability to handle Streams and Lists. For comprehensions deal with Streams and Lists in this case.
   * Also using lifted boolean function to filter result.
   * Projection of a single column made available by lifting an apply operator
   *
   * @param v dummy input value
   * @return comprehension result as list for showcasing. Would normally be lazily evaluated Stream
   */

  def comprehensionTest(v: Rep[ColumnValue]) = {

    val file1 = Stream(RecordValue(List(ColumnValue("1"), ColumnValue("Tom"), ColumnValue("18"), ColumnValue("1500"))),
      RecordValue(List(ColumnValue("2"), ColumnValue("Dick"), ColumnValue("23"), ColumnValue("300"))),
      RecordValue(List(ColumnValue("3"), ColumnValue("Harry"), ColumnValue("6"), ColumnValue("1"))),
      RecordValue(List(ColumnValue("4"), ColumnValue("John"), ColumnValue("6"), ColumnValue("300000"))))

    val file2 = Stream(RecordValue(List(ColumnValue("Low Tax"), ColumnValue("0"), ColumnValue("400"))),
      RecordValue(List(ColumnValue("High Tax"), ColumnValue("401"), ColumnValue("2000"))),
      RecordValue(List(ColumnValue("No Tax"), ColumnValue("2001"), ColumnValue("1000000"))))

    var result = for {
      person <- file1
      taxCategory <- file2

      if gt_int(person(3), taxCategory(1))
      if gt_int(taxCategory(2), person(3)) //Join test
      if gt_int(person(0), unit(2)) //Selection test
      if gt_int(unit(10), person(0))
    } yield (person(1), taxCategory(0))

    result.toList
  }

  /**
   * Test combining lifted comprehensions with our lazy parser.
   * LMS not too flexible, as it accepts single argument. We had to provide a list containing the file streams
   *
   * @param v a list containing the Streams resulting by parsing two files
   * @return result of a for-comprehension equivalent to a relational theta join
   */
  def ioTest1(v: Rep[List[Stream[RecordValue]]]) = {

    val fileStream1 = v(0)
    val fileStream2 = v(1)
    var result = for {
      person <- fileStream1
      taxCategory <- fileStream2
      if gt_int(person(3), taxCategory(1))
      if gt_int(taxCategory(2), person(3)) //Join test
      if gt_int(person(0), unit(2)) //Selection test
      if gt_int(unit(10), person(0))
    } yield (person(1), taxCategory(0))

    result
  }

  /**
   *
   * @param v dummy value in this case. Inputs to our comprehension defined inside the function
   * @return result of a for-comprehension equivalent to a relational theta join
   */
  def ioTest2(v: Rep[List[Stream[RecordValue]]]) = {

    val filePath1 = "input/employees.csv"
    val reader1 = new FileReader(filePath1)
    val source1 = scala.io.Source.fromFile(filePath1)
    val lines1 = source1.getLines

    val filePath2 = "input/taxes.csv"
    val reader2 = new FileReader(filePath2)
    val source2 = scala.io.Source.fromFile(filePath2)
    val lines2 = source2.getLines

    val fileStream1 = CSVlazy.nextRecord(lines1)
    val fileStream2 = CSVlazy.nextRecord(lines2)

    var result = for {
      person <- unit(fileStream1)
      taxCategory <- unit(fileStream2)
      if gt_int(person(3), taxCategory(1))
      if gt_int(taxCategory(2), person(3)) //Join test
      if gt_int(person(0), unit(2)) //Selection test
      if gt_int(unit(10), person(0))
    } yield (person(1), taxCategory(0))

    result
  }

  def rewriteTest(v: Rep[List[Stream[RecordValue]]]) = {

    //In this case, List of Streams considered a pre-existing constant by LMS.
    //If I provide it as input of the function, it is treated as a symbol to be evaluated
    val filePath1 = "input/employees.csv"
    val reader1 = new FileReader(filePath1)
    val source1 = scala.io.Source.fromFile(filePath1)
    val lines1 = source1.getLines

    val filePath2 = "input/taxes.csv"
    val reader2 = new FileReader(filePath2)
    val source2 = scala.io.Source.fromFile(filePath2)
    val lines2 = source2.getLines

    val fileStream1 = CSVlazy.nextRecord(lines1)
    val fileStream2 = CSVlazy.nextRecord(lines2)

    var result = for {
      person <- unit(fileStream1)
      if pos_int(person(3)) //Join test

    } yield person(1)

    result
  }


  //Used to test more complex rewrites
  def remapTest(v: Rep[Stream[RecordValue]]) = {

    val filePath1 = "input/employees.csv"
    val reader1 = new FileReader(filePath1)
    val source1 = scala.io.Source.fromFile(filePath1)
    val lines1 = source1.getLines

    val filePath2 = "input/taxes.csv"
    val reader2 = new FileReader(filePath2)
    val source2 = scala.io.Source.fromFile(filePath2)
    val lines2 = source2.getLines

    val fileStream1 = CSVlazy.nextRecord(lines1)
    val fileStream2 = CSVlazy.nextRecord(lines2)

    val result = for {
      person <- unit(fileStream1) //v
      taxes <- unit(fileStream2)
    } yield (person,taxes)

    result
  }
}
  /**
   * Code generation and execution
   */
object Usage extends App {

  val concreteProg = new Prog with EffectExp with RawExp with CompileScala with LiftNumeric with LiftString with ScalaOpsPkgExp with StreamOpsExp with TraversableOpsExp
		  					  with RawExpOpt with ColumnOpt { self =>
    override val codegen = new ScalaGenEffect with ScalaGenLinearAlgebra { val IR: self.type = self }

    //Displaying generated code - Good for informative purposes  - Can comment for clarity of output

    codegen.emitSource(comprehensionTest, "ComprehensionTest", new java.io.PrintWriter(System.out))
	
    codegen.emitSource(ioTest1, "InputTest1", new java.io.PrintWriter(System.out))

    codegen.emitSource(ioTest2, "InputTest2", new java.io.PrintWriter(System.out))

    codegen.emitSource(rewriteTest, "rewrites", new java.io.PrintWriter(System.out))


  }

 /*
 //Actual code execution
  val u = concreteProg.compile(concreteProg.comprehensionTest)
  println(u(ColumnImpl("Dummy Value")))*/

  //Opening two files, lazily parsing them and providing the resulting Streams to the function to be lifted
  val filePath1 = "input/employees.csv"
  val reader1 = new FileReader(filePath1)
  val source1 = scala.io.Source.fromFile(filePath1)
  val lines1 = source1.getLines

  val filePath2 = "input/taxes.csv"
  val reader2 = new FileReader(filePath2)
  val source2 = scala.io.Source.fromFile(filePath2)
  val lines2 = source2.getLines

  val fileStreams = List(CSVlazy.nextRecord(lines1), CSVlazy.nextRecord(lines2))
  val v = concreteProg.compile(concreteProg.ioTest1)
  println(v(fileStreams))

  val w = concreteProg.compile(concreteProg.ioTest2)
  println(w(fileStreams))

  val x = concreteProg.compile(concreteProg.rewriteTest)
  println(x(fileStreams))

//  val y = concreteProg.compile(concreteProg.remapTest)
//  println(y(fileStreams(0)))
}
