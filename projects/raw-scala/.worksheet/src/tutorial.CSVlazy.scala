package tutorial

import scala.util.parsing.combinator._
import scala.util.parsing.input.Positional
import java.io.FileReader

object CSVlazy {

  object CSVlazy extends RegexParsers {
    override val skipWhitespace = false // meaningful spaces in CSV

    def COMMA = ","
    def DQUOTE = "\""
    def DQUOTE2 = "\"\"" ^^ { case _ => "\"" } // combine 2 dquotes into 1
    def CRLF = "\r\n" | "\n"
    def TXT = "[^\",\r\n]".r
    def SPACES = "[ \t]+".r

    def file: Parser[List[RecordImpl]] = repsep(record, CRLF) <~ (CRLF?)

    def record: Parser[RecordImpl] =
      positioned(repsep(field, COMMA) ^^ { case f => RecordImpl(f) })

    def field: Parser[ColumnImpl] = escaped | nonescaped

    def escaped: Parser[ColumnImpl] = {
      ((SPACES?) ~> DQUOTE ~> ((TXT | COMMA | CRLF | DQUOTE2)*) <~ DQUOTE <~ (SPACES?)) ^^ {
        case ls => ColumnImpl(ls.mkString(""))
      }
    }

    def nonescaped: Parser[ColumnImpl] = (TXT*) ^^ { case ls => ColumnImpl(ls.mkString("")) }

    def parse(s: String) = parseAll(file, s) match {
      case Success(res, _) => res
      case e => throw new Exception(e.toString)
    }

    def nextRecordImpl(s: Iterator[String]): Stream[RecordImpl] = s.hasNext match {
      case false => Stream.empty
      case true => parse(record, s.next) match {
        case Success(res, _) => {
          //println("RECORD Read: " + res)
          res #:: nextRecordImpl(s)
        }
        case e => throw new Exception(e.toString)
      }
    }
  }

  trait ColumnValue { def value: String }

  trait RecordValue { def columns: List[ColumnValue] }
  
  abstract trait Statement extends Positional
  case class ColumnImpl(value: String) extends Statement with ColumnValue
  case class RecordImpl(columns: List[ColumnImpl]) extends Statement with RecordValue;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(1846); 

  val filePath = "/home/manolee/nodb/data/tmp.csv";System.out.println("""filePath  : java.lang.String = """ + $show(filePath ));$skip(40); 
  val reader = new FileReader(filePath);System.out.println("""reader  : java.io.FileReader = """ + $show(reader ));$skip(50); 
  val source = scala.io.Source.fromFile(filePath);System.out.println("""source  : scala.io.BufferedSource = """ + $show(source ));$skip(31); 

  val lines = source.getLines;System.out.println("""lines  : Iterator[String] = """ + $show(lines ));$skip(84); 

  //source.close()
  //var csv = CSVlazy.nextRecordImpl(lines)

  println("break");$skip(163); 
  //csv.toList
  var result = for {
    lineIter <- CSVlazy.nextRecordImpl(lines)
    //x = lineIter
    if lineIter.columns(0).value.toInt > 5
  } yield lineIter;System.out.println("""result  : scala.collection.immutable.Stream[tutorial.CSVlazy.RecordImpl] = """ + $show(result ));$skip(16); val res$0 = 
  result.toList;System.out.println("""res0: List[tutorial.CSVlazy.RecordImpl] = """ + $show(res$0));$skip(187); val res$1 = 
  //CSVlazy.nextRecordImpl(lines) filter (x => x.columns(0).value > "8") toList

  Stream(RecordImpl(List(ColumnImpl("11"), ColumnImpl("10"), ColumnImpl("1342177281"), ColumnImpl("1"))));System.out.println("""res1: scala.collection.immutable.Stream[tutorial.CSVlazy.RecordImpl] = """ + $show(res$1))}

}