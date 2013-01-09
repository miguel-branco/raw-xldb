package tutorial

import scala.util.parsing.combinator._
import scala.util.parsing.input.Positional
import java.io.FileReader

/**
 * Lazy CSV parser and declarations of data structures used throughout the project
 */

trait ColumnValue { def value: String }

trait RecordValue { def columns: List[ColumnValue] }

abstract trait Statement extends Positional
case class ColumnImpl(value: String) extends Statement with ColumnValue {
  override def toString = "tutorial.ColumnImpl(\"" + value + "\")"
}
case class RecordImpl(columns: List[ColumnImpl]) extends Statement with RecordValue {
  override def toString = "tutorial.RecordImpl(" + columns.toString + ")"
}

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
      positioned(repsep(field, COMMA) ^^ { case f => tutorial.RecordImpl(f) })

    def field: Parser[ColumnImpl] = escaped | nonescaped

    def escaped: Parser[ColumnImpl] = {
      ((SPACES?) ~> DQUOTE ~> ((TXT | COMMA | CRLF | DQUOTE2)*) <~ DQUOTE <~ (SPACES?)) ^^ {
        case ls => ColumnImpl(ls.mkString(""))
      }
    }

    def nonescaped: Parser[ColumnImpl] = (TXT*) ^^ { case ls => tutorial.ColumnImpl(ls.mkString("")) }

    def parse(s: String) = parseAll(file, s) match {
      case Success(res, _) => res
      case e => throw new Exception(e.toString)
    }

    def nextRecord(s: Iterator[String]): Stream[RecordImpl] = s.hasNext match {
      case false => Stream.empty
      case true => parse(record, s.next) match {
        case Success(res, _) => {
          //println("RECORD Read: " + res)
          res #:: nextRecord(s)
        }
        case e => throw new Exception(e.toString)
      }
    }
  }

  /*
  //Usage Example
  val filePath = "/home/manolee/nodb/data/tmp.csv"
  val reader = new FileReader(filePath)
  val source = scala.io.Source.fromFile(filePath)

  val lines = source.getLines

  var result = for {
    lineIter <- CSVlazy.nextRecord(lines)
    //x = lineIter
    if lineIter.columns(0).value.toInt > 5
  } yield lineIter
  result.toList
*/
