import akka.actor.{ Actor, ActorRef, Props, ActorSystem }

import scala.concurrent.{Await, Future}


case class ProcessStringMsg(string: String)
case class StringProcessedMsg(words: Integer)

class StringCounterActor extends Actor {
  def receive = {
    case ProcessStringMsg(string) => {
      val wordsInLine = string.split(" ").length
      println("wordsInLine :" + wordsInLine)
      sender ! StringProcessedMsg(wordsInLine)
    }
    case _ => println("Error: message not recognized")
  }
}

case class StartProcessFileMsg()

class WordCounterActor(filename: String) extends Actor{

  private var running = false
  private var totalLines = 0
  private var linesProcessed = 0
  private var result = 0
  private var fileSender: Option[ActorRef] = None

  def receive = {
    case StartProcessFileMsg() => {
      if (running) {
        // println just used for example purposes;
        // Akka logger should be used instead
        println("Warning: duplicate start message received")
      } else {
        running = true
        fileSender = Some(sender) // save reference to process invoker
        import scala.io.Source._
        fromFile(filename).getLines.foreach { line =>
          val childActor = context.actorOf(Props[StringCounterActor], s"Akka$totalLines")
          println("Child Actor " + childActor)
          childActor ! ProcessStringMsg(line)
          totalLines += 1
        }
      }
    }
    case StringProcessedMsg(words) => {

      result += words
      linesProcessed += 1

      println("totalLIne :" + totalLines + "linesProcessed :" + linesProcessed)

      if (linesProcessed == totalLines) {
        println("Call Process Invoker :" + fileSender + "result " + result)
        fileSender.map(_ ! result) // provide result to process invoker
      }
    }
    case _ => println("message not recognized!")
  }
}

object Sample extends App {

  import akka.util.Timeout
  import scala.concurrent.duration._
  import akka.pattern.ask
  import akka.dispatch.ExecutionContexts._
  implicit val ec = global

  override def main(args: Array[String]) {
    val system = ActorSystem("MyAkkaSystem")
    val actor = system.actorOf(Props(new WordCounterActor("/Users/selvan/MEGA/projects/LearnAkka/src/main/resource/baby_names.csv")),"Akka0")
    implicit val timeout = Timeout(25 seconds)
    val future: Future[Any] = actor ? StartProcessFileMsg()
    val result = Await.result(future,timeout.duration)
    println("Toal no of words : " + result)
    system.shutdown
  }
}