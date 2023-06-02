package io.univalence.unidb.job

import io.univalence.unidb.arg.ApplicationOption

import zio.*

case class HelpJob() extends Job[Any, ApplicationOption.HelpOption] {

  val helpDisplay: String =
    s"""UniDB <mode> [options...]
       |Usage:
       |  Mode
       |    cli                   Command line interface (interactive) mode
       |    server                Server mode
       |    web                   Web mode
       |    load                  Load data from file
       |    dump                  Dump table in output
       |    help                  Display help
       |    
       |  Common
       |  --store-dir <FILE>      Change default storage directory
       |  
       |  Server Mode
       |  --port <INT>            Change listening port in server mode
       |  
       |  Web Mode
       |  --port <INT>            Change listening port in server mode
       |  
       |  Load Mode
       |  --to <STORE>            Store name to load data to
       |  --from <FILE>           CSV file to load
       |  --keys <STRING>         Indicate which field to use as key (mandatory)
       |  --key-delim <CHAR>      Character to use as key delimiter (default: #)
       |  
       |  Dump Mode
       |  --from <STORE>          Store name to load dump
       |""".stripMargin

  override def run(option: ApplicationOption.HelpOption): RIO[Any, Unit] = Console.printLine(helpDisplay).orDie
}
