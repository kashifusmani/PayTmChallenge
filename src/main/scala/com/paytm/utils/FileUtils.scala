package com.paytm.utils

import java.io.{File, PrintWriter}

object FileUtils {
  def writeToFile(filePath: String, data: String): Unit = {
    val file = new File(filePath)
    val writer = new PrintWriter(file)
    writer.write(data)
    writer.close()
  }

  def deleteCountriesFile(countriesFilePath: String): Boolean = {
    new File(countriesFilePath).delete()
  }

}
