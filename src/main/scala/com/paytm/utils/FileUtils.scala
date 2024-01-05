package com.paytm.utils

import java.io.{File, PrintWriter}

object FileUtils {
  def write_to_file(file_path: String, data: String): Unit = {
    val file = new File(file_path)
    val writer = new PrintWriter(file)
    writer.write(data)
    writer.close()
  }

  def delete_countries_file(countries_file_path: String): Boolean = {
    new File(countries_file_path).delete()
  }

}
