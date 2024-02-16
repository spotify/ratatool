package com.spotify.ratatool

object BigQueryUtil {
  // a null TableFieldSchema mode can be treated as "NULLABLE", which is the
  // default value according to the docs, so return "NULLABLE" if fieldMode is null
  // otherwise return fieldMode
  def getFieldModeWithDefault(fieldMode: String): String =
    fieldMode match {
      case null => "NULLABLE"
      case _    => fieldMode
    }
}
