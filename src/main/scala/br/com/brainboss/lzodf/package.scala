package br.com.brainboss

import br.com.brainboss.util.hashStr
import org.apache.spark.sql.functions.udf

package object lzodf {
  val hashUdf = udf((str: String) => hashStr(str))
}
