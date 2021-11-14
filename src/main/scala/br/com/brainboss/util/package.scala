package br.com.brainboss

package object util {
  def hashStr (str: String): Int = {
    //    Funçao de hash aqui
    str.hashCode
  }
}
