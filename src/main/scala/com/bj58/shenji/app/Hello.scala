package com.bj58.shenji.app

import scala.collection.mutable.Map

object Hello {
  def main(args : Array[String]) {
    println( "Hello World!" )
    val testUser = "(HZGNrH7_u-FHn7I2rytdEhQsnNOaIk,50940), (pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE,39409), (uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE,37100), (RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk,33170), (m1NfUhbQubPhUbG5yWKpPYFn07FKuk,32937), (m1NfUh3QuhcYwNuzyAt30duwXMPKuk,30431), (NDwwyBqyugRvuDOOE1EosdR3ERRdNE,28696), (m1NfUh3QuA_oIR73N-E30DPlRh6Kuk,28512), (w-RDugRAubGPNLFWmYNoNgPJnAqvNE,28509), (uvVYENdyubQVuRw8pHwuEN65PLKOIk,28178), (RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE,27172), (UvqNu7K_uyIgyWR60gDvw7GjPA6GNE,27093), (yb0Qwj7_uRRC2YIREycfRM-jm17ZIk,26737), (m1NfUh3QuhR2NWNduDqWi7uWmdFKuk,26402), (njRWwDuARMmo0A6amNqCuDwiibRKuk,25908), (m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk,25574), (m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk,25363), (m1NfUhbQujboiZKAEM0zNY7OUYVKuk,25254), (m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk,24649)"
    val userCount = testUser.split("\\), \\(")
            .map(_.replace("(", "").replace(")",""))
            .map(_.split(","))
            .map(values => (values(0), values(1).toInt))
            
    val map = Map[String,Int]() ++= userCount
//    val rs = map ++: userCount
    println(map)
  }
}