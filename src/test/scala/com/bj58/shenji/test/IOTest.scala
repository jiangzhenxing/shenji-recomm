package com.bj58.shenji.test

import java.io._

object IOTest extends App {
  splitData
  println("OK!")
  
  def splitData = 
  {
    "(HZGNrH7_u-FHn7I2rytdEhQsnNOaIk,50940), (pvG8ihRAmWFiP17JpRcdwg7Y0LDYNE,39409), (uA-ZPD-AuHP2rAF_Pv-oIY_1w1FNNE,37100), (RDqMHZ6Ay-ufNRwoi1wFpZKFU7uhuk,33170), (m1NfUhbQubPhUbG5yWKpPYFn07FKuk,32937), (m1NfUh3QuhcYwNuzyAt30duwXMPKuk,30431), (NDwwyBqyugRvuDOOE1EosdR3ERRdNE,28696), (m1NfUh3QuA_oIR73N-E30DPlRh6Kuk,28512), (w-RDugRAubGPNLFWmYNoNgPJnAqvNE,28509), (uvVYENdyubQVuRw8pHwuEN65PLKOIk,28178), (RNu7u-GAm1Nd0vF3rNI7RWK8IZK_EE,27172), (UvqNu7K_uyIgyWR60gDvw7GjPA6GNE,27093), (yb0Qwj7_uRRC2YIREycfRM-jm17ZIk,26737), (m1NfUh3QuhR2NWNduDqWi7uWmdFKuk,26402), (njRWwDuARMmo0A6amNqCuDwiibRKuk,25908), (m1NfUh3Qu-PgnMw701FpmREvIZ6Kuk,25574), (m1NfUMnQu-PrmvqJP-PEiY7LIHPKuk,25363), (m1NfUhbQujboiZKAEM0zNY7OUYVKuk,25254), (m1NfUMK_mv_OEy7VnL0OpYndPd6Kuk,24649)"
    .split("\\), \\(")
    .map(_.replace("(", "").replace(")",""))
    .map(_.split(","))
    .map(values => (values(0), values(1).toInt))
    .map { case (cookieid, count) => 
                val reader = new BufferedReader(new InputStreamReader(new FileInputStream("data/userdata/" + cookieid)))
                val train = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/userdata/train/" + cookieid)))
                val valid = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/userdata/valid/" + cookieid)))
                val test = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("data/userdata/test/" + cookieid)))
                val num_train = (count * .8).intValue
                val num_valid = (count * .1).intValue
                val num_test = count - num_train - num_valid
                println(cookieid, count, num_train, num_valid, num_test)
                Range(0, num_train).foreach(x => train.write(reader.readLine() + "\n"))
                Range(0, num_valid).foreach(x => valid.write(reader.readLine() + "\n"))
                Range(0, num_test).foreach(x => test.write(reader.readLine() + "\n"))
                reader.close()
                train.close()
                valid.close()
                test.close()
         }
  }
}