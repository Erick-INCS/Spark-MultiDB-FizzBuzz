// load the databases as datasets
val db0 = spark.read.
    format("jdbc").
    option("url", "jdbc:firebirdsql:localhost/3050:/firebird/data/testdb.fdb?encoding=UTF8").
    option("user", "sysdba").
    option("password", "30d7f099e450d5a857a1").
    option("dbtable", "TESTTABLE").
    load()

val db1 = spark.read.
    format("jdbc").
    option("url", "jdbc:firebirdsql:localhost/3050:/firebird/data/testdb2.fdb?encoding=UTF8").
    option("user", "sysdba").
    option("password", "30d7f099e450d5a857a1").
    option("dbtable", "ANOTHERTABLE").
    load()


if (db0.count == 0) {
    (for (i <- 0 to 100 if i % 5 == 0) yield ("Num: " + i, i)).
    toDF("COL1", "COL2").write.
    mode(org.apache.spark.sql.SaveMode.Append).
    format("jdbc").
    option("url", "jdbc:firebirdsql:localhost/3050:/firebird/data/testdb.fdb?encoding=UTF8").
    option("user", "sysdba").
    option("password", "30d7f099e450d5a857a1").
    option("dbtable", "TESTTABLE").
    save()
}


if (db1.count == 0) {
    (for (i <- 0 to 100 if i % 3 == 0) yield ("Num: " + i, i)).
    toDF("ACOL1", "ACOL2").write.
    mode(org.apache.spark.sql.SaveMode.Append).
    format("jdbc").
    option("url", "jdbc:firebirdsql:localhost/3050:/firebird/data/testdb2.fdb?encoding=UTF8").
    option("user", "sysdba").
    option("password", "30d7f099e450d5a857a1").
    option("dbtable", "ANOTHERTABLE").
    save()
}


val fizzbuzz = db0.except(db1).withColumn("COL1", lit("Fizz")).
    union(db1.except(db0).withColumn("ACOL1", lit("Buzz"))).
    union(db0.intersect(db1).withColumn("COL1", lit("FizzBuzz")))

fizzbuzz.show(100)
fizzbuzz.write.
    mode(org.apache.spark.sql.SaveMode.Ignore).
    format("jdbc").
    option("url", "jdbc:firebirdsql:localhost/3050:/firebird/data/testdb.fdb?encoding=UTF8").
    option("user", "sysdba").
    option("password", "30d7f099e450d5a857a1").
    option("dbtable", "R").
    save()