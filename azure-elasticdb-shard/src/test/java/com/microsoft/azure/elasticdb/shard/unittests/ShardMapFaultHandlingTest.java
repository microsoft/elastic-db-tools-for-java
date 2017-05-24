package com.microsoft.azure.elasticdb.shard.unittests;

import java.sql.SQLException;

public class ShardMapFaultHandlingTest {

  public static SQLException TransientSqlException = ShardMapFaultHandlingTest.createSqlException();

  //TODO:Reflection in java
  private static SQLException createSqlException() {
    /*java.lang.reflect.Constructor ciSqlError = SqlError.class
        .GetConstructors(BindingFlags.Instance.getValue() | BindingFlags.NonPublic.getValue())
        .Single(c -> c.GetParameters().getLength() == 7);

    // C# TO JAVA CONVERTER WARNING: Unsigned integer types have no direct equivalent in Java:
    // ORIGINAL LINE: SqlError se = (SqlError)ciSqlError.Invoke(new object[] { (int)10928, (byte)0,
    // (byte)0, "", "", "", (int)0 });
    SqlError se = (SqlError) ciSqlError
        .newInstance(new Object[] {(int) 10928, (byte) 0, (byte) 0, "", "", "", (int) 0});

    java.lang.reflect.Constructor ciSqlErrorCollection = SqlErrorCollection.class
        .GetConstructors(BindingFlags.Instance.getValue() | BindingFlags.NonPublic.getValue())
        .Single();

    SqlErrorCollection sec = (SqlErrorCollection) ciSqlErrorCollection.newInstance(new Object[0]);

    java.lang.reflect.Method miSqlErrorCollectionAdd = SqlErrorCollection.class.GetMethod("Add",
        BindingFlags.Instance.getValue() | BindingFlags.NonPublic.getValue());

    miSqlErrorCollectionAdd.Invoke(sec, new Object[] {se});

    java.lang.reflect.Method miSqlExceptionCreate = SQLException.class.GetMethod("CreateException",
        BindingFlags.Static.getValue() | BindingFlags.NonPublic.getValue(), null,
        new java.lang.Class[] {SqlErrorCollection.class, String.class}, null);

    SQLException sqlException =
        (SQLException) miSqlExceptionCreate.invoke(null, new Object[] {sec, ""});*/

    return null;
  }
}
