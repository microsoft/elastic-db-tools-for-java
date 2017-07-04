package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

import com.microsoft.azure.elasticdb.core.commons.transientfaulthandling.ITransientErrorDetectionStrategy;
import com.microsoft.azure.elasticdb.query.exception.MultiShardAggregateException;
import com.microsoft.azure.elasticdb.query.helpers.Action0Param;
import com.microsoft.azure.elasticdb.query.helpers.Func1Param;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardConnection;
import com.microsoft.azure.elasticdb.query.multishard.MultiShardStatement;
import com.microsoft.azure.elasticdb.shard.base.ShardLocation;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Purpose:
 * Unit tests for the cross shard query client library. Tests create mock Connection, Statement and
 * ResultSet objects to enable greater flexibility in crafting test scenarios and also eliminate
 * the need for a running sqlserver instance.
 *
 * DEVNOTE (VSTS 2202789): Work in progress.
 */
public class MultiShardUnitTests {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void MyClassInitialize() {
  }

  public static void MyClassCleanup() {
  }

//TODO:
//  /**
//   * Simulate a long running command on a shard
//   * and validate that MultiShardCommand throws a
//   * timeout exception to the user
//   */
//  public final void TestShardCommandTimeoutException() {
//    ArrayList<Pair<ShardLocation, Connection>> shardConnections = CreateConnections(10, () -> {
//    });
//
//    Func1Param<MockSqlStatement, ResultSet> executeReaderFunc = (token, cmd) -> {
//      Thread.sleep(12);
//      return new MockSqlResultSet();
//    };
//    MockSqlStatement mockCmd = new MockSqlStatement();
//    mockCmd.setExecuteReaderFunc(
//        (CancellationToken arg1, MockSqlStatement arg2) -> executeReaderFunc.invoke(arg1, arg2));
//    mockCmd.setCommandText("Select 1");
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = MultiShardCommand.Create(conn, mockCmd, 1)) {
//        cmd.ExecuteReader();
//      }
//    }
//  }
//
//  /**
//   * Test the command Cancel()
//   */
//  public final void TestShardCommandFaultHandler() {
//    ArrayList<Pair<ShardLocation, Connection>> shardConnections = CreateConnections(10, () -> {
//    });
//
//    Func2Param<CancellationToken, MockSqlStatement, DbDataReader> executeReaderFunc = (token, cmd) -> {
//      throw new InsufficientMemoryException();
//    };
//    MockSqlStatement mockCmd = new MockSqlStatement();
//    mockCmd.setExecuteReaderFunc(
//        (CancellationToken arg1, MockSqlStatement arg2) -> executeReaderFunc.invoke(arg1, arg2));
//    mockCmd.setCommandText("Select 1");
//    java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean> passedLocations = new java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean>();
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = MultiShardCommand.Create(conn, mockCmd, 1)) {
//        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//        cmd.CommandTimeout = 300;
//        cmd.shardExecutionFaulted += new EventHandler<ShardExecutionEventArgs>((obj, eventArgs) -> {
//          Assert.IsTrue(shardConnections.Select(x -> x.Item1).Contains(eventArgs.ShardLocation),
//              "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
//          passedLocations.put(eventArgs.ShardLocation, true);
//          Assert.IsInstanceOfType(eventArgs.Exception, InsufficientMemoryException.class,
//              "An incorrect exception type was passed to the event handler.");
//        });
//        try {
//          // We want to execute to completion so we get to the validation at the end of the function.
//          cmd.ExecuteReader();
//        } catch (Exception e) {
//        }
//      }
//    }
//
//    Assert.AreEqual(shardConnections.size(), passedLocations.size(),
//        "Not every ShardLocation had its corresponding event handler invoked.");
//  }
//
//  /**
//   * Test the command Cancel()
//   */
//  public final void TestShardCommandCancellation() {
//    // Create connections to a few shards
//    var shardConnections = CreateConnections(10, () -> {
//    });
//
//    MockSqlStatement mockCmd = new MockSqlStatement();
//    ManualResetEvent cmdStartEvent = new ManualResetEvent(false);
//    mockCmd.setExecuteReaderFunc((token, cmd) -> {
//      while (true) {
//        if (token == null) {
//          break;
//        }
//        token.ThrowIfCancellationRequested();
//        Thread.sleep(500);
//        cmdStartEvent.Set();
//      }
//
//      return new MockSqlResultSet();
//    });
//    mockCmd.setCommandText("select 1");
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = MultiShardCommand.Create(conn, mockCmd, 300)) {
//        try {
//          // start the Cancel on a separate thread
//          Task executeTask = Task.Run(() -> {
//            cmdStartEvent.WaitOne();
//            cmd.Cancel();
//          });
//
//          cmd.ExecuteReader();
//          executeTask.Wait();
//          Assert.Fail("We should always be throwing an exception.");
//        } catch (RuntimeException ex) {
//          Assert.IsTrue(ex instanceof OperationCanceledException,
//              "OperationCanceledException expected. Found {0}!", ex.toString());
//        }
//      }
//    }
//  }
//
//  /**
//   * Test the command Cancel()
//   */
//  public final void TestShardCommandCancellationHandler() {
//    // Create connections to a few shards
//    var shardConnections = CreateConnections(10, () -> {
//    });
//
//    MockSqlStatement mockCmd = new MockSqlStatement();
//    ManualResetEvent cmdStartEvent = new ManualResetEvent(false);
//    mockCmd.setExecuteReaderFunc((token, cmd) -> {
//      while (true) {
//        if (token == null) {
//          break;
//        }
//        token.ThrowIfCancellationRequested();
//        Thread.sleep(500);
//        cmdStartEvent.Set();
//      }
//
//      return new MockSqlResultSet();
//    });
//
//    mockCmd.setCommandText("select 1");
//    java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean> passedLocations = new java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean>();
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = MultiShardCommand.Create(conn, mockCmd, 300)) {
//        cmd.shardExecutionCanceled += new EventHandler<ShardExecutionEventArgs>(
//            (obj, eventArgs) -> {
//              Assert.IsTrue(shardConnections.Select(x -> x.Item1).Contains(eventArgs.ShardLocation),
//                  "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
//              passedLocations.put(eventArgs.ShardLocation, true);
//            });
//        try {
//          // start the Cancel on a separate thread
//          Task executeTask = Task.Run(() -> {
//            cmdStartEvent.WaitOne();
//            cmd.Cancel();
//          });
//
//          cmd.ExecuteReader();
//          executeTask.Wait();
//          Assert.Fail("We should always be throwing an exception.");
//        } catch (RuntimeException ex) {
//          Assert.IsTrue(ex instanceof OperationCanceledException,
//              "OperationCanceledException expected. Found {0}!", ex.toString());
//        }
//      }
//    }
//    Assert.AreEqual(shardConnections.size(), passedLocations.size(),
//        "Not every ShardLocation had its corresponding event handler invoked.");
//  }
//
//  /**
//   * Test the command behavior validation
//   */
//  public final void TestShardCommandBehavior() {
//    var shardConnections = CreateConnections(10, () -> {
//    });
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = conn.CreateCommand()) {
//        CommandBehavior behavior = CommandBehavior.SingleResult;
//        behavior &= CommandBehavior.SingleRow;
//        cmd.ExecuteReader(behavior);
//      }
//    }
//  }
//
//  /**
//   * Test the event handler for OnShardBegin, ensuring that every shard in a successful execution
//   * has begin called at least once.
//   */
//  public final void TestShardCommandBeginHandler() {
//    var shardConnections = CreateConnections(10, () -> {
//    });
//    java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean> passedLocations = new java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean>();
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      Func2Param<CancellationToken, MockSqlStatement, DbDataReader> executeReaderFunc = (token, cmd) -> {
//        Thread.sleep(TimeSpan.FromSeconds(2));
//        return new MockSqlResultSet();
//      };
//
//      MockSqlStatement mockCmd = new MockSqlStatement();
//      mockCmd.setExecuteReaderFunc(
//          (CancellationToken arg1, MockSqlStatement arg2) -> executeReaderFunc.invoke(arg1, arg2));
//      mockCmd.setCommandText("Select 1");
//      try (MultiShardCommand cmd = MultiShardCommand.Create(conn, mockCmd, 10)) {
//        cmd.shardExecutionBegan += new EventHandler<ShardExecutionEventArgs>((obj, eventArgs) -> {
//          Assert.IsTrue(shardConnections.Select(x -> x.Item1).Contains(eventArgs.ShardLocation),
//              "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
//          passedLocations.put(eventArgs.ShardLocation, true);
//        });
//        CommandBehavior behavior = CommandBehavior.Default;
//        cmd.ExecuteReader(behavior);
//      }
//    }
//
//    Assert.AreEqual(shardConnections.size(), passedLocations.size(),
//        "Not every ShardLocation had its corresponding event handler invoked.");
//  }
//
//  /**
//   * Test the event handler for OnShardBegin, ensuring that every shard in a successful execution
//   * has begin called at least once.
//   */
//  public final void TestShardCommandSucceedHandler() {
//    var shardConnections = CreateConnections(10, () -> {
//    });
//    java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean> passedLocations = new java.util.concurrent.ConcurrentHashMap<ShardLocation, Boolean>();
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      Func2Param<CancellationToken, MockSqlStatement, DbDataReader> executeReaderFunc = (token, cmd) -> {
//        Thread.sleep(TimeSpan.FromSeconds(2));
//        return new MockSqlResultSet();
//      };
//
//      MockSqlStatement mockCmd = new MockSqlStatement();
//      mockCmd.setExecuteReaderFunc(
//          (CancellationToken arg1, MockSqlStatement arg2) -> executeReaderFunc.invoke(arg1, arg2));
//      mockCmd.setCommandText("Select 1");
//      try (MultiShardCommand cmd = MultiShardCommand.Create(conn, mockCmd, 10)) {
//        cmd.shardExecutionSucceeded += new EventHandler<ShardExecutionEventArgs>(
//            (obj, eventArgs) -> {
//              Assert.IsTrue(shardConnections.Select(x -> x.Item1).Contains(eventArgs.ShardLocation),
//                  "The ShardLocation passed to the event handler does not exist in the set of passed in ShardLocations");
//              passedLocations.put(eventArgs.ShardLocation, true);
//            });
//        CommandBehavior behavior = CommandBehavior.Default;
//        cmd.ExecuteReader(behavior);
//      }
//    }
//
//    Assert.AreEqual(shardConnections.size(), passedLocations.size(),
//        "Not every ShardLocation had its corresponding event handler invoked.");
//  }
//
//  /**
//   * Simple test that validates that
//   * the retry logic works as expected:
//   * - Create a retry policy so we retry upto n times on failure
//   * - Have the MockSqlConnection throw a transient exception upto the (n-1)th retry
//   * - Validate that the MultiShardCommand indeed retries upto (n-1) times for each
//   * shard and succeeds on the nth retry.
//   */
//  public final void TestShardCommandRetryBasic() {
//    RetryPolicy retryPolicy = new RetryPolicy(4, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10),
//        TimeSpan.FromMilliseconds(100));
//    int[] openRetryCounts = new int[10];
//    ArrayList<Tuple<ShardLocation, DbConnection>> shardConnections = new ArrayList<Tuple<ShardLocation, DbConnection>>();
//
//    // Create ten mocked connections, each will retry retryPolicy.RetryCount - 1 times,
//    // and keep it's own actual retry count in one of the elements of openRetryCounts
//    for (int i = 0; i < 10; i++) {
//      String database = String.format("Shard%1$s", i);
//
//      // We want to close on the value of i
//      int j = i;
//      Action0Param executeOnOpen = () -> {
//        if (openRetryCounts[j] < (retryPolicy.RetryCount - 1)) {
//          Logger.Log("Current retry count for database: {0} is {1}", database, openRetryCounts[j]);
//          openRetryCounts[j]++;
//          throw new TimeoutException();
//        }
//      };
//
//      MockSqlConnection mockCon = new MockSqlConnection(database, executeOnOpen);
//      shardConnections.add(
//          new Tuple<ShardLocation, DbConnection>(new ShardLocation("test", database), mockCon));
//    }
//
//    MockSqlStatement mockCmd = new MockSqlStatement();
//    mockCmd.setExecuteReaderFunc((t, c) -> new MockSqlResultSet());
//    mockCmd.setCommandText("select 1");
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = MultiShardCommand.Create(conn, mockCmd, 300)) {
//        cmd.ExecutionOptions = MultiShardExecutionOptions.None;
//        cmd.RetryPolicy = retryPolicy;
//        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//        cmd.ExecuteReader(CommandBehavior.Default);
//      }
//    }
//
//    for (int i = 0; i < openRetryCounts.length; i++) {
//      assert retryPolicy.RetryCount - 1 == openRetryCounts[i];
//    }
//  }
//
//  /**
//   * - Verify that upon retry exhaustion, the underlying exception
//   * is caught correctly.
//   * - Also validate that any open connections are closed.
//   */
//  public final void TestShardCommandRetryExhaustion() {
//    RetryPolicy retryPolicy = new RetryPolicy(2, TimeSpan.FromMilliseconds(100),
//        TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
//    ArrayList<Tuple<ShardLocation, DbConnection>> shardConnections = new ArrayList<Tuple<ShardLocation, DbConnection>>();
//
//    // Create ten mocked connections, half of them will throw exceptions on Open
//    for (int i = 0; i < 10; i++) {
//      String database = String.format("Shard%1$s", i);
//
//      int j = i;
//      Action0Param executeOnOpen = () -> {
//        if (j < 5) {
//          throw new TimeoutException();
//        }
//      };
//
//      MockSqlConnection mockCon = new MockSqlConnection(database, executeOnOpen);
//      shardConnections.add(
//          new Tuple<ShardLocation, DbConnection>(new ShardLocation("test", database), mockCon));
//    }
//
//    MockSqlStatement mockCmd = new MockSqlStatement();
//    mockCmd.setExecuteReaderFunc((t, c) -> new MockSqlResultSet());
//    mockCmd.setCommandText("select 1");
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = MultiShardCommand.Create(conn, mockCmd, 300)) {
//        cmd.ExecutionOptions = MultiShardExecutionOptions.None;
//        cmd.RetryPolicy = retryPolicy;
//        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//        MultiShardDataReader rdr = cmd.ExecuteReader(CommandBehavior.Default);
//
//        // Validate the right exception is re-thrown
//        Assert.IsTrue(rdr.MultiShardExceptions.size() == 5, "Expected MultiShardExceptions!");
//        for (MultiShardException ex : rdr.MultiShardExceptions) {
//          Assert.IsTrue(ex.getCause() instanceof TimeoutException, "Expected TimeoutException!");
//        }
//
//        // Validate that the connections for the faulted readers are closed
//        for (int i = 0; i < 5; i++) {
//          Assert.IsTrue(shardConnections.get(i).Item2.State == ConnectionState.Closed,
//              "Expected Connection to be Closed!");
//        }
//      }
//    }
//  }
//
//  /**
//   * - Close the connection upon hitting a transient exception
//   * - Validate that the command is re-tried and
//   * that the connection is re-opened
//   */
//  public final void TestShardCommandRetryConnectionReopen() {
//    RetryPolicy retryPolicy = new RetryPolicy(4, TimeSpan.FromMilliseconds(100),
//        TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100));
//    ArrayList<Tuple<ShardLocation, DbConnection>> shardConnections = new ArrayList<Tuple<ShardLocation, DbConnection>>();
//
//    // Callback to execute when the MockCommand is invoked
//    Func2Param<CancellationToken, MockSqlStatement, DbDataReader> ExecuteReaderFunc = (CancellationToken arg1, MockSqlStatement arg2) -> null
//        .invoke(arg1, arg2);
//
//    // Number of times each command has been retried
//    int[] commandRetryCounts = new int[10];
//
//    // Create ten mocked connections,
//    // a few of them will throw an exception on Open
//    // and the rest will throw an exception on command execution upto 2 retries
//    // At the end, all commands should complete successfully.
//    for (int i = 0; i < 10; i++) {
//      String database = String.format("%1$s", i);
//
//      int j = i;
//      int retryCount = 0;
//      Action0Param executeOnOpen = () -> {
//        if (j < 5) {
//          if (retryCount < 3) {
//            retryCount++;
//            throw new TimeoutException();
//          }
//        }
//      };
//
//      MockSqlConnection mockCon = new MockSqlConnection(database, executeOnOpen);
//      shardConnections.add(
//          new Tuple<ShardLocation, DbConnection>(new ShardLocation("Shard", database), mockCon));
//    }
//
//    ExecuteReaderFunc = (t, r) -> {
//      int index = Integer.parseInt(((MockSqlConnection) (r.Connection)).getConnectionString());
//      if (r.Connection.State == ConnectionState.Closed) {
//        throw new IllegalStateException("Command shouldn't be executed on a closed connection!");
//      }
//
//      if (index > 5 && commandRetryCounts[index] < 3) {
//        commandRetryCounts[index]++;
//        r.RetryCount++;
//        r.Connection.Close();
//        throw new TimeoutException();
//      } else {
//        MockSqlResultSet mockRdr = new MockSqlResultSet();
//        mockRdr.ExecuteOnReadAsync = (rdr) -> {
//          return Task.<Boolean>Run(() -> {
//            boolean isClosed = rdr.IsClosed;
//            rdr.Close();
//            return !isClosed;
//          });
//        };
//        return mockRdr;
//      }
//    };
//
//    MockSqlStatement mockCmd = new MockSqlStatement();
//    mockCmd.setExecuteReaderFunc(
//        (CancellationToken arg1, MockSqlStatement arg2) -> ExecuteReaderFunc.invoke(arg1, arg2));
//    mockCmd.setCommandText("select 1");
//    try (MultiShardConnection conn = new MultiShardConnection(shardConnections)) {
//      try (var cmd = MultiShardCommand.Create(conn, mockCmd, 300)) {
//        cmd.RetryPolicy = retryPolicy;
//        cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;
//        try (var reader = cmd.ExecuteReaderAsync().Result) {
//          // Validate that we successfully received a reader
//          // from each one of the shards
//          int readerCount = 0;
//          while (reader.Read()) {
//            readerCount++;
//          }
//          Assert.AreEqual(10, readerCount, "Expected 10 readers!");
//        }
//      }
//    }
//  }
//
//  /**
//   * Test the custom serializion logic for exceptions
//   */
//  public final void TestExceptionSerialization() {
//    ShardLocation sl1 = new ShardLocation("dataSource1", "database1");
//    ShardLocation sl2 = new ShardLocation("dataSource2", "database2");
//
//    MultiShardException innerEx1 = new MultiShardException(sl1);
//    MultiShardException innerEx2 = new MultiShardException(sl2);
//
//    ArrayList<RuntimeException> exList = new ArrayList<RuntimeException>();
//    exList.add(innerEx1);
//    exList.add(innerEx2);
//
//    MultiShardAggregateException aggEx = new MultiShardAggregateException(exList);
//
//    TestSerialization<MultiShardException> (innerEx1);
//    TestSerialization<MultiShardException> (innerEx2);
//    TestSerialization<MultiShardAggregateException> (aggEx);
//  }
//
//  /**
//   * Validates that the MultiShardDataReader
//   * handles an exception during Read() properly
//   */
//  public final void TestDataReaderReadException() {
//    // Setup two data readers from shards
//    MockSqlResultSet mockReader1 = new MockSqlResultSet("Reader1");
//    MockSqlResultSet mockReader2 = new MockSqlResultSet("Reader2");
//    boolean movedOnToNextReader = false;
//    int invokeCount = 1;
//    Func1Param<MockSqlResultSet, Task<Boolean>> ExecuteOnReadAsync = (MockSqlResultSet r) -> {
//      return Task.<Boolean>Run(() -> {
//        // First reader throws an exception when Read
//        if (r.Name.equals("Reader1")) {
//          if (invokeCount == 2) {
//            throw new IllegalStateException();
//          }
//        } else {
//          movedOnToNextReader = true;
//        }
//        return true;
//      });
//    };
//
//    Action1Param<MockSqlResultSet> ExecuteOnGetColumn = (MockSqlResultSet r) -> {
//      if (r.Name.equals("Reader1")) {
//        throw new IllegalStateException();
//      }
//    };
//
//    mockReader1.ExecuteOnReadAsync = ExecuteOnReadAsync;
//    mockReader1.ExecuteOnGetColumn = ExecuteOnGetColumn;
//    mockReader2.ExecuteOnReadAsync = ExecuteOnReadAsync;
//    LabeledDbDataReader[] labeledDataReaders = new LabeledDbDataReader[2];
//
//    labeledDataReaders[0] = new LabeledDbDataReader(mockReader1,
//        new ShardLocation("test", "Shard1"), new MockSqlStatement() {
//      Connection =new
//
//      MockSqlConnection("",() ->
//
//      {
//      })
//    });
//    labeledDataReaders[1] = new LabeledDbDataReader(mockReader2,
//        new ShardLocation("test", "Shard2"), new MockSqlStatement() {
//      Connection =new
//
//      MockSqlConnection("",() ->
//
//      {
//      })
//    });
//
//    // Create the MultiShardDataReader
//    var mockMultiShardCmd = MultiShardCommand.Create(null, "test");
//    MultiShardDataReader multiShardDataReader = new MultiShardDataReader(mockMultiShardCmd,
//        labeledDataReaders, MultiShardExecutionPolicy.PartialResults, false);
//
//    // Validate that if an exception is thrown when reading a column,
//    // it is propagated back to the user
//    try {
//      multiShardDataReader.Read();
//      invokeCount++;
//      multiShardDataReader.GetInt32(0);
//    } catch (RuntimeException ex) {
//      Assert.IsTrue(ex instanceof IllegalStateException, "Expected InvalidOperationException!");
//    }
//
//    // Validate that we didn't automatically move on to the next reader when we
//    // hit an exception whilst reading the column and that
//    // an exception from a second Read() call is stored and the reader is closed
//    multiShardDataReader.Read();
//    Assert.AreEqual(multiShardDataReader.MultiShardExceptions.size(), 1,
//        "Expected exception to be recorded");
//    Assert.IsTrue(mockReader1.IsClosed, "Expected reader to be closed!");
//
//    // Validate we immediately moved on to the next reader
//    multiShardDataReader.Read();
//    Assert.IsTrue(movedOnToNextReader, "Should've moved on to next reader");
//  }
//
//  /**
//   * Verify MultiShardDataReader handling of readers with a null schema
//   * for the following cases:
//   * - Case #1: All Readers have a null schema. Verify that no exception is thrown.
//   * - Case #2: The first half of readers have a null schema and the rest are non-null.
//   * Verify that a MultiShardDataReaderInternalException is thrown.
//   * - Case #3: The first half of readers have a non-null schema and the rest are null.
//   * Verify that a MultiShardDataReaderInternalException is thrown.
//   */
//  public final void TestAddDataReaderWithNullSchema() {
//    // Creates a MultiShardDataReader and verifies that the right exception is thrown
//    Func1Param<LabeledDbDataReader[], Boolean> createMultiShardReader = (LabeledDbDataReader[] readers) -> {
//      boolean hitNullSchemaException = false;
//
//      try {
//        var mockMultiShardCmd = MultiShardCommand.Create(null, "test");
//        MultiShardDataReader multiShardDataReader = new MultiShardDataReader(mockMultiShardCmd,
//            readers, MultiShardExecutionPolicy.PartialResults, false, readers.getLength());
//      } catch (MultiShardDataReaderInternalException ex) {
//        hitNullSchemaException = ex.getMessage().Contains("null schema");
//      }
//
//      return hitNullSchemaException;
//    };
//
//    LabeledDbDataReader[] labeledDataReaders = new LabeledDbDataReader[10];
//
//    // Create a few mock readers. All with a null schema
//    for (int i = 0; i < labeledDataReaders.length; i++) {
//      MockSqlResultSet mockReader = new MockSqlResultSet(String.format("Reader%1$s", i), null);
//      labeledDataReaders[i] = new LabeledDbDataReader(mockReader,
//          new ShardLocation("test", String.format("Shard%1$s", i)), new MockSqlStatement() {
//        Connection =new
//
//        MockSqlConnection("",() ->
//
//        {
//        })
//      });
//    }
//
//    // Case #1
//    boolean hitException = createMultiShardReader.invoke(labeledDataReaders);
//    Assert.IsFalse(hitException, "Unexpected exception! All readers have a null schema.");
//
//    // Case #2
//    for (int i = 0; i < labeledDataReaders.length; i++) {
//      MockSqlResultSet mockReader = (MockSqlResultSet) labeledDataReaders[i].DbDataReader;
//      mockReader.Open();
//
//      if (i > labeledDataReaders.length / 2) {
//        mockReader.DataTable = new DataTable();
//      }
//    }
//
//    hitException = createMultiShardReader.invoke(labeledDataReaders);
//    Assert.IsTrue(hitException,
//        "Exception not hit! Second half of readers don't have a null schema!");
//
//    // Case #3
//    for (int i = 0; i < labeledDataReaders.length; i++) {
//      MockSqlResultSet mockReader = (MockSqlResultSet) labeledDataReaders[i].DbDataReader;
//      mockReader.Open();
//
//      if (i < labeledDataReaders.length / 2) {
//        mockReader.DataTable = new DataTable();
//      } else {
//        mockReader.DataTable = null;
//      }
//    }
//
//    hitException = createMultiShardReader.invoke(labeledDataReaders);
//    Assert
//        .IsTrue(hitException, "Exception not hit! First half of readers don't have a null schema!");
//  }
//
//  private <T extends RuntimeException> void TestSerialization(T originalException) {
//    MemoryStream memStream = new MemoryStream();
//    BinaryFormatter formatter = new BinaryFormatter();
//
//    formatter.Serialize(memStream, originalException);
//    memStream.Seek(0, SeekOrigin.Begin);
//
//    T deserializedException = (T) formatter.Deserialize(memStream);
//    memStream.Close();
//
//    CompareForEquality(originalException, deserializedException);
//  }
//
//  private void CompareForEquality(RuntimeException first, RuntimeException second) {
//    assert first.getClass() == second.getClass();
//
//    if (first instanceof MultiShardException) {
//      DoExceptionComparison((MultiShardException) first, (MultiShardException) second);
//      return;
//    }
//    if (first instanceof MultiShardAggregateException) {
//      DoExceptionComparison((MultiShardAggregateException) first,
//          (MultiShardAggregateException) second);
//      return;
//    }
//    Assert.Fail(String.format("Unknown exception type: %1$s", first.getClass()));
//  }
//
//  private void DoExceptionComparison(MultiShardException first, MultiShardException second) {
//    assert first.getShardLocation().getDatabase().equals(second.getShardLocation().getDatabase());
//    assert first.getShardLocation().getDataSource()
//        .equals(second.getShardLocation().getDataSource());
//  }
//
//  private void DoExceptionComparison(MultiShardAggregateException first,
//      MultiShardAggregateException second) {
//    assert first.InnerExceptions.size() == second.InnerExceptions.size();
//    for (int i = 0; i < first.InnerExceptions.size(); i++) {
//      CompareForEquality((MultiShardException) (first.InnerExceptions[i]),
//          (MultiShardException) (second.InnerExceptions[i]));
//    }
//  }

  private ArrayList<Pair<ShardLocation, MockSqlConnection>> createConnections(int count,
      Action0Param executeOnOpen) {
    ArrayList<Pair<ShardLocation, MockSqlConnection>> shardConnections = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      String database = String.format("Shard%1$s", i);
      MockSqlConnection mockCon = new MockSqlConnection(
          MultiShardTestUtils.getTestConnectionString(database), executeOnOpen);
      shardConnections.add(new ImmutablePair<>(new ShardLocation(
          MultiShardTestUtils.getServerName(), database), mockCon));
    }

    return shardConnections;
  }

  /**
   * Test that an exception in Open()
   * by a particular shard is propagated by
   * MultiShardConnection back to the user
   */
  public final void testShardConnectionOpenException() throws Exception {
    try {
      Action0Param executeOnOpen = () -> {
        throw new OutOfMemoryError();
      };
      ArrayList<Pair<ShardLocation, MockSqlConnection>> shardConnections = createConnections(10,
          executeOnOpen);
      ShardLocation[] shardLocations = (ShardLocation[]) shardConnections.stream()
          .map(Pair::getLeft).toArray();
      try (MultiShardConnection conn = new MultiShardConnection(
          MultiShardTestUtils.MULTI_SHARD_TEST_CONN_STRING, shardLocations)) {
        shardConnections.get(0).getRight().open();
        try (MultiShardStatement cmd = MultiShardStatement.create(conn, "select 1", 100)) {
          cmd.executeQuery();
        }
      }
    } catch (RuntimeException ex) {
      if (ex instanceof MultiShardAggregateException) {
        MultiShardAggregateException maex = (MultiShardAggregateException) ex;
        log.info("Exception message: {}.\n Exception tostring: {}", ex.getMessage(), ex.toString());
        throw (MultiShardAggregateException) maex.getCause();
      }
      throw ex;
    }
  }

  /**
   * Open up a clean connection to each test database prior to each test.
   */
  public final void MyTestInitialize() {
  }

  /**
   * Close our connections to each test database after each test.
   */
  public final void MyTestCleanup() {
  }

  public static class MockTransientErrorDetectionStrategy implements
      ITransientErrorDetectionStrategy {

    private Func1Param<Exception, Boolean> evaluateException;

    public MockTransientErrorDetectionStrategy(
        Func1Param<Exception, Boolean> evaluateException) {
      this.evaluateException = evaluateException;
    }

    public final Func1Param<Exception, Boolean> getEvaluateException() {
      return evaluateException;
    }

    public final boolean isTransient(Exception ex) {
      return evaluateException.invoke(ex);
    }
  }
}
