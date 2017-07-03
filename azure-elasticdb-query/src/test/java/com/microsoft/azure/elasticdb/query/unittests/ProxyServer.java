package com.microsoft.azure.elasticdb.query.unittests;

/*Copyright (c) Microsoft. All rights reserved.
Licensed under the MIT license. See LICENSE file in the project root for full license information.*/

/**
 * This is a simple network listener that is redirects the traffic. It is used to simulate network
 * delay. Purpose: Network proxy useful for simulating network failures, connection failures, etc.
 */
public class ProxyServer /*implements AutoCloseable*/ {
//
//  private volatile boolean _stopRequested;
//  private StringBuilder _eventLog = new StringBuilder(s_logHeader, 1000);
//  private static String s_logHeader = "======== ProxyServer Log Start ========\n";
//  private static String s_logTrailer = "======== ProxyServer Log End ========\n";
//
//  /**
//   * Gets the event log for the proxy server
//   */
//  public final StringBuilder getEventLog() {
//    return _eventLog;
//  }
//
//  /**
//   * SleepResetEvent to cancel proxy server sleep delay
//   */
//  public ManualResetEventSlim SleepResetEvent = new ManualResetEventSlim();
//
//  /**
//   * The list of connections spawned by the server
//   */
//  private List<ProxyServerConnection> Connections;
//
//  public final List<ProxyServerConnection> getConnections() {
//    return Connections;
//  }
//
//  public final void setConnections(List<ProxyServerConnection> value) {
//    Connections = value;
//  }
//
//  /**
//   * Synchronization object on the list
//   */
//  private Object SyncRoot;
//
//  public final Object getSyncRoot() {
//    return SyncRoot;
//  }
//
//  public final void setSyncRoot(Object value) {
//    SyncRoot = value;
//  }
//
//  /**
//   * Gets the local port that is being listened on
//   */
//  private int LocalPort;
//
//  public final int getLocalPort() {
//    return LocalPort;
//  }
//
//  private void setLocalPort(int value) {
//    LocalPort = value;
//  }
//
//  /**
//   * Gets/Sets the remote end point to connect to
//   */
//  private IPEndPoint RemoteEndpoint;
//
//  public final IPEndPoint getRemoteEndpoint() {
//    return RemoteEndpoint;
//  }
//
//  public final void setRemoteEndpoint(IPEndPoint value) {
//    RemoteEndpoint = value;
//  }
//
//  /**
//   * Gets/Sets the listener
//   */
//  private TcpListener ListenerSocket;
//
//  protected final TcpListener getListenerSocket() {
//    return ListenerSocket;
//  }
//
//  protected final void setListenerSocket(TcpListener value) {
//    ListenerSocket = value;
//  }
//
//  /**
//   * Gets/Sets the listener thread
//   */
//  private Thread ListenerThread;
//
//  protected final Thread getListenerThread() {
//    return ListenerThread;
//  }
//
//  protected final void setListenerThread(Thread value) {
//    ListenerThread = value;
//  }
//
//  /**
//   * Delay incoming
//   */
//  private boolean SimulatedInDelay;
//
//  public final boolean getSimulatedInDelay() {
//    return SimulatedInDelay;
//  }
//
//  public final void setSimulatedInDelay(boolean value) {
//    SimulatedInDelay = value;
//  }
//
//  /**
//   * Delay outgoing
//   */
//  private boolean SimulatedOutDelay;
//
//  public final boolean getSimulatedOutDelay() {
//    return SimulatedOutDelay;
//  }
//
//  public final void setSimulatedOutDelay(boolean value) {
//    SimulatedOutDelay = value;
//  }
//
//  /**
//   * Simulated delay in milliseconds between each packet being written out. This simulates low
//   * bandwidth connection.
//   */
//  private int SimulatedPacketDelay;
//
//  public final int getSimulatedPacketDelay() {
//    return SimulatedPacketDelay;
//  }
//
//  private void setSimulatedPacketDelay(int value) {
//    SimulatedPacketDelay = value;
//  }
//
//  /**
//   * Size of Buffer
//   */
//  private int BufferSize;
//
//  public final int getBufferSize() {
//    return BufferSize;
//  }
//
//  public final void setBufferSize(int value) {
//    BufferSize = value;
//  }
//
//  /**
//   * Gets/Sets the flag whether the stop is requested
//   */
//  public final boolean getStopRequested() {
//    return _stopRequested;
//  }
//
//  public final void setStopRequested(boolean value) {
//    _stopRequested = value;
//  }
//
//  /**
//   * Set SimulatedPacketDelay
//   */
//  public final void SetSimulatedPacketDelay(int simulatedPacketDelay) {
//    Log("Setting SimulatedPacketDelay to: {0}", simulatedPacketDelay);
//    setSimulatedPacketDelay(simulatedPacketDelay);
//    SleepResetEvent.Reset();
//  }
//
//  /**
//   * Reset SimulatedPacketDelay
//   */
//  public final void ResetSimulatedPacketDelay() {
//    Log("Resetting SimulatedPacketDelay to: 0");
//    setSimulatedPacketDelay(0);
//    SleepResetEvent.Set();
//  }
//
//  /**
//   * Default constructor
//   */
//  public ProxyServer(int simulatedPacketDelay, boolean simulatedInDelay,
//      boolean simulatedOutDelay) {
//    this(simulatedPacketDelay, simulatedInDelay, simulatedOutDelay, 8192);
//  }
//
//  public ProxyServer(int simulatedPacketDelay, boolean simulatedInDelay) {
//    this(simulatedPacketDelay, simulatedInDelay, false, 8192);
//  }
//
//  public ProxyServer(int simulatedPacketDelay) {
//    this(simulatedPacketDelay, false, false, 8192);
//  }
//
//  public ProxyServer() {
//    this(0, false, false, 8192);
//  }
//
//  public ProxyServer(int simulatedPacketDelay, boolean simulatedInDelay, boolean simulatedOutDelay,
//      int bufferSize) {
//    setSyncRoot(new Object());
//    setConnections(new ArrayList<ProxyServerConnection>());
//    setSimulatedPacketDelay(simulatedPacketDelay);
//    setSimulatedInDelay(simulatedInDelay);
//    setSimulatedOutDelay(simulatedOutDelay);
//    setBufferSize(bufferSize);
//  }
//
//  /**
//   * Start the listener thread
//   */
//  public final void Start() {
//    setStopRequested(false);
//
//    Log("Starting the server...");
//
//    // Listen on any port
//    setListenerSocket(new TcpListener(new IPEndPoint(IPAddress.Any, 0)));
//    getListenerSocket().Start();
//
//    Log("Server is running on {0}...", getListenerSocket().LocalEndpoint);
//
//    setLocalPort(((IPEndPoint) getListenerSocket().LocalEndpoint).Port);
//
//    setListenerThread(new Thread()) {
//      void run () {
//        _RequestListener();
//      }
//    } ;
//    getListenerThread().Name = "Proxy Server Listener";
//    getListenerThread().start();
//  }
//
//  /**
//   * Stop the listener thread
//   */
//  public final void Stop() {
//    // Request the listener thread to stop
//    setStopRequested(true);
//
//    // Wait for termination
//    getListenerThread().join(1000);
//  }
//
//  public final void close() throws java.io.IOException {
//    Stop();
//  }
//
//  /**
//   * This method is used internally to notify server about the client disconnection
//   */
//  public final void NotifyClientDisconnection(ProxyServerConnection connection) {
//    synchronized (getSyncRoot()) {
//      // Remove the existing connection from the list
//      getConnections().remove(connection);
//    }
//  }
//
//  /**
//   * Processes all incoming requests
//   */
//  private void _RequestListener() {
//    try {
//      while (!getStopRequested()) {
//        if (getListenerSocket().Pending()) {
//          try {
//            Log("Connection received");
//
//            // Accept the connection
//            TcpClient newConnection = getListenerSocket().AcceptTcpClient();
//
//            //Log("Connection accepted");
//
//            // Start a new connection
//            ProxyServerConnection proxyConnection = new ProxyServerConnection(newConnection, this);
//            proxyConnection.Start();
//
//            // Registering new connection
//            synchronized (getSyncRoot()) {
//              getConnections().add(proxyConnection);
//            }
//          } catch (RuntimeException ex) {
//            Log(ex.toString());
//          }
//        } else {
//          // Pause a bit
//          Thread.sleep(10);
//        }
//      }
//    } catch (RuntimeException ex) {
//
//      Log(ex.toString());
//    }
//
//    Log("Stopping the server...");
//
//    // Stop the server
//    getListenerSocket().Stop();
//    setListenerSocket(null);
//
//    Log("Waiting for client connections to terminate...");
//
//    // Wait for connections
//    int connectionsLeft = Short.MAX_VALUE;
//    while (connectionsLeft > 0) {
//      synchronized (getSyncRoot()) {
//        // Check the amount of connections left
//        connectionsLeft = getConnections().size();
//      }
//
//      // Wait a bit
//      Thread.sleep(10);
//    }
//
//    Log("Server is stopped");
//  }
//
//  /**
//   * Write a string to the log
//   */
//  public final void Log(String text, Object... args) {
//    synchronized (getEventLog()) {
////TODO TASK: The '0:O' format specifier is not converted to Java:
//      getEventLog().append(String.format("[{0:O}]: ", java.time.LocalDateTime.now()));
//      getEventLog().AppendFormat(text, args);
//      getEventLog().append("\r\n");
//    }
//  }
//
//  /**
//   * Return the ProxyServer log
//   */
//  public final String GetServerEventLog() {
//    synchronized (getEventLog()) {
//      getEventLog().append(s_logTrailer);
//      return getEventLog().toString();
//    }
//  }
//
//  /**
//   * Get Server IPEndPoint
//   */
//  public static IPEndPoint GetServerIpEndpoint(IDbConnection conn) {
//    try (IDbCommand cmd = conn.CreateCommand()) {
//      cmd.CommandText =
//          "select local_net_address, local_tcp_port " + "from [sys].[dm_exec_connections] "
//              + "where local_net_address is not null and local_tcp_port is not null and session_id = @@spid";
//      try (IDataReader reader = cmd.ExecuteReader()) {
//        if (reader.Read()) {
//          String ipAddress = reader.GetString(0);
//          int port = reader.GetInt32(1);
//          return new IPEndPoint(IPAddress.Parse(ipAddress), port);
//        } else {
//          // No rows are non-null, so assume it is local
//          // Assume that we're listening on the default port
//          return new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1433);
//        }
//      }
//    }
//  }
//
//  /**
//   * Kills all currently open connections
//   *
//   * @param softKill If true will perform a shutdown before closing, otherwise close will happen
//   * with lingering disabled
//   */
//  public final void KillAllConnections() {
//    KillAllConnections(false);
//  }
//
//  public final void KillAllConnections(boolean softKill) {
//    synchronized (getSyncRoot()) {
//      for (ProxyServerConnection connection : getConnections()) {
//        connection.Kill(softKill);
//      }
//    }
//  }
}