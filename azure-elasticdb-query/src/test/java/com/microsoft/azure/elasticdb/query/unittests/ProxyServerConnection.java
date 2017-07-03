package com.microsoft.azure.elasticdb.query.unittests;

/**
 * This class maintains the tunnel between incoming connection and outgoing connection
 */
public class ProxyServerConnection {
//
//  /**
//   * This is a processing thread
//   */
//  private Thread ProcessorThread;
//
//  protected final Thread getProcessorThread() {
//    return ProcessorThread;
//  }
//
//  protected final void setProcessorThread(Thread value) {
//    ProcessorThread = value;
//  }
//
//  /**
//   * Returns the proxy server this connection belongs to
//   */
//  private ProxyServer Server;
//
//  public final ProxyServer getServer() {
//    return Server;
//  }
//
//  public final void setServer(ProxyServer value) {
//    Server = value;
//  }
//
//  /**
//   * Incoming connection
//   */
//  private TcpClient IncomingConnection;
//
//  protected final TcpClient getIncomingConnection() {
//    return IncomingConnection;
//  }
//
//  protected final void setIncomingConnection(TcpClient value) {
//    IncomingConnection = value;
//  }
//
//  /**
//   * Outgoing connection
//   */
//  private TcpClient OutgoingConnection;
//
//  protected final TcpClient getOutgoingConnection() {
//    return OutgoingConnection;
//  }
//
//  protected final void setOutgoingConnection(TcpClient value) {
//    OutgoingConnection = value;
//  }
//
//  /**
//   * Standard constructor
//   */
//  public ProxyServerConnection(TcpClient incomingConnection, ProxyServer server) {
//    setIncomingConnection(incomingConnection);
//    setServer(server);
//  }
//
//  /**
//   * Runs the processing thread
//   */
//  public final void Start() {
//    setProcessorThread(new Thread()) {
//      void run () {
//        _ProcessorHandler();
//      }
//    } ;
//
//    System.Net.EndPoint tempVar = getIncomingConnection().Client.RemoteEndPoint;
//    IPEndPoint incomingIPEndPoint = (IPEndPoint) ((tempVar instanceof IPEndPoint) ? tempVar : null);
//    getProcessorThread().Name = String
//        .format("Proxy Server Connection %1$s Thread", incomingIPEndPoint);
//
//    getProcessorThread().start();
//  }
//
//  /**
//   * Handles the bidirectional data transfers
//   */
//  private void _ProcessorHandler() {
//    try {
//      getServer().Log("Connecting to {0}...", getServer().getRemoteEndpoint());
//      getServer().Log("Remote end point address family {0}",
//          getServer().getRemoteEndpoint().AddressFamily);
//
//      // Establish outgoing connection to the proxy
//      setOutgoingConnection(new TcpClient(getServer().getRemoteEndpoint().AddressFamily));
//      getOutgoingConnection().Connect(getServer().getRemoteEndpoint());
//
//      // Writing connection information
//      getServer().Log("Connection established");
//
//      // Obtain network streams
//      NetworkStream outStream = getOutgoingConnection().GetStream();
//      NetworkStream inStream = getIncomingConnection().GetStream();
//
//      // Tunnel the traffic between two connections
//      while (getIncomingConnection().Connected && getOutgoingConnection().Connected && !getServer()
//          .getStopRequested()) {
//        // Check incoming buffer
//        if (inStream.DataAvailable) {
//          CopyData(inStream, "client", outStream, "server", getServer().getSimulatedInDelay());
//        }
//
//        // Check outgoing buffer
//        if (outStream.DataAvailable) {
//          CopyData(outStream, "server", inStream, "client", getServer().getSimulatedOutDelay());
//        }
//
//        // Poll the sockets
//        if ((getIncomingConnection().Client.Poll(100, SelectMode.SelectRead)
//            && !inStream.DataAvailable) || (
//            getOutgoingConnection().Client.Poll(100, SelectMode.SelectRead)
//                && !outStream.DataAvailable)) {
//          break;
//        }
//
//        Thread.sleep(10);
//      }
//    } catch (RuntimeException ex) {
//      getServer().Log(ex.toString());
//    }
//
//    try {
//      // Disconnect the client
//      getIncomingConnection().Close();
//      getOutgoingConnection().Close();
//    } catch (RuntimeException e) {
//    }
//    // Logging disconnection message
//    getServer().Log("Connection closed");
//
//    // Notify parent
//    getServer().NotifyClientDisconnection(this);
//  }
//
//  private void CopyData(NetworkStream readStream, String readStreamName, NetworkStream writeStream,
//      String writeStreamName, boolean simulateDelay) {
//    // Note that the latency/bandwidth delay algorithm used here is a simple approximation used for test purposes
//
//    getServer()
//        .Log("Copying message from {0} to {1}, delay is {2}", readStreamName, writeStreamName,
//            simulateDelay);
//
//    // Read all available data from readStream
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: var outBytes = new List<Tuple<byte[], int>>();
//    ArrayList<Tuple<byte[], Integer>> outBytes = new ArrayList<Tuple<byte[], Integer>>();
//    while (readStream.DataAvailable) {
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: byte[] buffer = new byte[Server.BufferSize];
//      byte[] buffer = new byte[getServer().getBufferSize()];
//      int numBytes = readStream.Read(buffer, 0, buffer.length);
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: outBytes.Add(new Tuple<byte[], int>(buffer, numBytes));
//      outBytes.add(new Tuple<byte[], Integer>(buffer, numBytes));
//      getServer().Log("\tRead {0} bytes from {1}", numBytes, readStreamName);
//    }
//
//    // Write all data to writeStream
////WARNING: Unsigned integer types have no direct equivalent in Java:
////ORIGINAL LINE: foreach (var b in outBytes)
//    for (Tuple<byte[], Integer> b : outBytes) {
//      // Delay for bandwidth, only do the simulated packet delay if SleepResetEvent hasn't been set
//      if (simulateDelay && !getServer().SleepResetEvent.IsSet) {
//        getServer().Log("\tSleeping packet delay for {0}", getServer().getSimulatedPacketDelay());
//        getServer().SleepResetEvent.Wait(getServer().getSimulatedPacketDelay());
//      }
//
//      writeStream.Write(b.Item1, 0, b.Item2);
//      getServer().Log("\tWrote {0} bytes to {1}", b.Item2, writeStreamName);
//    }
//  }
//
//  /**
//   * Kills this connection
//   *
//   * @param softKill If true will perform a shutdown before closing, otherwise close will happen
//   * with lingering disabled
//   *
//   * See VSTS item 2672651. We've been encountering an issue in our lab dttp runs in which we hit an
//   * ObjectDisposed exception in the call to IncomingConnection.Client.Shutdown.  Per the msdn docs
//   * on that call, the ObjectDisposedException happens if the Socket has already been closed.  Since
//   * the point of this method is to close the socket, we'll just catch the ObjectDisposedException
//   * and ignore it. Note that the LingerState property can also throw an ObjectDisposedException if
//   * the Socket has already been closed, so we'll put the try-catch block around both code paths.
//   * Finally, note that the docuemntation for the Close call does not list an
//   * ObjectDisposedException as something that can be thrown if the Socket has already been closed,
//   * so we will leave those calls out of the try-catch block. If we start seeing
//   * ObjectDisposedExcptions coming from there then we can investigate further.  For now the goal is
//   * to be conservative in the error handling case so that we don't inadvertantly ignore exceptions
//   * that are happening for a different reason.
//   */
//  public final void Kill(boolean softKill) {
//    getServer().Log("About to kill a client connection.");
//    try {
//      if (softKill) {
//        // Soft close, do shutdown first
//        getIncomingConnection().Client.Shutdown(SocketShutdown.Both);
//        getServer().Log("Successfully issued a soft kill to client connection.");
//      } else {
//        // Hard close - force no lingering
//        getIncomingConnection().Client.LingerState = new LingerOption(true, 0);
//        getServer().Log("Successfully issued a hard kill to client connection.");
//      }
//    } catch (ObjectDisposedException ode) {
//      getServer()
//          .Log("Ignoring ObjectDisposedException while preparing to close a client connection.");
//      getServer().Log(
//          "Exception info:\n\t Message: {0} \n\t ObjectName: {1} \n\t Source: {2} \n\t StackTrace: {3}",
//          ode.getMessage(), ode.ObjectName, ode.Source, ode.StackTrace);
//    }
//
//    getIncomingConnection().Client.Close();
//    getServer().Log("Successfully closed incoming connection.");
//    getOutgoingConnection().Client.Close();
//    getServer().Log("Successfully closed outgoing connection.");
//  }
}
