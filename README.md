# azure-elastic-db-tools

# IDE
1. Use IntelliJ, install lombok plugin, enable "Auto Import on the fly" in Settings.

# TODO
1. Change method names and private variable names to camel case (Camel case is default java convention).
2. In some classes, private variables starts with underscore, remove the underscore.
3. Change Task to Callable.
4. Change java.io.Closeable to AutoClosable interface.
5. Under core module, you can delete most of logging related classes using Slf4j library.
6. Remove setter methods, it is against concept of immutable objects. A private variable can only be set via constructor.
7. Change Func to Function.
8. There is StopWatch class available in guava library.
