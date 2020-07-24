# jumbo-java
Java client for the jumbo database, to connect via TCP to a standalone Jumbo server.

Example basic usage (for a Jumbo located on localhost of course):

```
JumboTcpClient client = JumboTcpClient.connect(Inet4Address.getLocalHost(), 8080);
JumboTcp sender = client.blockUntilConnected();
int table = 99;
sender.put(table, "Hello".getBytes(), "World!".getBytes());
byte[] ret = sender.get(table, "Hello".getBytes());
```

Using the type registry:

```
JumboTcpClient client = JumboTcpClient.connect(Inet4Address.getLocalHost(), 8080);
JumboTcp sender = client.blockUntilConnected();
int table = 99;
Codec<TradeKey, Trade> codec = getTradeCodec();
CodecRegistry.registerCodec(table, codec);
TradeKey key = getTradeKey();
Trade value = getTrade();
sender.put(table, key, value);
Trade ret = (Trade) sender.get(table, key);
```
