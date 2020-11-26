package examples.datasource;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;

public class SocketTransactionDataSource {

    private static final StringBuilder stringBuilder = new StringBuilder();
    private static final Random random = new Random();
    private static final String[] items = new String[] {
            "PC",
            "TV",
            "Phone",
            "Tablet",
            "Headphones",
            "Mouse",
            "Keyboard"
    };

    public static void main(String[] args) throws IOException {
        final ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", 9090));

        try (SocketChannel socketChannel = serverSocket.accept()) {
            System.out.println("New Connection: " + socketChannel.getLocalAddress().toString());
            final ByteBuffer bb = ByteBuffer.allocateDirect(1024);

            while (true) {
                final long timestamp = System.currentTimeMillis();
                final int price = random.nextInt(1000);
                final String item = items[random.nextInt(items.length)];

                final byte[] data = createData(price, item, timestamp);

                bb.clear();
                bb.put(data);
                bb.flip();
                socketChannel.write(bb);

                Thread.sleep(50);
            }

        } catch (IOException ex) {
            System.out.println("Connection closed.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            serverSocket.close();
        }
    }

    private static byte[] createData(int price, String item, long timestamp) {
        stringBuilder.setLength(0);
        stringBuilder
                .append(timestamp)
                .append(",")
                .append(item)
                .append(",")
                .append(price)
                .append("|");

        return stringBuilder.toString().getBytes();
    }
}
