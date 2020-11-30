package examples.datasource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;

public class SocketValuesDataSource {

    private static final StringBuilder stringBuilder = new StringBuilder();
    private static final Random random = new Random();


    public static void main(String[] args) throws IOException {
        final ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", 9090));

        try (SocketChannel socketChannel = serverSocket.accept()) {
            System.out.println("New Connection: " + socketChannel.getLocalAddress().toString());
            final ByteBuffer bb = ByteBuffer.allocateDirect(1024);

            int counter = 0;
            while (true) {
                final int value = random.nextInt(1000);
                final int key = random.nextInt(2);
                System.out.println(key + ", " + value);
                final byte[] data = createData(key, value);

                bb.clear();
                bb.put(data);
                bb.flip();
                socketChannel.write(bb);

                counter++;
                Thread.sleep(50);

                if (counter % 10 == 0) {
                    Thread.sleep(10000);
                }
            }

        } catch (IOException ex) {
            System.out.println("Connection closed.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            serverSocket.close();
        }
    }

    private static byte[] createData(int key,int value) {
        stringBuilder.setLength(0);
        stringBuilder
                .append(key)
                .append(",")
                .append(value)
                .append("|");

        return stringBuilder.toString().getBytes();
    }
}
