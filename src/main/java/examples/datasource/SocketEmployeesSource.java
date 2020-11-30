package examples.datasource;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Random;
import java.util.UUID;

public class SocketEmployeesSource {
    private static final StringBuilder stringBuilder = new StringBuilder();
    private static final Random random = new Random();

    private static final String[] departments = new String[] {
            "Purchase",
            "Finance",
            "Marketing",
            "Production",
            "Security",
            "Algo"
    };

    public static void main(String[] args) throws IOException {
        final ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", 9090));

        try (SocketChannel socketChannel = serverSocket.accept()) {
            System.out.println("New Connection: " + socketChannel.getLocalAddress().toString());
            final ByteBuffer bb = ByteBuffer.allocateDirect(1024);

            while (true) {
                final String department = departments[random.nextInt(departments.length)];
                final String employeeUuid = UUID.randomUUID().toString();

                System.out.println(employeeUuid + ", " + department);
                final byte[] data = createData(employeeUuid, department);

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

    private static byte[] createData(String employeeId, String department) {
        stringBuilder.setLength(0);
        stringBuilder
                .append(employeeId)
                .append(",")
                .append(department)
                .append("|");

        return stringBuilder.toString().getBytes();
    }
}
