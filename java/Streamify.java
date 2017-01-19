import java.net.*;
import java.io.*;

public class Streamify extends Thread {
	private ServerSocket serverSocket;
	private volatile Thread blinker;
	private String filename;
	private int lines_per_sec = 1000;

	public Streamify(String filename, int port, int rate) throws IOException {
		serverSocket = new ServerSocket(port);
		this.filename = filename;
		this.lines_per_sec = rate;
	}

	public void start() {
		blinker = new Thread(this);
		blinker.start();
	}

	public void stop_thread() {
		blinker = null;
	}

	public void run() {
		Thread thisThread = Thread.currentThread();
		while (blinker == thisThread) {
			try {
				System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
				Socket server = serverSocket.accept();
				System.out.println("Connected to " + server.getRemoteSocketAddress());

				DataOutputStream out = new DataOutputStream(server.getOutputStream());

				BufferedReader br = new BufferedReader(new FileReader(this.filename));
				int count = 0;
				for (String line; (line = br.readLine()) != null;) {
					out.writeBytes(line + "\n");
					System.out.println(String.valueOf(count) + ": " + line);
					Thread.sleep(1000 / this.lines_per_sec);
					count++;

				}
				System.out.println(
						"Streaming has been finished. Closing connection to " + server.getRemoteSocketAddress());
				br.close();
				server.close();
			} catch (SocketTimeoutException s) {
				System.out.println("Socket timed out!");
				break;
			} catch (IOException e) {
				System.out.println("There is no active connection");
				// e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
				break;
			}
		}
	}

	public static void main(String[] args) {
		if (args.length != 3) {
			System.out.println("Usage: $ java Streamify <file name> <port number> <lines per seconds>");
			System.exit(NORM_PRIORITY);
		}
		String filename = args[0];
		int port = Integer.parseInt(args[1]);
		int rate = Integer.parseInt(args[2]);

		Streamify t = null;
		try {
			t = new Streamify(filename, port, rate);
			t.start();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (t != null && t.isAlive()) {
				t.stop_thread();
			}

		}
	}
}
