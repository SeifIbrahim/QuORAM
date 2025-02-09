package Messages;

/**
 * Created by ajmagat on 8/10/16.
 */
public class MessageTypes {
	// Protocol
	public static final int CLIENT_REQUEST = 0;
	public static final int CLIENT_READ_REQUEST = 98;
	public static final int CLIENT_WRITE_REQUEST = 99;
	public static final int PROXY_READ_REQUEST = 1;
	public static final int PROXY_WRITE_REQUEST = 2;
	public static final int SERVER_RESPONSE = 3;
	public static final int PROXY_RESPONSE = 4;
	public static final int PROXY_INITIALIZE_REQUEST = 5;

	// For testing
	public static final int PRINT_SUBTREE = 11;

	// For profiling
	public static final int WRITE_STATS = 12;

	// The proxies need to know when load tests start and end so that we can reset
	// counters and stats and then log them at the end
	public static final int INIT_LOAD_TEST = 13;
	public static final int FINISH_LOAD_TEST = 14;
}
