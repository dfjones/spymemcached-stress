package org.sugis.memcache;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.joda.time.Duration;
import com.google.common.collect.ImmutableList;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;

public class MemcacheStress implements Runnable {
	private static final Random R = new Random();
	private static final List<String> KEY_LIST =
		Collections.synchronizedList(new ArrayList<String>());
  private static final List<String> TINY_KEY_LIST =
    Collections.synchronizedList(new ArrayList<String>());
	private static long shutdownTime;
	private static final MemcachedClient MC;
  private static final String LARGE_STRING = genString(19);

  boolean tiny = false;

  public MemcacheStress(boolean tiny) {
    this.tiny = tiny;
  }

	static {
		try {
			MC = new MemcachedClient(new DefaultConnectionFactory(),
					ImmutableList.of(
						new InetSocketAddress(
							Inet4Address.getByName("127.0.0.1"), 11211)));
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}


    public static void main( String[] args ) {
    	shutdownTime = System.currentTimeMillis() + Duration.standardDays(2).getMillis();

      int half = Runtime.getRuntime().availableProcessors() / 2;
    	for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
        new Thread(new MemcacheStress(i < half)).start();
        //new Thread(new MemcacheStress(true)).start();
      }

    }

    @Override
	public void run() {
    	try {
          if (!tiny) {
            System.out.println("Start large...");
            runLarge();
          }
          else {
            System.out.println("Start tiny...");
            runTiny();
          }
    	} catch (IOException e) {
    		e.printStackTrace();
    		throw new RuntimeException(e);
    	}
    }

	public void runLarge() throws IOException {
		long gets = 0;
		long sets = 0;
		while (System.currentTimeMillis() < shutdownTime) {
      String key = "" + R.nextLong();
      String value = LARGE_STRING;
      MC.set(key, (int) Duration.standardDays(1).getStandardSeconds(), value);
      KEY_LIST.add(key);

			String rkey = KEY_LIST.get(R.nextInt(KEY_LIST.size()));
			Object out = MC.get(rkey);
      if (out == null) {
        System.out.println("Null response");
        System.exit(0);
      }
			gets++;
		}
		System.out.println(gets + " gets and " + sets + " sets");
		System.exit(0);
	}

  public void runTiny() throws IOException {
    while(System.currentTimeMillis() < shutdownTime) {
      String key = "" + R.nextLong();
      Long value = R.nextLong();
      MC.set(key, (int) Duration.standardHours(1).getStandardSeconds(), value);
      TINY_KEY_LIST.add(key);

      String rkey = TINY_KEY_LIST.get(R.nextInt(TINY_KEY_LIST.size()));
      Object out = MC.get(rkey);
    }

    System.exit(0);
  }

  public static String genString(int mb) {
    // Generates a string with mb of content
    int size = mb * 1024 * 1024;
    byte[] buff = new byte[size];
    R.nextBytes(buff);
    return new String(buff);
  }
}
