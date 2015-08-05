package tool;

import backtype.storm.Config;
import redis.clients.jedis.Jedis;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.bytedeco.javacpp.opencv_highgui.cvLoadImage;
import static topology.StormConfigManager.*;

/**
 * Created by Tom Fu on Mar 3, 2015
 * For moving contents among different queues with rate control mechanism
 */
public class RedisInRedisOutBArrSender {

    private String host;
    private int port;
    private byte[] qin;
    private byte[] qout;
    boolean toPop;

    public RedisInRedisOutBArrSender(String confile) throws FileNotFoundException {

        Config conf = readConfig(confile);
        this.host = getString(conf, "redis.host");
        this.port = getInt(conf, "redis.port");
        this.qin = getString(conf, "riro-qin").getBytes();
        this.qout = getString(conf, "riro-qout").getBytes();
    }

    public RedisInRedisOutBArrSender(String confile, String qIn, String qOut, boolean toPop) throws FileNotFoundException {
        this(confile);
        this.qin = qIn.getBytes();
        this.qout = qOut.getBytes();
        this.toPop = toPop;
    }

    public void send2Queue(int st, int end, int fps) throws IOException {
        Jedis jedis = new Jedis(host, port);
        int generatedFrames = st;
        int targetCount = end - st;

        try {
            long start = System.currentTimeMillis();
            long last = start;
            long qinLen = 0;
            long qoutLen = 0;

            while (generatedFrames < targetCount) {

                byte[] data =  this.toPop ? jedis.lpop(this.qin) : jedis.lrange(this.qin, generatedFrames, generatedFrames+1).get(0);
                jedis.rpush(this.qout, data);

                generatedFrames ++;
                if (generatedFrames % fps == 0) {
                    long current = System.currentTimeMillis();
                    long elapse = current - last;
                    long remain = 1000 - elapse;
                    if (remain > 0) {
                        Thread.sleep(remain);
                    }
                    last = System.currentTimeMillis();
                    qinLen = jedis.llen(this.qin);
                    qoutLen = jedis.llen(this.qout);
                    System.out.println("Current: " + last + ", elapsed: " + (last - start)
                            + ",totalSend: " + generatedFrames+ ", remain: " + remain + ", qinLen: " + qinLen + ", qoutLen: " + qoutLen);
                }
            }

        } catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.out.println("usage: ImageSender <confFile> qinName qoutName toPop? <st> <end> <fps>");
            return;
        }
        RedisInRedisOutBArrSender sender = new RedisInRedisOutBArrSender(args[0], args[1], args[2], Boolean.parseBoolean(args[3]));
        System.out.println("start sender, qin: " + sender.qin + ", qout: " + sender.qout + ", toPop: " + sender.toPop);
        sender.send2Queue(Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]));
        System.out.println("end sender");
    }

}
