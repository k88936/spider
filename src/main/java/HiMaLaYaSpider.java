import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;
import us.codecraft.webmagic.*;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.scheduler.DuplicateRemovedScheduler;
import us.codecraft.webmagic.scheduler.MonitorableScheduler;
import us.codecraft.webmagic.scheduler.component.DuplicateRemover;
import us.codecraft.webmagic.selector.Json;

import java.io.*;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class HiMaLaYaSpider implements PageProcessor {

    public static final String URL_MAIN = "https://www.ximalaya.com/revision/album/v1/getTracksList?albumId=30917322&pageNum=INDEX&sort=1";
    public static final String URL_HOME = "https://www.ximalaya.com";
    static int pageNum = 0;
    private final Site site = Site.me().setRetryTimes(3).setSleepTime(1000).setTimeOut(10000);

    public static void main(String[] args) throws IOException {


        MyConsolePipeline CP = new MyConsolePipeline(".\\data\\");

        Spider.create(new HiMaLaYaSpider())
                .setScheduler(new MyFileCacheQueueScheduler(".\\.cfg"))
                .addUrl(URL_MAIN.replaceFirst("INDEX", String.valueOf(++pageNum)))
                .addPipeline(CP)
                .thread(5)
                .run();

    }


    @Override
    public void process(Page page) {


        if (page.isDownloadSuccess()) {
            if (page.getUrl().regex("album").match()) {


                List list = page.getJson().jsonPath("data.tracks").nodes();

                if(list.isEmpty())
                {
                    return;
                }

                for (int i = 0; i < list.size(); i++) {

                    Json dataJson = new Json(list.get(i).toString());


                    page.addTargetRequest(URL_HOME + dataJson.jsonPath("url").toString());

                }

                page.addTargetRequest((URL_MAIN.replaceFirst("INDEX", String.valueOf(++pageNum))));


            } else {


                page.putField("title", page.getHtml().css("h1").toString().replaceAll("<h1 class=\"title-wrapper kn_\">|</h1>", ""));

                page.putField("time", page.getHtml().regex("(?<=<span class=\"time kn_\">).*?(?=</span>)").toString());
                page.putField("article", page.getHtml().css("article").css("span").all().toString().replaceAll("</span>|<span>|,", ""));

            }
        } else {


        }
    }

    @Override
    public Site getSite() {

        return site;
    }
}

class MyConsolePipeline implements Pipeline {


    String address;

    public MyConsolePipeline(String address) throws IOException {

        this.address = address;

    }

    @Override
    public void process(ResultItems resultItems, Task task) {


        Map<String, Object> map = resultItems.getAll();

        if (map.get("time") != null) {

            File file = new File(address + map.get("time").toString().substring(0, 10) + map.get("title").toString().replaceAll("\\p{Punct}", "") + ".txt");

            for (Map.Entry<String, Object> entry : resultItems.getAll().entrySet()) {


                try {

                    FileWriter writer = new FileWriter(file, true);

                    writer.write("\r\n" + entry.getValue());

                    writer.close();
                    System.out.println("finish");

                } catch (Exception ex) {

                    ex.printStackTrace();

                    ex.getMessage();

                }
            }
        }

    }


}





 class MyFileCacheQueueScheduler extends DuplicateRemovedScheduler implements MonitorableScheduler, Closeable {

    private String filePath = System.getProperty("java.io.tmpdir");

    private String fileUrlAllName = ".urls.txt";

    private Task task;

    private String fileCursor = ".cursor.txt";

    private PrintWriter fileUrlWriter;

    private PrintWriter fileCursorWriter;

    private AtomicInteger cursor = new AtomicInteger();

    private AtomicBoolean inited = new AtomicBoolean(false);

    private BlockingQueue<Request> queue;

    private Set<String> urls;

    private ScheduledExecutorService flushThreadPool;

    public MyFileCacheQueueScheduler(String filePath) {
        if (!filePath.endsWith("/") && !filePath.endsWith("\\")) {
            filePath += "/";
        }
        this.filePath = filePath;
        initDuplicateRemover();
    }

    private void flush() {
        fileUrlWriter.flush();
        fileCursorWriter.flush();
    }

    private void init(Task task) {
        this.task = task;
        File file = new File(filePath);
        if (!file.exists()) {
            file.mkdirs();
        }
        readFile();
        initWriter();
        initFlushThread();
        inited.set(true);
        logger.info("init cache scheduler success");
    }


     /*
     * 重新实现
     *
     * 加入筛选
     *
     *
     */

    private void initDuplicateRemover() {
        setDuplicateRemover(
                new DuplicateRemover() {
                    @Override
                    public boolean isDuplicate(Request request, Task task) {
                        if (!inited.get()) {
                            init(task);
                        }

                        if (request.getUrl().contains("album")) {
                            return false;
                        }
                        return !urls.add(request.getUrl());
                    }

                    @Override
                    public void resetDuplicateCheck(Task task) {
                        urls.clear();
                    }

                    @Override
                    public int getTotalRequestsCount(Task task) {
                        return urls.size();
                    }
                });
    }

    private void initFlushThread() {
        flushThreadPool = Executors.newScheduledThreadPool(1);
        flushThreadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }, 10, 10, TimeUnit.SECONDS);
    }

    private void initWriter() {
        try {
            fileUrlWriter = new PrintWriter(new FileWriter(getFileName(fileUrlAllName), true));
            fileCursorWriter = new PrintWriter(new FileWriter(getFileName(fileCursor), false));
        } catch (IOException e) {
            throw new RuntimeException("init cache scheduler error", e);
        }
    }

    private void readFile() {
        try {
            queue = new LinkedBlockingQueue<Request>();
            urls = new LinkedHashSet<String>();
            readCursorFile();
            readUrlFile();
            // initDuplicateRemover();
        } catch (FileNotFoundException e) {
            //init
            logger.info("init cache file " + getFileName(fileUrlAllName));
        } catch (IOException e) {
            logger.error("init file error", e);
        }
    }

    private void readUrlFile() throws IOException {
        String line;
        BufferedReader fileUrlReader = null;
        try {
            fileUrlReader = new BufferedReader(new FileReader(getFileName(fileUrlAllName)));
            int lineReaded = 0;
            while ((line = fileUrlReader.readLine()) != null) {
                urls.add(line.trim());
                lineReaded++;
                if (lineReaded > cursor.get()) {
                    queue.add(deserializeRequest(line));
                }
            }
        } finally {
            if (fileUrlReader != null) {
                IOUtils.closeQuietly(fileUrlReader);
            }
        }
    }

    private void readCursorFile() throws IOException {
        BufferedReader fileCursorReader = null;
        try {
            fileCursorReader = new BufferedReader(new FileReader(getFileName(fileCursor)));
            String line;
            //read the last number
            while ((line = fileCursorReader.readLine()) != null) {
                cursor = new AtomicInteger(NumberUtils.toInt(line));
            }
        } finally {
            if (fileCursorReader != null) {
                IOUtils.closeQuietly(fileCursorReader);
            }
        }
    }

    public void close() throws IOException {
        flushThreadPool.shutdown();
        fileUrlWriter.close();
        fileCursorWriter.close();
    }

    private String getFileName(String filename) {
        return filePath + task.getUUID() + filename;
    }

    @Override
    protected void pushWhenNoDuplicate(Request request, Task task) {
        if (!inited.get()) {
            init(task);
        }
        queue.add(request);
        fileUrlWriter.println(serializeRequest(request));
    }

    @Override
    public synchronized Request poll(Task task) {
        if (!inited.get()) {
            init(task);
        }
        fileCursorWriter.println(cursor.incrementAndGet());
        return queue.poll();
    }

    @Override
    public int getLeftRequestsCount(Task task) {
        return queue.size();
    }

    @Override
    public int getTotalRequestsCount(Task task) {
        return getDuplicateRemover().getTotalRequestsCount(task);
    }

    protected String serializeRequest(Request request) {
        return request.getUrl();
    }

    protected Request deserializeRequest(String line) {
        return new Request(line);
    }

}











