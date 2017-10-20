import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

File file = new File("./log-${LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy_HH-mm-ss-SSS"))}")

(1..1000).each {file << (LogLine.getLog())}

class LogLine {
    private static final AtomicLong counter = new AtomicLong(0)
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z")
    private static final Random random = new Random()

    static String getLog() {
        "${getIP()} - - [${getDate()}] \"${getRequest()} ${getPath()} HTTP/1.1\" ${getCode()} ${getBytes()} \"-\" \"${getUserAgent()}\"\n"
    }

    static String getIP() {
        Random r = new Random()
        return r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256)
    }

    static int uid() {
        return (int) uidL()
    }

    static long uidL() {
        return counter.incrementAndGet() % Integer.MAX_VALUE
    }

    static String getDate() {
        return ZonedDateTime.now().plusMinutes(uid()).format(formatter)
    }

    static String getRequest() {
        def requests = ["GET","POST","DELETE","PUT"]
        return requests[uid() % requests.size()]
    }

    static String getPath() {
        def paths = ["/list","/wp-content","/wp-admin","/explore","/search/tag/list","/app/main/posts","/posts/posts/explore","/apps/cart.jsp?appID="]
        return paths[uid() % paths.size()]
    }

    static String getCode() {
        def response = ["200","404","500","301"]
        return response[uid() % response.size()]
    }

    static String getBytes() {
        return String.valueOf((int)random.nextGaussian() * 1000)
    }

    static String getUserAgent() {
        def agents = ["Firefox/5.0 Linux", "Opera/2.1 Windows", "Mozilla/10.0 Android",  "Safari/6.6 iPad"]
        return agents[uid() % agents.size()]
    }
}

