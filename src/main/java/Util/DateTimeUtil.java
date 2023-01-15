package Util;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;




public class DateTimeUtil {
    private final static DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Date => String
    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formater.format(localDateTime);
    }

    // String YmDHms => Long timeStamp
    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formater);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

    // timeStamp => String YmDHms
    public static String todtString(Long ts){
        DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.ofInstant(new Date(ts).toInstant(), ZoneId.systemDefault());
        return formater.format(localDateTime);
    }

    // timeStamp => String YmDHms
    public static String todtStringV2(Long ts){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(ts));
    }

    public static void main(String[] args) {
        // SimpleDateFormat 与 DateTimeFormatter 区别？后者线程安全，支持LocalDateTime或ZonedDateTime
        Long time = 1527816283000L;
        System.out.println(todtStringV2(time));
        System.out.println(todtStringV2(time));
    }
}

