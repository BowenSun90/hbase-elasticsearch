package com.alex.space.common;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

/**
 * 生成Key和value
 *
 * @author Alex Created by Alex on 2018/9/9.
 */
@Slf4j
public class KeyValueGenerator {

  private static char[] CHARS = "abcdefghijklmnopqrstuvwxyz1234567890"
      .toCharArray();

  private static char[] SHORT_CHARS = "abcde".toCharArray();

  private static int ARRAY_MAX_LENGTH = 5;

  private static City[] cities = {
      new City("北京市", "北京市"), new City("天津市", "天津市"),
      new City("重庆市", "重庆市"), new City("上海市", "上海市"),
      new City("广东省", "广州市"), new City("广东省", "深圳市"),
      new City("广东省", "汕头市"), new City("广东省", "韶关市"),
      new City("广东省", "佛山市"), new City("广东省", "湛江市"),
      new City("广东省", "茂名市"), new City("辽宁省", "沈阳市"),
      new City("辽宁省", "大连市"), new City("辽宁省", "抚顺市"),
      new City("辽宁省", "抚顺市"), new City("辽宁省", "丹东市"),
      new City("辽宁省", "锦州市"), new City("辽宁省", "阜新市"),
      new City("辽宁省", "葫芦岛市"), new City("江苏省", "南京市"),
      new City("江苏省", "苏州市"), new City("江苏省", "常州市"),
      new City("江苏省", "连云港市"), new City("江苏省", "盐城市"),
      new City("湖北省", "武汉市"), new City("湖北省", "黄石市"),
      new City("湖北省", "宜昌市"), new City("四川省", "成都市"),
      new City("四川省", "自贡市"), new City("四川省", "德阳市"),
      new City("陕西省", "西安市"), new City("陕西省", "铜川市"),
      new City("陕西省", "延安市"), new City("陕西省", "汉中市"),
      new City("河北省", "石家庄市"), new City("河北省", "唐山市"),
      new City("河北省", "秦皇岛市"), new City("河北省", "邯郸市"),
      new City("山西省", "太原市"), new City("山西省", "大同市"),
      new City("山西省", "长治市"), new City("山西省", "晋中市"),
      new City("河南省", "郑州市"), new City("河南省", "开封市"),
      new City("河南省", "洛阳市"), new City("河南省", "安阳市"),
      new City("河南省", "新乡市"), new City("吉林省", "长春市"),
      new City("吉林省", "吉林市"), new City("黑龙江省", "哈尔滨市"),
      new City("黑龙江省", "齐齐哈尔市"), new City("黑龙江省", "鸡西市"),
      new City("黑龙江省", "鹤岗市"), new City("内蒙古", "呼和浩特市"),
      new City("内蒙古", "包头市"), new City("江苏省", "盐城市"),
      new City("江苏省", "宿迁市"), new City("浙江省", "嘉兴市"),
      new City("浙江省", "湖州市"), new City("福建省", "福州市"),
      new City("福建省", "厦门市"), new City("广西壮族自治区", "柳州"),
      new City("广西壮族自治区", "梧州"), new City("广西壮族自治区", "玉林"),
      new City("贵州省", "六盘水市"), new City("贵州省", "安顺市"),
      new City("贵州省", "黔南布依族苗族自治州"), new City("云南省", "昆明市"),
      new City("西藏自治区", "拉萨市"), new City("西藏自治区", "阿里地区"),
      new City("甘肃省", "兰州市"), new City("甘肃省", "天水市"),
      new City("甘肃省", "陇南市"), new City("青海省", "西宁市"),
      new City("新疆维吾尔族自治区", "乌鲁木齐市"),
      new City("新疆维吾尔族自治区", "克拉玛依市"),
      new City("新疆维吾尔族自治区", "阿拉尔市"),
      new City("新疆维吾尔族自治区", "阿拉尔市")
  };

  private static String[] items = {
      "AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG", "HHH", "III", "JJJ",
      "KKK", "LLL", "MMM", "NNN", "OOO", "PPP", "QQQ", "RRR", "SSS", "TTT",
      "UUU", "VVV", "WWW", "XXX", "YYY", "ZZZ", "aa", "bb", "cc", "dd", "ee",
      "ff", "gg", "hh", "ii", "jj", "kk", "ll", "mm", "nn", "oo", "pp", "qq",
      "rr", "ss", "tt", "uu", "vv", "ww", "xx", "yy", "zz", "abc", "def", "ghi",
      "jkl", "mno", "pqr", "stu", "vw", "xyz", "Jacob",
      "雅各布", "Michael", "迈克尔", "Ethan", "伊桑", "Joshua", "乔舒亚",
      "Alexander", "亚历山大", "Anthony", "安东尼", "William", "威廉", "Christopher", "克里斯托弗",
      "Jayden", "杰顿", "Andrew", "安德鲁", "Joseph", "约瑟夫", "David", "大卫",
      "Noad", "诺阿", "Aiden", "艾丹", "James", "詹姆斯", "Ryan", "赖恩", "Logan", "洛根",
      "John", "约翰", "Nathan", "内森", "Elijah", "伊莱贾", "Christian", "克里斯琴",
      "Gabriel", "加布里尔", "Benjamin", "本杰明", "Jonathan", "乔纳森", "Tyler", "泰勒",
      "Samuel", "塞缪尔", "Nicholas", "尼古拉斯", "Gavin", "加文", "Dylan", "迪兰",
      "Jackson", "杰克逊", "Brandon", "布兰顿", "Caleb", "凯勒布", "Brandon", "布兰顿",
      "Mason", "梅森", "Angel", "安吉尔", "Isaac", "艾萨克", "Evan", "埃文",
      "Jack", "杰克", "Kevin", "凯文", "Jose", "乔斯", "Isaiah", "艾塞亚", "Luke", "卢克",
      "Landon", "兰登", "Justin", "贾斯可", "Lucas", "卢卡斯", "Zachary", "扎克里",
      "Jordan", "乔丹", "Robert", "罗伯特", "Aaron", "艾伦", "Brayden", "布雷登",
      "Thomas", "托马斯", "Cameron", "卡梅伦", "Hunter", "亨特", "Austin", "奥斯汀",
      "Adrian", "艾德里安", "Connor", "康纳", "Owen", "欧文", "Aidan", "艾丹",
      "Jason", "贾森", "Julian", "朱利安", "Wyatt", "怀亚特", "Charles", "查尔斯",
      "Luis", "路易斯", "Carter", "卡特", "Juan", "胡安", "Chase", "蔡斯",
      "Diego", "迪格", "Jeremiah", "杰里迈亚", "Brody", "布罗迪", "Zavier", "泽维尔",
      "Adam", "亚当", "Liam", "利亚姆", "Hayden", "海顿", "Jesus", "杰西斯", "Ian", "伊恩",
      "Tristan", "特里斯坦", "Sean", "肖恩", "Cole", "科尔", "Alex", "亚历克斯",
      "Eric", "埃里克", "Brian", "布赖恩", "Jaden", "杰登", "Carson", "卡森", "Blake", "布莱克",
      "Ayden", "艾登", "Cooper", "库珀", "Dominic", "多米尼克", "Brady", "布雷迪",
      "Caden", "凯登", "Josiah", "乔塞亚", "Kyle", "凯尔", "Colton", "克尔顿", "Kaden", "凯登", "Eli", "伊莱"
  };


  /**
   * 保留两位小数
   */
  private static DecimalFormat df = new DecimalFormat("#.00");

  /**
   * 生成Column name <p> 每种类型生成100列，列名：数据类型+num
   *
   * @param dataTypeEnum 数据类型
   */
  public static String randomKey(DataTypeEnum dataTypeEnum) {
    // Json格式数据生成10列，以免数据列过多，导入elastic search有问题
    int num = dataTypeEnum == DataTypeEnum.Json
        ? ThreadLocalRandom.current().nextInt(CommonConstants.JSON_MAX_NUMBER)
        : ThreadLocalRandom.current().nextInt(CommonConstants.COLUMN_MAX_NUMBER);
    return dataTypeEnum.getKeyName() + num;
  }

  public static String generateKey(DataTypeEnum dataTypeEnum, int num) {
    return dataTypeEnum.getKeyName() + num;
  }

  public static String[] randomStringArray() {
    String[] array = new String[ThreadLocalRandom.current().nextInt(ARRAY_MAX_LENGTH)];
    for (int i = 0; i < array.length; i++) {
      array[i] = items[ThreadLocalRandom.current().nextInt(items.length)];
    }
    return array;
  }

  public static double[] randomNumberArray() {

    double[] array = new double[ThreadLocalRandom.current().nextInt(ARRAY_MAX_LENGTH)];
    for (int i = 0; i < array.length; i++) {
      array[i] = randomNumberValue();
    }
    return array;
  }

  public static boolean[] randomBoolArray() {

    boolean[] array = new boolean[ThreadLocalRandom.current().nextInt(ARRAY_MAX_LENGTH)];
    for (int i = 0; i < array.length; i++) {
      array[i] = randomBoolValue();
    }
    return array;
  }

  /**
   * 产生一个给定长度的随机字符串
   */
  public static String randomStringValue(int numItems) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numItems; i++) {
      sb.append(CHARS[ThreadLocalRandom.current().nextInt(CHARS.length)]);
    }
    return sb.toString();
  }

  /**
   * 产生一个给定长度的字符串
   */
  public static String randomStringValue(int numItems, char[] chars) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numItems; i++) {
      sb.append(chars[ThreadLocalRandom.current().nextInt(chars.length)]);
    }
    return sb.toString();
  }

  /**
   * 随机Bool <p> 0~10000的随机数
   */
  public static double randomNumberValue() {

    String number = df.format(ThreadLocalRandom.current().nextDouble(10000));
    return Double.parseDouble(number);
  }

  /**
   * 随机Bool
   */
  public static boolean randomBoolValue() {
    return ThreadLocalRandom.current().nextInt(10) % 2 == 0;
  }

  /**
   * 随机日期 <p> 距离now() 0～30天的日期
   */
  public static Date randomDateValue() {
    LocalDateTime localDateTime = LocalDateTime.now()
        .minusDays(ThreadLocalRandom.current().nextInt(30));

    ZoneId zone = ZoneId.systemDefault();
    Instant instant = localDateTime.atZone(zone).toInstant();
    return Date.from(instant);
  }

  /**
   * 根据数据类型，随机生成数据
   *
   * @param dataTypeEnum 数据类型
   */
  public static String randomValue(DataTypeEnum dataTypeEnum) {
    try {

      switch (dataTypeEnum) {
        case String:
          return randomStringValue(ThreadLocalRandom.current().nextInt(1, 20));
        case Number:
          return String.valueOf(randomNumberValue());
        case Bool:
          return String.valueOf(randomBoolValue());
        case Date:
          return String.valueOf(randomDateValue().getTime());
        case StringArray:
          JSONArray strArray = new JSONArray();
          for (int i = 0; i < ThreadLocalRandom.current().nextInt(ARRAY_MAX_LENGTH); i++) {
            strArray.put(items[ThreadLocalRandom.current().nextInt(items.length)]);
          }
          return strArray.toString();
        case NumberArray:
          JSONArray numArray = new JSONArray();
          for (int i = 0; i < ThreadLocalRandom.current().nextInt(ARRAY_MAX_LENGTH); i++) {
            numArray.put(randomNumberValue());
          }
          return numArray.toString();
        case BoolArray:
          JSONArray boolArray = new JSONArray();
          for (int i = 0; i < ThreadLocalRandom.current().nextInt(ARRAY_MAX_LENGTH); i++) {
            boolArray.put(randomBoolValue());
          }
          return boolArray.toString();
        case Json:
          City city = cities[ThreadLocalRandom.current().nextInt(cities.length)];
          JSONObject jsonObject = new JSONObject();
          jsonObject.put("value", randomStringValue(10, CHARS));
          jsonObject.put("province", city.province);
          jsonObject.put("city", city.city);
          return jsonObject.toString();
        default:
          return randomStringValue(10);
      }
    } catch (Exception e) {
      log.error("randomValue with exception: {}, type: {}", e.getMessage(), dataTypeEnum);
    }
    return "";
  }

  @AllArgsConstructor
  static class City {

    private String province;

    private String city;
  }

}
