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

  private static char[] CHARS = "abcdefghijklmnopqrstuvwxyz"
      .toCharArray();

  private static char[] SHORT_CHARS = "abcd".toCharArray();

  private static int ARRAY_MAX_LENGTH = 5;

  private static City[] cities = {
      new City("北京市", "北京市"),
      new City("天津市", "天津市"),
      new City("重庆市", "重庆市"),
      new City("广东省", "广州市"),
      new City("广东省", "深圳市"),
      new City("辽宁省", "沈阳市"),
      new City("辽宁省", "大连市"),
      new City("江苏省", "南京市"),
      new City("江苏省", "苏州市"),
      new City("湖北省", "武汉市"),
      new City("湖北省", "黄石市"),
      new City("四川省", "成都市"),
      new City("四川省", "自贡市"),
      new City("陕西省", "西安市"),
      new City("陕西省", "铜川市"),
      new City("河北省", "石家庄市"),
      new City("河北省", "唐山市"),
      new City("山西省", "太原市"),
      new City("山西省", "大同市"),
      new City("广东省", "汕头市"),
      new City("广东省", "韶关市"),
      new City("辽宁省", "丹东市"),
      new City("辽宁省", "锦州市"),
      new City("辽宁省", "阜新市"),
      new City("辽宁省", "葫芦岛市"),
      new City("江苏省", "盐城市"),
      new City("江苏省", "宿迁市"),
      new City("浙江省", "嘉兴市"),
      new City("浙江省", "湖州市"),
      new City("福建省", "福州市"),
      new City("福建省", "厦门市"),
      new City("广西壮族自治区", "柳州"),
      new City("广西壮族自治区", "梧州"),
      new City("广西壮族自治区", "玉林"),
      new City("贵州省", "六盘水市"),
      new City("贵州省", "安顺市"),
      new City("贵州省", "黔南布依族苗族自治州"),
      new City("云南省", "昆明市"),
      new City("西藏自治区", "拉萨市"),
      new City("西藏自治区", "阿里地区"),
      new City("甘肃省", "兰州市"),
      new City("甘肃省", "天水市"),
      new City("甘肃省", "陇南市"),
      new City("青海省", "西宁市"),
      new City("新疆维吾尔族自治区", "乌鲁木齐市"),
      new City("新疆维吾尔族自治区", "克拉玛依市"),
      new City("新疆维吾尔族自治区", "阿拉尔市"),
      new City("新疆维吾尔族自治区", "阿拉尔市")
  };

  private static String[] items = {
      "AAA", "BBB", "CCC", "DDD", "EEE",
      "FFF", "GGG", "HHH", "III", "JJJ",
      "KKK", "LLL", "MMM", "NNN", "OOO",
      "PPP", "QQQ", "RRR", "SSS", "TTT",
      "UUU", "VVV", "WWW", "XXX", "YYY",
      "ZZZ", "aa", "bb", "cc", "dd", "ee",
      "ff", "gg", "hh", "ii", "jj", "kk",
      "ll", "mm", "nn", "oo", "pp", "qq",
      "rr", "ss", "tt", "uu", "vv", "ww",
      "xx", "yy", "zz", "abc", "def", "ghi",
      "jkl", "mno", "pqr", "stu", "vw", "xyz"
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
          return randomStringValue(ThreadLocalRandom.current().nextInt(10));
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
