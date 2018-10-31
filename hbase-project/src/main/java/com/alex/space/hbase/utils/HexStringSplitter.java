package com.alex.space.hbase.utils;

import java.math.BigInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

public final class HexStringSplitter {

  final static String DEFAULT_MIN_HEX = "00000000";
  final static String DEFAULT_MAX_HEX = "FFFFFFFF";

  private static String firstRow = DEFAULT_MIN_HEX;
  private static BigInteger firstRowInt = BigInteger.ZERO;
  private static String lastRow = DEFAULT_MAX_HEX;
  private static BigInteger lastRowInt = new BigInteger(lastRow, 16);
  private static int rowComparisonLength = lastRow.length();

  /**
   * split with 2 region number
   */
  public static byte[] split(byte[] start, byte[] end) {
    BigInteger s = convertToBigInteger(start);
    BigInteger e = convertToBigInteger(end);
    return convertToByte(split2(s, e));
  }

  public static byte[][] split(int n) {
    // +1 to range because the last row is inclusive
    BigInteger range = lastRowInt.subtract(firstRowInt).add(BigInteger.ONE);
    BigInteger[] splits = new BigInteger[n - 1];
    BigInteger sizeOfEachSplit = range.divide(BigInteger.valueOf(n));
    for (int i = 1; i < n; i++) {
      // NOTE: this means the last region gets all the slop.
      // This is not a big deal if we're assuming n << MAXHEX
      splits[i - 1] = firstRowInt.add(sizeOfEachSplit.multiply(BigInteger
          .valueOf(i)));
    }
    return convertToBytes(splits);
  }

  public static byte[] firstRow() {
    return convertToByte(firstRowInt);
  }

  public static byte[] lastRow() {
    return convertToByte(lastRowInt);
  }

  /**
   * Divide 2 numbers in half (for split algorithm)
   */
  public static BigInteger split2(BigInteger a, BigInteger b) {
    return a.add(b).divide(BigInteger.valueOf(2)).abs();
  }

  /**
   * Returns an array of bytes corresponding to an array of BigIntegers
   *
   * @param bigIntegers numbers to convert
   * @return bytes corresponding to the bigIntegers
   */
  public static byte[][] convertToBytes(BigInteger[] bigIntegers) {
    byte[][] returnBytes = new byte[bigIntegers.length][];
    for (int i = 0; i < bigIntegers.length; i++) {
      returnBytes[i] = convertToByte(bigIntegers[i]);
    }
    return returnBytes;
  }

  /**
   * Returns the bytes corresponding to the BigInteger
   */
  public static byte[] convertToByte(BigInteger bigInteger, int pad) {
    String bigIntegerString = bigInteger.toString(16);
    bigIntegerString = StringUtils.leftPad(bigIntegerString, pad, '0');
    return Bytes.toBytes(bigIntegerString);
  }

  /**
   * Returns the bytes corresponding to the BigInteger
   */
  public static byte[] convertToByte(BigInteger bigInteger) {
    return convertToByte(bigInteger, rowComparisonLength);
  }

  /**
   * Returns the BigInteger represented by the byte array
   */
  public static BigInteger convertToBigInteger(byte[] row) {
    return (row.length > 0) ? new BigInteger(Bytes.toString(row), 16)
        : BigInteger.ZERO;
  }
}
