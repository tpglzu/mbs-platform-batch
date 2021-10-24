package com.ycu.tang.msbplatform.batch.utils;

import java.util.Calendar;

public class DateUtils {
  public static int getNowSec(){
    return Calendar.getInstance().get(Calendar.SECOND);
  }
}
