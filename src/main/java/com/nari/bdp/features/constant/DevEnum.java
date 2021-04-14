package com.nari.bdp.features.constant;

public enum DevEnum {
  PWRGRID("0101","电网"),
  ACLINE("1201","交流线路"),
  AVLINEEND("1210","电网");

 private String code;
 private String name;

  DevEnum(String code, String name) {
    this.code = code;
    this.name = name;
  }

  public String getCode() {
    return code;
  }

  public String getName() {
    return name;
  }
}
