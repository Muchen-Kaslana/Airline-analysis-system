package com.po;

import java.util.Map;

public class ResultTo {
    public Map value;
    public String msg; //消息
    public Map getValue() {
        return value;
    }
    public void setValue(Map value) {
        this.value = value;
    }
    public String getMsg() {
        return msg;
    }
    public void setMsg(String msg) {
        this.msg = msg;
    }

}