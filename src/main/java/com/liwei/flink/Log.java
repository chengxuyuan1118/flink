package com.liwei.flink;

import java.io.Serializable;

public class Log implements Serializable {
    private String id;
    private String pNo;
    private String up;
    private String down;

    public Log(){

    }

    public Log(String id, String pNo, String up, String down) {
        this.id = id;
        this.pNo = pNo;
        this.up = up;
        this.down = down;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getpNo() {
        return pNo;
    }

    public void setpNo(String pNo) {
        this.pNo = pNo;
    }

    public String getUp() {
        return up;
    }

    public void setUp(String up) {
        this.up = up;
    }

    public String getDown() {
        return down;
    }

    public void setDown(String down) {
        this.down = down;
    }

    @Override
    public String toString() {
        return "Log{" +
                "id='" + id + '\'' +
                ", pNo='" + pNo + '\'' +
                ", up='" + up + '\'' +
                ", down='" + down + '\'' +
                '}';
    }
}
