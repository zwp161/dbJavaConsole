package com.zwp.bussiness.flowSystem;

/**
 * @author: zhouwp
 * @date:Create in 13:53 2021/10/22
 */
public class Line {
    private int lineNumber;
    private String line;

    public Line(int lineNumber, String line) {
        this.lineNumber = lineNumber;
        this.line = line;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(int lineNumber) {
        this.lineNumber = lineNumber;
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }
}
