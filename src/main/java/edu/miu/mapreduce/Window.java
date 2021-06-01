package edu.miu.mapreduce;

import java.util.ArrayList;
import java.util.List;

public class Window {
    String key;
    List<String> values;

    public Window(String key) {
        this.key = key;
        this.values = new ArrayList<>();
    }

    public void addValue(String value) {
        this.values.add(value);
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "Window{" +
                "key='" + key + '\'' +
                ", values=" + values +
                '}';
    }
}
