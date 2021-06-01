package edu.miu.mapreduce;

import java.util.ArrayList;
import java.util.List;

public final class FileUtils {
    public static List<Window> extractWindowFromString(String s) {
        List<Window> res = new ArrayList<>();
        String[] splits = s.split(" ");
        int pointer = 0;
        while (pointer < splits.length) {
            String key = splits[pointer];
            Window w = new Window(key);
            pointer++;
            while (pointer < splits.length && !splits[pointer].equals(key)) {
                w.addValue(splits[pointer]);
                pointer++;
            }
            res.add(w);
        }
        return res;
    }
}
