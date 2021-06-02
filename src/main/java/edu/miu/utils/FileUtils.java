package edu.miu.utils;

import edu.miu.mapreduce.Window;

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
            int p = pointer;
            while (p < splits.length && !splits[pointer].equals(key)) {
                w.addValue(splits[p]);
                p++;
            }
            res.add(w);
        }
        System.out.println(res);
        return res;
    }
}
