package io.univalence.unidb.command;

import java.io.IOException;
import java.nio.file.Paths;

public class WriteFileChannelMain {

    public static void main(String[] args) throws IOException {
        var cl = new CommitLog(Paths.get("data/_cl1.dta"));
        cl.add("hello1");

        cl = new CommitLog(Paths.get("data/_cl2.dta"));
        cl.add("hello2");

        cl = new CommitLog(Paths.get("data/_cl3.dta"));
        cl.add("hello3");
    }

}
