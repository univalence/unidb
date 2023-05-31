package io.univalence.unidb.command;

import java.io.IOException;
import java.nio.file.Paths;

public class ReadFileChannelMain {

    public static void main(String[] args) throws IOException {
        var cl = new CommitLog(Paths.get("data/_cl1.dta"));
        System.out.println(cl.read());

        cl = new CommitLog(Paths.get("data/_cl2.dta"));
        System.out.println(cl.read());

        cl = new CommitLog(Paths.get("data/_cl3.dta"));
        System.out.println(cl.read());
    }

}
