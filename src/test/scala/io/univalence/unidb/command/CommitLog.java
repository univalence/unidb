package io.univalence.unidb.command;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class CommitLog {

    private final Path file;

    public CommitLog(Path file) throws IOException {
        this.file = file;
        if (!Files.exists(file)) {
            file.toFile().createNewFile();
        }
    }

    public void add(String data) throws IOException {
        try (var f = FileChannel.open(file,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.DSYNC
        )) {
            var content = data + "\n";
            var raw = content.getBytes();
            var buffer = ByteBuffer.wrap(raw);

            f.write(buffer);
            f.force(false);
        }
    }

    public String read() throws IOException {
        try (var f = FileChannel.open(file,
                StandardOpenOption.READ,
                StandardOpenOption.DSYNC
                )) {
            var buffer = ByteBuffer.allocate(4096);
            var byteRead = f.read(buffer);
            var content = "";
            while (byteRead >= 0) {
                var chunk = new String(buffer.array(), buffer.arrayOffset(), byteRead);
                content += chunk;
                buffer.clear();
                byteRead = f.read(buffer);
            }

            return content;
        }
    }

}
