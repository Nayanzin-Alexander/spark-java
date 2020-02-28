package com.nayanzin.sparkjava.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.Comparator.reverseOrder;

public abstract class TestUtils {
    public static Path getTestResource(String path) {
        return new File(TestUtils.class.getResource(path).getFile()).toPath();
    }

    public static int deleteFiles(Path dir) throws IOException {
        return Files.exists(dir, NOFOLLOW_LINKS) ? Files.walk(dir)
                .sorted(reverseOrder())
                .map(Path::toFile)
                .mapToInt(file -> file.delete() ? 1 : 0)
                .sum() : 0;
    }
}
