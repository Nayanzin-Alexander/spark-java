package com.nayanzin.sparkjava.testutils;

import com.nayanzin.sparkjava.ch01basics.WordCountTest;

import java.io.File;

public abstract class TestUtils {

    public static String getResourcesPath() {
        return new File(WordCountTest.class.getResource("/").getFile()).toString();
    }
}
