package com.nayanzin.highperformancespark.utils;

import java.io.Serializable;
import java.util.Comparator;

public interface SerializableComparator<T> extends Comparator<T>, Serializable {
}
