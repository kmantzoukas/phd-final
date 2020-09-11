package uk.ac.city.monitor.utils;

public interface Morpher<T> {
    T invoke(Object... arguments);
}