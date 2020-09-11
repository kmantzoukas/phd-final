package uk.ac.city.monitor.enums;

/*
Spark supports two basic types of transformations; transformations with narrow dependencies
and transformations with wide dependencies
*/
public enum OperationType {
    READRDD, WRITERDD, READSHUFFLE, WRITESHUFFLE;
}
