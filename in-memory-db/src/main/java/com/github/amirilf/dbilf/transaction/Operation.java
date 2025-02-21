package com.github.amirilf.dbilf.transaction;

@FunctionalInterface
public interface Operation {
    void undo();
}
