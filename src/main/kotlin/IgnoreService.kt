package com.sync

import java.io.File
import java.nio.file.FileSystems
import java.nio.file.PathMatcher
import java.nio.file.Paths

class IgnoreService(sourceRoot: File) {
    private val matchers: List<PathMatcher>
    val rulesCount: Int

    init {
        val ignoreFile = File(sourceRoot, ".syncignore")
        val patterns = if (ignoreFile.exists()) {
            ignoreFile.readLines()
                .map { it.trim() }
                .filter { it.isNotEmpty() && !it.startsWith("#") }
        } else {
            emptyList()
        }
        rulesCount = patterns.size
        matchers = patterns.map { pattern ->
            FileSystems.getDefault().getPathMatcher("glob:$pattern")
        }
    }

    fun isIgnored(relativePath: String): Boolean {
        if (matchers.isEmpty()) return false
        val path = Paths.get(relativePath)
        return matchers.any { matcher ->
            matcher.matches(path) ||
            // Also check against just the file name for patterns like *.log
            matcher.matches(path.fileName) ||
            // Check each path component for directory patterns
            (0 until path.nameCount).any { i ->
                matcher.matches(path.subpath(0, i + 1))
            }
        }
    }
}
