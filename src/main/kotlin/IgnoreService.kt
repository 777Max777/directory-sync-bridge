package com.sync

import java.io.File
import java.nio.file.FileSystems
import java.nio.file.PathMatcher
import java.nio.file.Paths

/**
 * Reads `.syncignore` from the source root and decides whether a file
 * (given by its source-relative path, using "/" separators) is excluded.
 *
 * Supports .gitignore-style rules:
 *   - `*.log`            — bare glob, matches that name in any directory
 *   - `build/`           — directory rule, ignores the folder and everything inside it
 *   - `build/sub`        — anchored glob (contains a slash), relative to the source root
 *   - `**`+`/__pycache__/` — directory rule matched at any depth
 *   - `secret.txt`       — a concrete file name anywhere
 *   - leading `#`        — comment, ignored
 */
class IgnoreService(sourceRoot: File) {

    private data class Rule(
        val matcher: PathMatcher,
        /** Pattern ended with "/" — only matches directories (and their contents). */
        val dirOnly: Boolean,
        /** Pattern contains a "/" (besides a trailing one) — matched against the full relative path. */
        val anchored: Boolean,
    )

    private val rules: List<Rule>
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
        rules = patterns.map { raw ->
            val dirOnly = raw.endsWith("/")
            // Strip a leading "/" (root anchor) and any trailing "/" for matching.
            val normalized = raw.trim('/')
            val anchored = normalized.contains('/')
            Rule(
                matcher = FileSystems.getDefault().getPathMatcher("glob:$normalized"),
                dirOnly = dirOnly,
                anchored = anchored,
            )
        }
    }

    /** @param relativePath path of the file relative to the source root, using "/" separators. */
    fun isIgnored(relativePath: String): Boolean {
        if (rules.isEmpty()) return false
        val path = Paths.get(relativePath)
        val n = path.nameCount
        return rules.any { rule -> rule.matchesFile(path, n) }
    }

    private fun Rule.matchesFile(path: java.nio.file.Path, n: Int): Boolean {
        if (anchored) {
            // Match the full path; also match any ancestor directory prefix so that
            // everything *inside* a matched directory is ignored too.
            // For a dir-only rule we never match the file path itself, only its ancestors.
            val last = if (dirOnly) n - 1 else n
            for (i in 1..last) {
                if (matcher.matches(path.subpath(0, i))) return true
            }
            return false
        }

        // Bare-name rule: match against individual path components.
        // For a dir-only rule, only ancestor components count (the last component is the file).
        val last = if (dirOnly) n - 1 else n
        for (i in 0 until last) {
            if (matcher.matches(path.getName(i))) return true
        }
        return false
    }
}