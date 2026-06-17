package com.sync

import kotlinx.coroutines.Job
import kotlinx.coroutines.yield
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.FileTime
import java.security.MessageDigest
import kotlin.coroutines.coroutineContext

data class SyncStats(
    var copied: Int = 0,
    var overwritten: Int = 0,
    var skipped: Int = 0,
    var deleted: Int = 0,
    var ignored: Int = 0,
    var errors: Int = 0
)

class SyncService(
    private val room: Room,
    private val sourcePath: String,
    private val destPath: String,
    private val deleteExtra: Boolean
) {
    private val sourceDir = File(sourcePath)
    private val destDir = File(destPath)
    private val ignoreService = IgnoreService(sourceDir)
    private val stats = SyncStats()

    suspend fun execute() {
        // Log ignore rules
        if (ignoreService.rulesCount > 0) {
            sendLog("info", "[INFO] Загружен .syncignore: ${ignoreService.rulesCount} правил")
        }

        // Collect source files
        val sourceFiles = sourceDir.walkTopDown()
            .filter { it.isFile }
            .toList()

        val total = sourceFiles.size
        var current = 0
        val processedDestPaths = mutableSetOf<String>()

        for (file in sourceFiles) {
            if (!coroutineContext[Job]!!.isActive) {
                sendLog("warn", "[WARN] Синхронизация отменена")
                return
            }

            val relativePath = file.relativeTo(sourceDir).path.replace("\\", "/")
            current++

            // Check ignore
            if (ignoreService.isIgnored(relativePath)) {
                stats.ignored++
                sendLog("ignore", "[IGNORE] $relativePath — исключён по правилу .syncignore")
                sendProgress(current, total, relativePath)
                yield()
                continue
            }

            val destFile = File(destDir, relativePath)
            processedDestPaths.add(destFile.absolutePath)

            try {
                if (!destFile.exists()) {
                    // Copy new file
                    destFile.parentFile?.mkdirs()
                    Files.copy(file.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
                    Files.setLastModifiedTime(destFile.toPath(), FileTime.fromMillis(file.lastModified()))
                    stats.copied++
                    sendLog("ok", "[COPY] $relativePath — скопирован")
                } else {
                    val srcMd5 = md5(file)
                    val dstMd5 = md5(destFile)
                    if (srcMd5 == dstMd5) {
                        stats.skipped++
                        sendLog("info", "[SKIP] $relativePath — без изменений")
                    } else {
                        Files.copy(file.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING)
                        Files.setLastModifiedTime(destFile.toPath(), FileTime.fromMillis(file.lastModified()))
                        stats.overwritten++
                        sendLog("warn", "[OVERWRITE] $relativePath — перезаписан (приоритет инициатора)")
                    }
                }
            } catch (e: SecurityException) {
                stats.errors++
                sendLog("error", "[ERROR] $relativePath — нет доступа, пропущен")
            } catch (e: java.nio.file.AccessDeniedException) {
                stats.errors++
                sendLog("error", "[ERROR] $relativePath — нет доступа, пропущен")
            } catch (e: Exception) {
                stats.errors++
                sendLog("error", "[ERROR] $relativePath — ${e.message}")
            }

            sendProgress(current, total, relativePath)
            yield()
        }

        // Handle extra files in destination
        if (deleteExtra) {
            if (!coroutineContext[Job]!!.isActive) return

            destDir.walkTopDown()
                .filter { it.isFile }
                .forEach { destFile ->
                    if (!coroutineContext[Job]!!.isActive) return

                    val relativePath = destFile.relativeTo(destDir).path.replace("\\", "/")
                    if (!ignoreService.isIgnored(relativePath)) {
                        val sourceFile = File(sourceDir, relativePath)
                        if (!sourceFile.exists()) {
                            try {
                                destFile.delete()
                                stats.deleted++
                                sendLog("warn", "[DELETE] $relativePath — удалён у получателя")
                            } catch (e: Exception) {
                                stats.errors++
                                sendLog("error", "[ERROR] Не удалось удалить $relativePath — ${e.message}")
                            }
                        }
                    }
                }
        }

        // Send done
        val doneMsg = buildJsonObject {
            put("type", "done")
            put("stats", buildJsonObject {
                put("copied", stats.copied)
                put("overwritten", stats.overwritten)
                put("skipped", stats.skipped)
                put("deleted", stats.deleted)
                put("ignored", stats.ignored)
                put("errors", stats.errors)
            })
        }
        room.broadcastToAll(doneMsg.toString())
    }

    private fun md5(file: File): String {
        val digest = MessageDigest.getInstance("MD5")
        file.inputStream().use { input ->
            val buffer = ByteArray(8192)
            var read: Int
            while (input.read(buffer).also { read = it } != -1) {
                digest.update(buffer, 0, read)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private suspend fun sendLog(level: String, message: String) {
        val msg = buildJsonObject {
            put("type", "log")
            put("level", level)
            put("message", message)
        }
        room.broadcastToAll(msg.toString())
    }

    private suspend fun sendProgress(current: Int, total: Int, file: String) {
        val msg = buildJsonObject {
            put("type", "progress")
            put("current", current)
            put("total", total)
            put("file", file)
        }
        room.broadcastToAll(msg.toString())
    }
}
