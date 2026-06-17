package com.sync

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory
import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.security.MessageDigest
import kotlin.coroutines.coroutineContext
import java.util.Base64

private val logger = LoggerFactory.getLogger("PeerManager")

object PeerManager {
    // Browser WebSocket session (local UI)
    var browserSession: WebSocketSession? = null

    // Peer WebSocket session (the other instance)
    var peerSession: WebSocketSession? = null

    // State
    var localPath: String? = null
    var remotePath: String? = null
    var isSyncing = false
    var syncInitiator: String? = null // "local" or "remote"
    var syncJob: Job? = null
    val mutex = Mutex()

    // Ktor HTTP client for outgoing WebSocket connections
    private val client = HttpClient(CIO) {
        install(io.ktor.client.plugins.websocket.WebSockets) {
            pingInterval = 15_000
        }
    }

    private const val CHUNK_SIZE = 1_048_576 // 1 MB

    val isPeerConnected: Boolean get() = peerSession != null

    // ── Send helpers ──

    suspend fun sendToBrowser(msg: JsonObject) {
        try {
            browserSession?.send(Frame.Text(msg.toString()))
        } catch (e: Exception) {
            logger.warn("Failed to send to browser: ${e.message}")
        }
    }

    suspend fun sendToPeer(msg: JsonObject) {
        try {
            peerSession?.send(Frame.Text(msg.toString()))
        } catch (e: Exception) {
            logger.warn("Failed to send to peer: ${e.message}")
        }
    }

    // ── Status broadcasting ──

    suspend fun broadcastStatus() {
        val status = buildJsonObject {
            put("type", "status")
            put("my_path", localPath)
            put("my_ready", localPath != null)
            put("peer_path", remotePath)
            put("peer_ready", remotePath != null)
            put("peer_connected", isPeerConnected)
        }
        sendToBrowser(status)
    }

    // ── Connect to remote peer (as WS client) ──

    fun connectToPeer(host: String, port: Int, scope: CoroutineScope) {
        scope.launch {
            try {
                client.webSocket(host = host, port = port, path = "/peer") {
                    peerSession = this
                    logger.info("Connected to peer at $host:$port")

                    // Send our path if already set
                    if (localPath != null) {
                        sendToPeer(buildJsonObject {
                            put("type", "peer_path")
                            put("path", localPath!!)
                        })
                    }
                    broadcastStatus()

                    try {
                        for (frame in incoming) {
                            if (frame is Frame.Text) {
                                handlePeerMessage(frame.readText(), scope)
                            }
                        }
                    } finally {
                        peerSession = null
                        remotePath = null
                        broadcastStatus()
                        logger.info("Disconnected from peer")
                    }
                }
            } catch (e: Exception) {
                logger.error("Failed to connect to peer: ${e.message}")
                sendToBrowser(buildJsonObject {
                    put("type", "error")
                    put("message", "Не удалось подключиться к пиру: ${e.message}")
                })
            }
        }
    }

    // ── Handle messages from peer ──

    suspend fun handlePeerMessage(text: String, scope: CoroutineScope) {
        val json = Json.parseToJsonElement(text).jsonObject
        val type = json["type"]?.jsonPrimitive?.content ?: return

        when (type) {
            "peer_path" -> {
                remotePath = json["path"]?.jsonPrimitive?.content
                broadcastStatus()
            }

            "peer_start_sync" -> {
                // Peer wants to be the initiator (source), we are the receiver (dest)
                val deleteExtra = json["delete_extra"]?.jsonPrimitive?.boolean ?: false
                mutex.withLock {
                    if (isSyncing) {
                        sendToPeer(buildJsonObject {
                            put("type", "peer_sync_rejected")
                            put("reason", "already_started")
                        })
                        return
                    }
                    isSyncing = true
                    syncInitiator = "remote"
                }

                sendToBrowser(buildJsonObject {
                    put("type", "sync_started")
                    put("initiator", "peer")
                })
                sendToPeer(buildJsonObject {
                    put("type", "peer_sync_accepted")
                })
            }

            "peer_sync_accepted" -> {
                // Our sync request was accepted, start scanning and sending files
                val deleteExtra = json["delete_extra"]?.jsonPrimitive?.boolean ?: false
                sendToBrowser(buildJsonObject {
                    put("type", "sync_started")
                    put("initiator", "you")
                })
                syncJob = scope.launch {
                    try {
                        performSyncAsInitiator()
                    } finally {
                        mutex.withLock {
                            isSyncing = false
                            syncInitiator = null
                            syncJob = null
                        }
                        broadcastStatus()
                    }
                }
            }

            "peer_sync_rejected" -> {
                mutex.withLock {
                    isSyncing = false
                    syncInitiator = null
                }
                sendToBrowser(buildJsonObject {
                    put("type", "sync_rejected")
                    put("reason", json["reason"]?.jsonPrimitive?.content ?: "rejected")
                })
                broadcastStatus()
            }

            "peer_file_list" -> {
                // We are the receiver — got file list from initiator
                val deleteExtra = json["delete_extra"]?.jsonPrimitive?.boolean ?: false
                val files = json["files"]!!.jsonArray
                syncJob = scope.launch {
                    try {
                        handleFileListAsReceiver(files, deleteExtra)
                    } finally {
                        mutex.withLock {
                            isSyncing = false
                            syncInitiator = null
                            syncJob = null
                        }
                        broadcastStatus()
                    }
                }
            }

            "peer_file_data" -> {
                // Receiving a file from initiator
                handleIncomingFile(json)
            }

            "peer_log" -> {
                // Relay peer log to our browser
                sendToBrowser(buildJsonObject {
                    put("type", "log")
                    put("level", json["level"]?.jsonPrimitive?.content ?: "info")
                    put("message", json["message"]?.jsonPrimitive?.content ?: "")
                })
            }

            "peer_progress" -> {
                sendToBrowser(buildJsonObject {
                    put("type", "progress")
                    put("current", json["current"]?.jsonPrimitive?.int ?: 0)
                    put("total", json["total"]?.jsonPrimitive?.int ?: 0)
                    put("file", json["file"]?.jsonPrimitive?.content ?: "")
                })
            }

            "peer_done" -> {
                sendToBrowser(buildJsonObject {
                    put("type", "done")
                    put("stats", json["stats"]!!)
                })
                mutex.withLock {
                    isSyncing = false
                    syncInitiator = null
                    syncJob = null
                }
                broadcastStatus()
            }

            "peer_cancel" -> {
                syncJob?.cancel()
                syncJob = null
                mutex.withLock {
                    isSyncing = false
                    syncInitiator = null
                }
                sendLog("warn", "[WARN] Синхронизация отменена другой стороной")
                broadcastStatus()
            }

            "peer_request_file" -> {
                // Receiver requests a specific file from us (initiator)
                val path = json["path"]?.jsonPrimitive?.content ?: return
                scope.launch { sendFile(path) }
            }

            "peer_file_written" -> {
                // Receiver confirms file was written — used for progress tracking
            }

            "peer_delete_confirm" -> {
                // Receiver confirms deletion
            }
        }
    }

    // ── Sync as initiator (source) ──

    private var pendingDeleteExtra = false

    suspend fun startSync(deleteExtra: Boolean, scope: CoroutineScope) {
        mutex.withLock {
            if (isSyncing) {
                sendToBrowser(buildJsonObject {
                    put("type", "sync_rejected")
                    put("reason", "already_started")
                })
                return
            }
            if (localPath == null || remotePath == null || peerSession == null) return
            isSyncing = true
            syncInitiator = "local"
            pendingDeleteExtra = deleteExtra
        }

        sendToPeer(buildJsonObject {
            put("type", "peer_start_sync")
            put("delete_extra", deleteExtra)
        })
        // Actual sync starts when we receive peer_sync_accepted
    }

    private suspend fun performSyncAsInitiator() {
        val srcDir = File(localPath!!)
        if (!srcDir.exists() || !srcDir.isDirectory) {
            sendLog("error", "[ERROR] Папка источника не существует: $localPath")
            return
        }

        val ignoreService = IgnoreService(srcDir)
        if (ignoreService.rulesCount > 0) {
            sendLog("info", "[INFO] Загружен .syncignore: ${ignoreService.rulesCount} правил")
            sendPeerLog("info", "[INFO] Загружен .syncignore: ${ignoreService.rulesCount} правил")
        }

        // Scan files
        val allFiles = srcDir.walkTopDown().filter { it.isFile }.toList()
        val fileInfos = mutableListOf<JsonObject>()
        var ignoredCount = 0

        for (file in allFiles) {
            if (!coroutineContext[Job]!!.isActive) return
            val relativePath = file.relativeTo(srcDir).path.replace("\\", "/")

            if (ignoreService.isIgnored(relativePath)) {
                ignoredCount++
                sendLog("ignore", "[IGNORE] $relativePath — исключён по правилу .syncignore")
                sendPeerLog("ignore", "[IGNORE] $relativePath — исключён по правилу .syncignore")
                continue
            }

            val md5 = md5(file)
            fileInfos.add(buildJsonObject {
                put("path", relativePath)
                put("md5", md5)
                put("size", file.length())
                put("lastModified", file.lastModified())
            })
        }

        // Send file list to peer
        sendToPeer(buildJsonObject {
            put("type", "peer_file_list")
            put("files", JsonArray(fileInfos))
            put("delete_extra", pendingDeleteExtra)
            put("ignored_count", ignoredCount)
        })
    }

    // ── Sync as receiver (dest) ──

    private suspend fun handleFileListAsReceiver(files: JsonArray, deleteExtra: Boolean) {
        val destDir = File(localPath!!)
        if (!destDir.exists()) destDir.mkdirs()

        val stats = mutableMapOf(
            "copied" to 0, "overwritten" to 0, "skipped" to 0,
            "deleted" to 0, "ignored" to 0, "errors" to 0
        )

        val remoteFiles = files.map { it.jsonObject }
        val totalFiles = remoteFiles.size
        val remotePaths = mutableSetOf<String>()

        for ((index, fileInfo) in remoteFiles.withIndex()) {
            if (!coroutineContext[Job]!!.isActive) return

            val path = fileInfo["path"]!!.jsonPrimitive.content
            val md5 = fileInfo["md5"]!!.jsonPrimitive.content
            val size = fileInfo["size"]!!.jsonPrimitive.long
            val lastModified = fileInfo["lastModified"]!!.jsonPrimitive.long
            remotePaths.add(path)

            val localFile = File(destDir, path)

            // Send progress
            sendProgressBoth(index + 1, totalFiles, path)

            if (localFile.exists()) {
                val localMd5 = md5(localFile)
                if (localMd5 == md5) {
                    stats["skipped"] = stats["skipped"]!! + 1
                    sendLogBoth("info", "[SKIP] $path — без изменений")
                    continue
                } else {
                    // Need to overwrite — request file from peer
                    sendToPeer(buildJsonObject {
                        put("type", "peer_request_file")
                        put("path", path)
                        put("lastModified", lastModified)
                    })
                    // Wait for file data (handled in handleIncomingFile)
                    waitForFile(path)
                    stats["overwritten"] = stats["overwritten"]!! + 1
                    sendLogBoth("warn", "[OVERWRITE] $path — перезаписан (приоритет инициатора)")
                }
            } else {
                // Need to copy — request file from peer
                sendToPeer(buildJsonObject {
                    put("type", "peer_request_file")
                    put("path", path)
                    put("lastModified", lastModified)
                })
                waitForFile(path)
                stats["copied"] = stats["copied"]!! + 1
                sendLogBoth("ok", "[COPY] $path — скопирован")
            }

            yield()
        }

        // Delete extra files
        if (deleteExtra) {
            destDir.walkTopDown().filter { it.isFile }.forEach { file ->
                val relativePath = file.relativeTo(destDir).path.replace("\\", "/")
                if (relativePath !in remotePaths) {
                    try {
                        file.delete()
                        stats["deleted"] = stats["deleted"]!! + 1
                        sendLogBoth("warn", "[DELETE] $relativePath — удалён у получателя")
                    } catch (e: Exception) {
                        stats["errors"] = stats["errors"]!! + 1
                        sendLogBoth("error", "[ERROR] Не удалось удалить $relativePath")
                    }
                }
            }
        }

        // Send done to both sides
        val doneStats = buildJsonObject {
            stats.forEach { (k, v) -> put(k, v) }
        }
        sendToBrowser(buildJsonObject {
            put("type", "done")
            put("stats", doneStats)
        })
        sendToPeer(buildJsonObject {
            put("type", "peer_done")
            put("stats", doneStats)
        })
    }

    // ── File transfer ──

    // Pending file writes — receiver waits for file data
    private val pendingFiles = mutableMapOf<String, CompletableDeferred<Unit>>()

    private suspend fun waitForFile(path: String): Boolean {
        val deferred = CompletableDeferred<Unit>()
        pendingFiles[path] = deferred
        return try {
            withTimeout(300_000) { deferred.await() } // 5 min timeout per file
            true
        } catch (e: Exception) {
            pendingFiles.remove(path)
            false
        }
    }

    private suspend fun handleIncomingFile(json: JsonObject) {
        val path = json["path"]?.jsonPrimitive?.content ?: return
        val data = json["data"]?.jsonPrimitive?.content ?: ""
        val lastModified = json["lastModified"]?.jsonPrimitive?.long ?: System.currentTimeMillis()
        val chunkIndex = json["chunk_index"]?.jsonPrimitive?.int ?: 0
        val totalChunks = json["total_chunks"]?.jsonPrimitive?.int ?: 1
        val isLast = json["is_last"]?.jsonPrimitive?.boolean ?: true

        val destDir = File(localPath!!)
        val targetFile = File(destDir, path)
        targetFile.parentFile?.mkdirs()

        val bytes = if (data.isEmpty()) ByteArray(0) else Base64.getDecoder().decode(data)

        if (chunkIndex == 0) {
            // First chunk (or empty file) — create/overwrite file
            targetFile.writeBytes(bytes)
        } else {
            // Append subsequent chunks
            targetFile.appendBytes(bytes)
        }

        if (isLast) {
            Files.setLastModifiedTime(targetFile.toPath(), FileTime.fromMillis(lastModified))
            pendingFiles.remove(path)?.complete(Unit)
        }
    }

    private suspend fun sendFile(relativePath: String) {
        val srcDir = File(localPath!!)
        val file = File(srcDir, relativePath)
        if (!file.exists()) return

        val bytes = file.readBytes()

        if (bytes.isEmpty()) {
            // Empty file — send a single empty chunk
            sendToPeer(buildJsonObject {
                put("type", "peer_file_data")
                put("path", relativePath)
                put("data", "")
                put("chunk_index", 0)
                put("total_chunks", 1)
                put("is_last", true)
                put("lastModified", file.lastModified())
            })
            return
        }

        val totalChunks = (bytes.size + CHUNK_SIZE - 1) / CHUNK_SIZE

        for (i in 0 until totalChunks) {
            val start = i * CHUNK_SIZE
            val end = minOf(start + CHUNK_SIZE, bytes.size)
            val chunk = bytes.copyOfRange(start, end)
            val encoded = Base64.getEncoder().encodeToString(chunk)

            sendToPeer(buildJsonObject {
                put("type", "peer_file_data")
                put("path", relativePath)
                put("data", encoded)
                put("chunk_index", i)
                put("total_chunks", totalChunks)
                put("is_last", i == totalChunks - 1)
                put("lastModified", file.lastModified())
            })
            yield()
        }
    }

    // ── Cancel ──

    suspend fun cancelSync() {
        mutex.withLock {
            if (syncInitiator == "local") {
                syncJob?.cancel()
                syncJob = null
                isSyncing = false
                syncInitiator = null
                sendToPeer(buildJsonObject { put("type", "peer_cancel") })
            }
        }
        broadcastStatus()
    }

    // ── Disconnect ──

    suspend fun disconnectPeer() {
        syncJob?.cancel()
        mutex.withLock {
            isSyncing = false
            syncInitiator = null
            syncJob = null
        }
        try { peerSession?.close() } catch (_: Exception) {}
        peerSession = null
        remotePath = null
        broadcastStatus()
    }

    // ── Helpers ──

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
        sendToBrowser(buildJsonObject {
            put("type", "log")
            put("level", level)
            put("message", message)
        })
    }

    private suspend fun sendPeerLog(level: String, message: String) {
        sendToPeer(buildJsonObject {
            put("type", "peer_log")
            put("level", level)
            put("message", message)
        })
    }

    private suspend fun sendLogBoth(level: String, message: String) {
        sendLog(level, message)
        sendPeerLog(level, message)
    }

    private suspend fun sendProgressBoth(current: Int, total: Int, file: String) {
        val progress = buildJsonObject {
            put("type", "progress")
            put("current", current)
            put("total", total)
            put("file", file)
        }
        sendToBrowser(progress)
        sendToPeer(buildJsonObject {
            put("type", "peer_progress")
            put("current", current)
            put("total", total)
            put("file", file)
        })
    }
}
