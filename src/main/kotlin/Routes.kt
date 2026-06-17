package com.sync

import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.websocket.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.*
import java.io.File

fun Application.configureRoutes() {
    routing {
        // Serve the UI
        get("/") {
            val indexFile = this::class.java.classLoader.getResource("static/index.html")
            if (indexFile != null) {
                call.respondText(indexFile.readText(), ContentType.Text.Html)
            } else {
                call.respondText("index.html not found", status = HttpStatusCode.NotFound)
            }
        }

        // Browse local filesystem
        get("/browse") {
            val path = call.request.queryParameters["path"] ?: browseRoot

            val normalizedPath = File(path).canonicalPath
            val normalizedRoot = File(browseRoot).canonicalPath
            if (!normalizedPath.startsWith(normalizedRoot)) {
                call.respond(HttpStatusCode.Forbidden, "Access denied")
                return@get
            }

            val dir = File(normalizedPath)
            if (!dir.exists() || !dir.isDirectory) {
                call.respond(HttpStatusCode.NotFound, "Directory not found")
                return@get
            }

            val dirs = try {
                dir.listFiles()
                    ?.filter { it.isDirectory && !it.isHidden }
                    ?.map { it.name }
                    ?.sorted()
                    ?: emptyList()
            } catch (e: SecurityException) {
                emptyList()
            }

            val response = buildJsonObject {
                put("dirs", buildJsonArray { dirs.forEach { add(it) } })
                put("path", normalizedPath.replace("\\", "/"))
            }
            call.respondText(response.toString(), ContentType.Application.Json)
        }

        // Browser WebSocket — local UI connects here
        webSocket("/ws") {
            if (PeerManager.browserSession != null) {
                close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "browser already connected"))
                return@webSocket
            }
            PeerManager.browserSession = this
            PeerManager.broadcastStatus()

            try {
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        val text = frame.readText()
                        val json = Json.parseToJsonElement(text).jsonObject
                        val type = json["type"]?.jsonPrimitive?.content ?: continue

                        when (type) {
                            // User selected a local folder
                            "ready" -> {
                                val path = json["path"]?.jsonPrimitive?.content ?: continue
                                val normalizedPath = File(path).canonicalPath
                                val normalizedRoot = File(browseRoot).canonicalPath
                                if (!normalizedPath.startsWith(normalizedRoot)) continue

                                PeerManager.localPath = path
                                // Notify peer about our path
                                PeerManager.sendToPeer(buildJsonObject {
                                    put("type", "peer_path")
                                    put("path", path)
                                })
                                PeerManager.broadcastStatus()
                            }

                            // Connect to a remote peer instance
                            "connect_peer" -> {
                                val host = json["host"]?.jsonPrimitive?.content ?: continue
                                val port = json["port"]?.jsonPrimitive?.int ?: 8000
                                PeerManager.connectToPeer(host, port, this@configureRoutes)
                            }

                            // Start sync (this user is the initiator)
                            "start_sync" -> {
                                val deleteExtra = json["delete_extra"]?.jsonPrimitive?.boolean ?: false
                                PeerManager.startSync(deleteExtra, this@configureRoutes)
                            }

                            // Cancel sync
                            "cancel" -> {
                                PeerManager.cancelSync()
                            }

                            // Disconnect from peer
                            "disconnect_peer" -> {
                                PeerManager.disconnectPeer()
                            }
                        }
                    }
                }
            } finally {
                PeerManager.browserSession = null
            }
        }

        // Peer WebSocket — the other instance connects here
        webSocket("/peer") {
            if (PeerManager.peerSession != null) {
                close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "peer already connected"))
                return@webSocket
            }
            PeerManager.peerSession = this
            // Send our path to the newly connected peer
            if (PeerManager.localPath != null) {
                PeerManager.sendToPeer(buildJsonObject {
                    put("type", "peer_path")
                    put("path", PeerManager.localPath!!)
                })
            }
            PeerManager.broadcastStatus()

            try {
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        PeerManager.handlePeerMessage(frame.readText(), this@configureRoutes)
                    }
                }
            } finally {
                PeerManager.peerSession = null
                PeerManager.remotePath = null
                PeerManager.mutex.withLock {
                    if (PeerManager.isSyncing) {
                        PeerManager.syncJob?.cancel()
                        PeerManager.isSyncing = false
                        PeerManager.syncInitiator = null
                        PeerManager.syncJob = null
                    }
                }
                PeerManager.broadcastStatus()
            }
        }
    }
}
