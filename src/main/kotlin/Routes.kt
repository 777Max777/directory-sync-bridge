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
import java.util.UUID

fun Application.configureRoutes() {
    routing {
        get("/") {
            val indexFile = this::class.java.classLoader.getResource("static/index.html")
            if (indexFile != null) {
                call.respondText(indexFile.readText(), ContentType.Text.Html)
            } else {
                call.respondText("index.html not found", status = HttpStatusCode.NotFound)
            }
        }

        get("/browse") {
            val path = call.request.queryParameters["path"] ?: mountRoot

            // Path traversal protection
            val normalizedPath = File(path).canonicalPath
            val normalizedRoot = File(mountRoot).canonicalPath
            if (!normalizedPath.startsWith(normalizedRoot)) {
                call.respond(HttpStatusCode.Forbidden, "Access denied")
                return@get
            }

            val dir = File(normalizedPath)
            if (!dir.exists() || !dir.isDirectory) {
                call.respond(HttpStatusCode.NotFound, "Directory not found")
                return@get
            }

            val dirs = dir.listFiles()
                ?.filter { it.isDirectory }
                ?.map { it.name }
                ?.sorted()
                ?: emptyList()

            val response = buildJsonObject {
                put("dirs", buildJsonArray {
                    dirs.forEach { add(it) }
                })
                put("path", normalizedPath.replace("\\", "/"))
            }
            call.respondText(response.toString(), ContentType.Application.Json)
        }

        webSocket("/ws") {
            val roomId = call.request.queryParameters["room"]
            if (roomId.isNullOrBlank()) {
                close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "room parameter required"))
                return@webSocket
            }

            val clientId = UUID.randomUUID().toString()
            val room = RoomManager.joinRoom(roomId, clientId, this)
            if (room == null) {
                close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "room is full"))
                return@webSocket
            }

            try {
                for (frame in incoming) {
                    if (frame is Frame.Text) {
                        val text = frame.readText()
                        val json = Json.parseToJsonElement(text).jsonObject
                        val type = json["type"]?.jsonPrimitive?.content ?: continue

                        when (type) {
                            "ready" -> {
                                val path = json["path"]?.jsonPrimitive?.content ?: continue
                                // Validate path is within mount root
                                val normalizedPath = File(path).canonicalPath
                                val normalizedRoot = File(mountRoot).canonicalPath
                                if (!normalizedPath.startsWith(normalizedRoot)) continue

                                room.mutex.withLock {
                                    room.getClient(clientId)?.path = path
                                }
                                room.broadcastStatus()
                            }

                            "start_sync" -> {
                                val deleteExtra = json["delete_extra"]?.jsonPrimitive?.boolean ?: false

                                room.mutex.withLock {
                                    if (room.syncInitiator != null) {
                                        // Already running
                                        val msg = buildJsonObject {
                                            put("type", "sync_rejected")
                                            put("reason", "already_started")
                                        }
                                        room.sendTo(clientId, msg.toString())
                                        return@withLock
                                    }

                                    val me = room.getClient(clientId)
                                    val peer = room.getPeer(clientId)
                                    if (me?.ready != true || peer?.ready != true) return@withLock

                                    room.syncInitiator = clientId

                                    // Notify both
                                    val initiatorMsg = buildJsonObject {
                                        put("type", "sync_started")
                                        put("initiator", "you")
                                    }
                                    val peerMsg = buildJsonObject {
                                        put("type", "sync_started")
                                        put("initiator", "peer")
                                    }
                                    room.sendTo(clientId, initiatorMsg.toString())
                                    room.sendTo(peer.clientId, peerMsg.toString())

                                    // Start sync in background
                                    val syncService = SyncService(
                                        room = room,
                                        sourcePath = me.path!!,
                                        destPath = peer.path!!,
                                        deleteExtra = deleteExtra
                                    )

                                    room.syncJob = launch {
                                        try {
                                            syncService.execute()
                                        } finally {
                                            room.mutex.withLock {
                                                room.syncJob = null
                                                room.syncInitiator = null
                                            }
                                            room.broadcastStatus()
                                        }
                                    }
                                }
                            }

                            "cancel" -> {
                                room.mutex.withLock {
                                    if (room.syncInitiator == clientId) {
                                        room.syncJob?.cancel()
                                        room.syncJob = null
                                        room.syncInitiator = null
                                    }
                                }
                                room.broadcastStatus()
                            }
                        }
                    }
                }
            } finally {
                RoomManager.leaveRoom(roomId, clientId)
            }
        }
    }
}
