package com.sync

import io.ktor.websocket.*
import kotlinx.coroutines.Job
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import java.util.concurrent.ConcurrentHashMap

data class ClientState(
    val clientId: String,
    var path: String? = null,
    val session: WebSocketSession
) {
    val ready: Boolean get() = path != null
}

class Room(val roomId: String) {
    var clientA: ClientState? = null
    var clientB: ClientState? = null
    var syncJob: Job? = null
    var syncInitiator: String? = null
    val mutex = Mutex()

    fun getClient(clientId: String): ClientState? =
        when {
            clientA?.clientId == clientId -> clientA
            clientB?.clientId == clientId -> clientB
            else -> null
        }

    fun getPeer(clientId: String): ClientState? =
        when {
            clientA?.clientId == clientId -> clientB
            clientB?.clientId == clientId -> clientA
            else -> null
        }

    fun buildStatusFor(clientId: String): JsonObject {
        val me = getClient(clientId)
        val peer = getPeer(clientId)
        return buildJsonObject {
            put("type", "status")
            put("my_path", me?.path)
            put("my_ready", me?.ready ?: false)
            put("peer_path", peer?.path)
            put("peer_ready", peer?.ready ?: false)
        }
    }

    suspend fun broadcastStatus() {
        listOfNotNull(clientA, clientB).forEach { client ->
            try {
                val status = buildStatusFor(client.clientId)
                client.session.send(Frame.Text(status.toString()))
            } catch (_: Exception) {}
        }
    }

    suspend fun broadcastToAll(message: String) {
        listOfNotNull(clientA, clientB).forEach { client ->
            try {
                client.session.send(Frame.Text(message))
            } catch (_: Exception) {}
        }
    }

    suspend fun sendTo(clientId: String, message: String) {
        try {
            getClient(clientId)?.session?.send(Frame.Text(message))
        } catch (_: Exception) {}
    }

    fun removeClient(clientId: String) {
        when {
            clientA?.clientId == clientId -> clientA = null
            clientB?.clientId == clientId -> clientB = null
        }
    }

    val isEmpty: Boolean get() = clientA == null && clientB == null
}

object RoomManager {
    private val rooms = ConcurrentHashMap<String, Room>()

    fun getOrCreateRoom(roomId: String): Room =
        rooms.getOrPut(roomId) { Room(roomId) }

    suspend fun joinRoom(roomId: String, clientId: String, session: WebSocketSession): Room? {
        val room = getOrCreateRoom(roomId)
        room.mutex.withLock {
            val clientState = ClientState(clientId, session = session)
            when {
                room.clientA == null -> room.clientA = clientState
                room.clientB == null -> room.clientB = clientState
                else -> return null // room full
            }
        }
        room.broadcastStatus()
        return room
    }

    suspend fun leaveRoom(roomId: String, clientId: String) {
        val room = rooms[roomId] ?: return
        room.mutex.withLock {
            if (room.syncInitiator == clientId) {
                room.syncJob?.cancel()
                room.syncJob = null
                room.syncInitiator = null
            }
            room.removeClient(clientId)
            if (room.isEmpty) {
                rooms.remove(roomId)
                return
            }
        }
        room.broadcastStatus()
    }
}
