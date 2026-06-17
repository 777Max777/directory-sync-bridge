package com.sync

import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.websocket.*
import kotlinx.serialization.json.Json
import java.time.Duration

/**
 * Configurable browse root — limits which directories users can browse.
 * Default "/" allows browsing the entire local filesystem.
 * Set BROWSE_ROOT env var to restrict, e.g. BROWSE_ROOT=/home/user
 */
val browseRoot: String = System.getenv("BROWSE_ROOT") ?: "/"

fun main() {
    val port = System.getenv("PORT")?.toIntOrNull() ?: 8000
    embeddedServer(Netty, port = port) {
        install(WebSockets) {
            pingPeriod = Duration.ofSeconds(15)
            timeout = Duration.ofSeconds(30)
            maxFrameSize = Long.MAX_VALUE
            masking = false
        }
        install(ContentNegotiation) {
            json(Json {
                prettyPrint = false
                isLenient = true
                ignoreUnknownKeys = true
            })
        }
        configureRoutes()
    }.start(wait = true)
}
