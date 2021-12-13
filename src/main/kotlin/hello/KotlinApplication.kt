package hello

import com.google.api.core.ApiFuture
import com.google.cloud.ServiceOptions
import com.google.cloud.bigquery.storage.v1.*
import hello.ArenaUpdate.Arena
import hello.ArenaUpdate.Arena.PlayerState
import org.json.JSONArray
import org.json.JSONObject
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.body
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue


@SpringBootApplication
class KotlinApplication {

    internal val committedStream: WriteCommittedStream =
        WriteCommittedStream(ServiceOptions.getDefaultProjectId(), "snowball", "events")
    val moveList = ConcurrentLinkedQueue<String>()

    @Bean
    fun routes() = router {
        GET(fun(request: ServerRequest): Mono<out ServerResponse> {
            request.queryParam("move").ifPresent { move -> moveList.add(move) }
            return ServerResponse.ok()
                .body(Mono.just("Let the battle begin! Get param: ${request.queryParam("move").orElse("nil")}"))
        })

        ////"F", "R", "L", "T"
        POST("/**", accept(APPLICATION_JSON)) { request ->
            request.bodyToMono(ArenaUpdate::class.java).flatMap { arenaUpdate ->
                println(arenaUpdate)
                committedStream.send(arenaUpdate.arena);
                ServerResponse.ok().body(Mono.just(Optional.ofNullable(moveList.poll()).orElse("T")))
            }
        }
    }

    internal class WriteCommittedStream(projectId: String?, datasetName: String?, tableName: String?) {
        var jsonStreamWriter: JsonStreamWriter? = null

        init {
            BigQueryWriteClient.create().use { client ->
                val stream: WriteStream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build()
                val parentTable: TableName = TableName.of(projectId, datasetName, tableName)
                val createWriteStreamRequest: CreateWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                    .setParent(parentTable.toString())
                    .setWriteStream(stream)
                    .build()
                val writeStream: WriteStream = client.createWriteStream(createWriteStreamRequest)
                jsonStreamWriter =
                    JsonStreamWriter.newBuilder(writeStream.getName(), writeStream.getTableSchema()).build()
            }
        }

        fun send(arena: Arena): ApiFuture<AppendRowsResponse> {
            val now: Instant = Instant.now()
            val jsonArray = JSONArray()
            arena.state.forEach { (url: String?, playerState: PlayerState) ->
                val jsonObject = JSONObject()
                jsonObject.put("x", playerState.x)
                jsonObject.put("y", playerState.y)
                jsonObject.put("direction", playerState.direction)
                jsonObject.put("wasHit", playerState.wasHit)
                jsonObject.put("score", playerState.score)
                jsonObject.put("player", url)
                jsonObject.put("timestamp", now.getEpochSecond() * 1000 * 1000)
                jsonArray.put(jsonObject)
            }
            return jsonStreamWriter!!.append(jsonArray)
        }

    }

}

fun main(args: Array<String>) {
    runApplication<KotlinApplication>(*args)
}

data class ArenaUpdate(val _links: Links, val arena: Arena) {
    data class Arena(val dims: List<Int>, val state: Map<String, PlayerState>) {
        data class PlayerState(val x: Int, val y: Int, val direction: String, val score: Int, val wasHit: Boolean)
    }

    data class Links(val self: Self) {
        data class Self(val href: String)
    }
}

