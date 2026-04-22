package skunk.sharp.example

import cats.effect.IO
import cats.syntax.all.*
import ciris.*

case class DbConfig(
  host: String,
  port: Int,
  database: String,
  user: String,
  password: String,
  maxSessions: Int
)

case class ServerConfig(host: String, port: Int)

case class AppConfig(db: DbConfig, server: ServerConfig)

object AppConfig {

  def load: IO[AppConfig] =
    (
      env("DB_HOST").default("localhost"),
      env("DB_PORT").as[Int].default(5432),
      env("DB_NAME").default("example"),
      env("DB_USER").default("example"),
      env("DB_PASSWORD").default("example"),
      env("DB_MAX_SESSIONS").as[Int].default(10),
      env("SERVER_HOST").default("0.0.0.0"),
      env("SERVER_PORT").as[Int].default(8080)
    ).parMapN { (dbHost, dbPort, dbName, dbUser, dbPass, maxSess, serverHost, serverPort) =>
      AppConfig(
        db     = DbConfig(dbHost, dbPort, dbName, dbUser, dbPass, maxSess),
        server = ServerConfig(serverHost, serverPort)
      )
    }.load[IO]

}
