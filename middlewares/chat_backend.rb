require 'faye/websocket'
require 'kafka'
require 'thread'
require 'json'
require 'erb'

module ChatDemo
  class ChatBackend
    KEEPALIVE_TIME = 15 # in seconds
    CHANNEL        = "chat-demo"

    def initialize(app)
      @app     = app
      @clients = []

      @kafka = Kafka.new(
        seed_brokers: ENV.fetch("KAFKA_URL"),
        ssl_ca_cert: ENV.fetch("KAFKA_TRUSTED_CERT"),
        ssl_client_cert: ENV.fetch("KAFKA_CLIENT_CERT"),
        ssl_client_cert_key: ENV.fetch("KAFKA_CLIENT_CERT_KEY"),
      )

      @producer = @kafka.producer

      Thread.new do
        kafka = Kafka.new(
          seed_brokers: ENV.fetch("KAFKA_URL"),
          ssl_ca_cert: ENV.fetch("KAFKA_TRUSTED_CERT"),
          ssl_client_cert: ENV.fetch("KAFKA_CLIENT_CERT"),
          ssl_client_cert_key: ENV.fetch("KAFKA_CLIENT_CERT_KEY"),
        )

        consumer = kafka.consumer(group_id: "chat")
        consumer.subscribe(CHANNEL)

        consumer.each_message do |message|
          puts "Broadcasting #{message.value.inspect}"
          @clients.each {|ws| ws.send(message.value) }
        end
      end
    end

    def call(env)
      if Faye::WebSocket.websocket?(env)
        ws = Faye::WebSocket.new(env, nil, ping: KEEPALIVE_TIME)
        ws.on :open do |event|
          p [:open, ws.object_id]
          @clients << ws
        end

        ws.on :message do |event|
          p [:message, event.data]
          @producer.produce(sanitize(event.data), topic: CHANNEL, partition: 0)
          @producer.deliver_messages
        end

        ws.on :close do |event|
          p [:close, ws.object_id, event.code, event.reason]
          @clients.delete(ws)
          ws = nil
        end

        # Return async Rack response
        ws.rack_response
      else
        @app.call(env)
      end
    end

    private
    def sanitize(message)
      json = JSON.parse(message)
      json.each {|key, value| json[key] = ERB::Util.html_escape(value) }
      JSON.generate(json)
    end
  end
end
