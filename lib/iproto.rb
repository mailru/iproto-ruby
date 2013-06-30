module IProto
  VERSION = '0.3.10'
  class IProtoError < StandardError; end
  class ConnectionError < IProtoError; end
  class CouldNotConnect < ConnectionError; end
  class Disconnected < ConnectionError; end
  class UnexpectedResponse < IProtoError; end

  require 'iproto/connection_api'

  # types:
  # :em
  # :block
  def self.get_connection(host, port, type = :block, reconnect = true)
    case type
    when :em
      require 'iproto/em'
      ::EM.connect host, port, IProto::EMFiberedConnection, host, port, reconnect
    when :em_callback
      require 'iproto/em'
      ::EM.connect host, port, IProto::EMCallbackConnection, host, port, reconnect
    when :block
      require 'iproto/tcp_socket'
      IProto::TCPSocket.new(host, port, reconnect)
    else
      raise "Undefined type #{type}"
    end    
  end
end
