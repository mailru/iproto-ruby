module IProto
  VERSION = '0.2'
  class IProtoError < StandardError; end
  class CouldNotConnect < IProtoError; end
  class UnexpectedResponse < IProtoError; end
  class Disconnected < IProtoError; end

  require 'iproto/connection_api'

  # types:
  # :em
  # :block
  def self.get_connection(host, port, type = :block)
    case type
    when :em
      require 'iproto/em'
      ::EM.connect host, port, IProto::EM::FiberedConnection, host, port
    when :block
      require 'iproto/tcp_socket'
      IProto::TCPSocket.new(host, port)
    else
      raise "Undefined type #{type}"
    end    
  end
end
