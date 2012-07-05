require 'socket'
module IProto
  # TODO: timeouts
  class TCPSocket
    include IProto::ConnectionAPI
    def initialize(host, port, reconnect = true)
      @host = host
      @port = port
      @reconnect = true
    end

    def close
      @reconnect = false
      if @socket
        @socket.close rescue nil
      end
    end

    def socket
      @socket ||= ::TCPSocket.new(@host, @port)
    rescue Errno::ECONNREFUSED => e
      raise CouldNotConnect, e
    end

    # begin ConnectionAPI
    def send_request(request_type, body)
      request_id = next_request_id
      r = socket.send ([request_type, body.bytesize, request_id].pack(PACK) << body), 0
      response_size = recv_header request_id
      recv_response response_size
    rescue Errno::EPIPE => e
      @socket = nil  if @reconnect
      raise Disconnected, e
    end
    # end ConnectionAPI

    def recv_header(request_id)
      header = socket.read(12)  or begin
        @socket = nil  if @reconnect
        raise Disconnected, 'disconnected while read'
      end
      type, response_size, recv_request_id = header.unpack(PACK)
      unless request_id == recv_request_id
        raise UnexpectedResponse.new("Waiting response for request_id #{request_id}, but received for #{recv_request_id}")
      end
      response_size
    end

    def recv_response(response_size)
      socket.read(response_size)  or begin
        @socket = nil  if @reconnect
        raise Disconnected, 'disconnected while read'
      end
    end
  end
end
