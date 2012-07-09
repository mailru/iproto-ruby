require 'socket'
module IProto
  # TODO: timeouts
  class TCPSocket
    include IProto::ConnectionAPI
    def initialize(host, port, reconnect = true)
      @host = host
      @port = port
      @reconnect_timeout = Numeric === reconnect ? reconnect : DEFAULT_RECONNECT
      @reconnect = !!reconnect
      @socket = nil
      @reconnect_time = Time.now - 1
    end

    def close
      @reconnect = false
      if @socket
        @socket.close rescue nil
        @socket = :disconnected
      end
    end

    def connected?
      @socket && @socket != :disconnected
    end

    def could_be_connected?
      @socket ? @socket != :disconnected
              : @reconnect_time < Time.now
    end

    def socket
      if (sock = @socket)
        sock != :disconnected ? sock : raise(Disconnected, "disconnected earlier")
      else
        @socket = ::TCPSocket.new(@host, @port)
      end
    rescue Errno::ECONNREFUSED => e
      @socket = :disconnected  unless @reconnect
      raise CouldNotConnect, e
    end

    # begin ConnectionAPI
    def send_request(request_type, body)
      request_id = next_request_id
      r = socket.send ([request_type, body.bytesize, request_id].pack(PACK) << body), 0
      response_size = recv_header request_id
      recv_response response_size
    rescue Errno::EPIPE => e
      _raise_disconnected(e)
    end
    # end ConnectionAPI

    def recv_header(request_id)
      header = socket.read(12)  or _raise_disconnected('disconnected while read')
      type, response_size, recv_request_id = header.unpack(PACK)
      unless request_id == recv_request_id
        raise UnexpectedResponse.new("Waiting response for request_id #{request_id}, but received for #{recv_request_id}")
      end
      response_size
    end

    def recv_response(response_size)
      socket.read(response_size)  or _raise_disconnected('disconnected while read')
    end

    def _raise_disconnected(message)
      if @reconnect
        @socket = nil
        @reconnect_time = Time.now + @reconnect_timeout
      else
        @socket = :disconnected
      end
      raise Disconnected, message
    end
  end
end
