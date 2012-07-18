require 'socket'
module IProto
  # TODO: timeouts
  class TCPSocket
    include IProto::ConnectionAPI
    def initialize(host, port, reconnect = true)
      @addr = [host, port]
      @reconnect_timeout = Numeric === reconnect ? reconnect : DEFAULT_RECONNECT
      @reconnect = !!reconnect
      @socket = nil
      @reconnect_time = Time.now - 1
      @retry = true
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
              : (@retry || @reconnect_time < Time.now)
    end

    def socket
      if (sock = @socket)
        sock != :disconnected ? sock : raise(Disconnected, "disconnected earlier")
      else
        sock = @socket = ::TCPSocket.new(*@addr)
        @retry = true
      end
      sock
    rescue Errno::ECONNREFUSED => e
      @socket = :disconnected  unless @reconnect
      raise CouldNotConnect, e
    end

    class Retry < RuntimeError; end

    # begin ConnectionAPI
    def send_request(request_type, body)
      begin
        request_id = next_request_id
        r = socket.send ([request_type, body.bytesize, request_id].pack(PACK) << body), 0
        response_size = recv_header request_id
        recv_response response_size
      rescue Errno::EPIPE, Retry => e
        _raise_disconnected(e, !@retry)
        @retry = false
        retry
      end
    end
    # end ConnectionAPI

    def recv_header(request_id)
      header = socket.read(HEADER_SIZE)  or _raise_disconnected('disconnected while read', @retry ? :retry : true)
      type, response_size, recv_request_id = header.unpack(PACK)
      unless request_id == recv_request_id
        raise UnexpectedResponse.new("Waiting response for request_id #{request_id}, but received for #{recv_request_id}")
      end
      response_size
    end

    def recv_response(response_size)
      socket.read(response_size)  or _raise_disconnected('disconnected while read', 2)
    end

    def _raise_disconnected(message, _raise = true)
      if @reconnect
        @socket = nil
        @reconnect_time = Time.now + @reconnect_timeout
      else
        @socket = :disconnected
      end
      case _raise
      when true
        raise Disconnected, message
      when :retry
        raise Retry
      end
    end
  end
end
