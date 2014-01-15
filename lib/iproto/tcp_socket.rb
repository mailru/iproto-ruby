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
              : (@reconnect || @reconnect_time < Time.now)
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
      unless @reconnect
        @socket = :disconnected
      else
        @reconnect_time = Time.now + @reconnect_timeout
      end
      raise CouldNotConnect, e
    end

    class Retry < RuntimeError; end

    # begin ConnectionAPI
    def send_request(request_type, body)
      unless could_be_connected?
        raise Disconnected, "connection is closed"
      end
      begin
        request_id = next_request_id
        socket.send pack_request(request_type, request_id, body), 0
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
      response_size = ::BinUtils.get_int32_le(header, 4)
      recv_request_id = ::BinUtils.get_int32_le(header, 8)
      unless request_id == recv_request_id
        raise UnexpectedResponse.new("Waiting response for request_id #{request_id}, but received for #{recv_request_id}")
      end
      response_size
    end

    def recv_response(response_size)
      socket.read(response_size)  or _raise_disconnected('disconnected while read', true)
    end

    def _raise_disconnected(message, _raise = true)
      old_socket = @socket
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
        begin
          # if request were sent, then we will not have EPIPE exception
          old_socket.send '\x00', 0
        rescue Errno::EPIPE
          # OS knows that socket is closed, and request were not sent
          raise Retry
        else
          # OS didn't notice socket is closed, request were sent probably
          raise Disconnected, message
        end
      end
    end
  end
end
