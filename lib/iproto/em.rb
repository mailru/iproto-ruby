require 'eventmachine'
require 'fiber'
require 'iproto/core-ext'

module IProto
  class EMConnection < ::EM::Connection
    module FixedLengthProtocol
      def post_init
        raise "you should set @_needed_size"  unless @_needed_size
      end

      def receive_data(data)
        @buffer ||= ''.b
        offset = 0
        while (chunk = data.byteslice(offset, _needed_size - @buffer.bytesize)).size > 0
          @buffer << chunk
          offset += chunk.size
          if @buffer.size == _needed_size
            chunk = @buffer
            @buffer = ''.b
            receive_chunk chunk
          end
        end 
      end

      def receive_chunk(chunk)
        # for override
      end

      def buffer_reset
        @buffer = ''.b
      end
    end

    include IProto::ConnectionAPI
    include FixedLengthProtocol

    attr_reader :host, :port

    def initialize(host, port, reconnect = true)
      @host = host
      @port = port
      @reconnect_timeout = Numeric === reconnect ? reconnect : DEFAULT_RECONNECT
      @should_reconnect = !!reconnect
      @reconnect_timer = nil
      @connected = :init_waiting
      @waiting_requests = {}
      @waiting_for_connect = []
      init_protocol
      @shutdown_hook = false
      shutdown_hook
    end

    def connected?
      @connected == true
    end

    def could_be_connected?
      @connected && (@connected != :force || ::EM.reactor_running?)
    end

    def shutdown_hook
      unless @shutdown_hook
        ::EM.add_shutdown_hook {
          @connected = @should_reconnect ? :force : false
          if Integer === @reconnect_timer
            ::EM.cancel_timer @reconnect_timer
          end
          @reconnect_timer = nil
          @shutdown_hook = false
        }
        @shutdown_hook = true
      end
    end

    def connection_completed
      @reconnect_timer = nil
      @connected = true
      init_protocol
      _perform_waiting_for_connect(true)
    end

    attr_reader :_needed_size
    def init_protocol
      @_needed_size = HEADER_SIZE
      @_state = :receive_header
      buffer_reset
    end

    def receive_chunk(chunk)
      if @_state == :receive_header
        body_size = ::BinUtils.get_int32_le(chunk, 4)
        @request_id = ::BinUtils.get_int32_le(chunk, 8)
        if body_size > 0
          @_needed_size = body_size
          @_state = :receive_body
          return
        else
          chunk = ''
        end
      end
      request = @waiting_requests.delete @request_id
      raise IProto::UnexpectedResponse.new("For request id #{@request_id}") unless request
      @_needed_size = HEADER_SIZE
      @_state = :receive_header
      do_response(request, chunk)
    end

    def do_response(request, data)
      request.call data
    end

    def _setup_reconnect_timer(timeout)
      if @reconnect_timer.nil?
        @reconnect_timer = :waiting
        shutdown_hook
        if timeout == 0
          @connected = :waiting
          reconnect @host, @port
        else
          @reconnect_timer = ::EM.add_timer(timeout) do
            reconnect @host, @port
          end
        end
      end
    end

    def _send_request(request_type, body, request)
      if @connected == true
        _do_send_request(request_type, body, request)
      elsif could_be_connected?
        @waiting_for_connect << [request_type, body, request]
        if @connected == :force
          _setup_reconnect_timer(0)
        end
      elsif ::EM.reactor_running?
        EM.next_tick{
          do_response(request, IProto::Disconnected.new("connection is closed"))
        }
      else
        do_response(request, IProto::Disconnected.new("connection is closed"))
      end
    end

    def _perform_waiting_for_connect(real)
      if real
        @waiting_for_connect.each do |request_type, body, request|
          ::EM.next_tick{
          _do_send_request(request_type, body, request)
          }
        end
      else
        i = -1
        @waiting_for_connect.each do |request_type, body, request|
          @waiting_requests[i] = request
          i -= 1
        end
      end
      @waiting_for_connect.clear
    end

    def _do_send_request(request_type, body, request)
      while @waiting_requests.include?(request_id = next_request_id); end
      send_data pack_request(request_type, request_id, body)
      @waiting_requests[request_id] = request
    end

    def close
      close_connection(false)
    end

    def close_connection(*args)
      @should_reconnect = nil
      if Integer === @reconnect_timer
        ::EM.cancel_timer @reconnect_timer
      end
      @reconnect_timer = nil

      if @connected == true
        super(*args)
      end
      @connected = false
      discard_requests
    end

    def discard_requests
      exc = IProto::Disconnected.new("discarded cause of disconnect")
      _perform_waiting_for_connect(false)
      @waiting_requests.keys.each do |req|
        request = @waiting_requests.delete req
        do_response request, exc
      end
    end

    def unbind
      prev_connected = @connected
      @connected = false
      discard_requests
      @connected = prev_connected

      case @should_reconnect
      when true
        @reconnect_timer = nil
        unless @connected == :force
          @connected = false
          _setup_reconnect_timer(@reconnect_timeout)
        end
      when false
        if @connected == :init_waiting
          raise IProto::CouldNotConnect
        else
          raise IProto::Disconnected
        end
      when nil
        # do nothing cause we explicitely disconnected
      end
    end
  end

  class EMFiberedConnection < EMConnection
    def send_request(request_type, body)
      _send_request(request_type, body, Fiber.current)
      result = Fiber.yield
      raise result  if Exception === result
      result
    end
  end

  class EMCallbackConnection < EMConnection
    def send_request(request_type, body, cb = nil, &block)
      _send_request(request_type, body, cb || block)
    end
  end
end
