require 'eventmachine'
require 'fiber'
require 'iproto/core-ext'

module IProto
  module EM
    module FixedLengthProtocol
      def post_init
        raise "you should set @_needed_size"  unless @_needed_size
      end

      def receive_data(data)
        @buffer ||= ''
        offset = 0
        while (chunk = data[offset, _needed_size - @buffer.size]).size > 0
          @buffer << chunk
          offset += chunk.size
          if @buffer.size == _needed_size
            chunk = @buffer
            @buffer = ''
            receive_chunk chunk
          end
        end 
      end

      def receive_chunk(chunk)
        # for override
      end
    end

  class Connection < ::EM::Connection
    include IProto::ConnectionAPI
    include FixedLengthProtocol
    HEADER_SIZE = 12

    def initialize(host, port, reconnect = true)
      @host = host
      @port = port
      @reconnect_timeout = Numeric === reconnect ? reconnect : DEFAULT_RECONNECT
      @should_reconnect = !!reconnect
      @reconnect_timer = :waiting
      @connected = false
      @waiting_requests = {}
      @waiting_for_connect = []
      init_protocol
    end

    def connected?
      !!@connected
    end

    def could_be_connected?
      @connected || @reconnect_timer == :waiting ||
        (@reconnect_timer == :force && ::EM.reactor_running?)
    end

    def shutdown_hook
      ::EM.add_shutdown_hook {
        @connected = false
        if @reconnect_timer && !(Symbol === @reconnect_timer)
          ::EM.cancel_timer @reconnect_timer
        end
        @reconnect_timer = @should_reconnect ? :force : nil
      }
    end

    def connection_completed
      @reconnect_timer = nil
      @connected = true
      shutdown_hook
      _perform_waiting_for_connect(true)
    end

    attr_reader :_needed_size
    def init_protocol
      @_needed_size = HEADER_SIZE
      @_state = :receive_header
    end

    def receive_chunk(chunk)
      if @_state == :receive_header
        @type, body_size, @request_id = chunk.unpack(PACK)
        @_needed_size = body_size
        @_state = :receive_body
      else
        request = @waiting_requests.delete @request_id
        raise IProto::UnexpectedResponse.new("For request id #{@request_id}") unless request
        @_needed_size = HEADER_SIZE
        @_state = :receive_header
        do_response(request, chunk)
      end
    end

    def do_response(request, data)
      request.call data
    end

    def _setup_reconnect_timer(timeout)
      if @reconnect_timer.nil? || @reconnect_timer == :force
        @reconnect_timer = :waiting
        if @timeout == 0
          reconnect @host, @port
        else
          @reconnect_timer = ::EM.add_timer(timeout) do
            reconnect @host, @port
          end
        end
      end
    end

    def _send_request(request_type, body, request)
      unless @connected
        unless @reconnect_timer && (@reconnect_timer != :force || ::EM.reactor_running?)
          if ::EM.reactor_running?
            EM.next_tick{
              do_response(request, IProto::Disconnected.new("connection is closed"))
            }
          else
            do_response(request, IProto::Disconnected.new("connection is closed"))
          end
        else
          @waiting_for_connect << [request_type, body, request]
          _setup_reconnect_timer(0)
        end
      else
        _do_send_request(request_type, body, request)
      end
    end

    def _perform_waiting_for_connect(real)
      if real
        @waiting_for_connect.each do |request_type, body, request|
          _do_send_request(request_type, body, request)
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
      send_data [request_type, body.size, request_id].pack(PACK) + body
      @waiting_requests[request_id] = request
    end

    def close
      close_connection(false)
    end

    def close_connection(*args)
      @should_reconnect = nil
      if @reconnect_timer
        ::EM.cancel_timer @reconnect_timer  unless Symbol === @reconnect_timer
        @reconnect_timer = nil
      end
      if @connected
        super(*args)
      end
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
      discard_requests
      case @should_reconnect
      when true
        @connected = false
        _setup_reconnect_timer(@reconnect_timeout) unless @reconnect_timer == :force
      when false
        if @connected
          raise IProto::Disconnected
        else
          raise IProto::CouldNotConnect
        end
      when nil
        # do nothing cause we explicitely disconnected
      end
    end
  end

  class FiberedConnection < Connection
    def send_request(request_type, body)
      _send_request(request_type, body, Fiber.current)
      result = Fiber.yield
      raise result  if Exception === result
      result
    end
  end

  class CallbackConnection < Connection
    def send_request(request_type, body, cb = nil, &block)
      _send_request(request_type, body, cb || block)
    end
  end

  end
end
